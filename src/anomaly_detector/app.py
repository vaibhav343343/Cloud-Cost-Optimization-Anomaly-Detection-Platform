"""
Anomaly Detection Lambda (ML Inference)
-----------------------------------------
Triggered daily by EventBridge (after the Cost Collector).
1. Reads the last 90 days of per-service daily cost data from DynamoDB.
2. Trains / re-trains an Isolation Forest model on that history.
3. Runs inference to flag anomalous days.
4. Persists prediction results to DynamoDB.
5. Sends an SNS alert when anomalies are found.

Model: scikit-learn IsolationForest (contamination=auto).
Features: [cost_usd, usage_qty, day_of_week, month] per service.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import uuid
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Any

import boto3
import numpy as np
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
dynamodb  = boto3.resource("dynamodb")
sns_client = boto3.client("sns")

COST_HISTORY_TABLE  = os.environ["COST_HISTORY_TABLE"]
ANOMALY_TABLE       = os.environ["ANOMALY_TABLE"]
SNS_TOPIC_ARN       = os.environ["SNS_TOPIC_ARN"]

# Model hyper-parameters
ISOLATION_FOREST_ESTIMATORS   = int(os.environ.get("IF_N_ESTIMATORS",   "200"))
ISOLATION_FOREST_CONTAMINATION = float(os.environ.get("IF_CONTAMINATION", "0.05"))
MIN_TRAINING_SAMPLES           = int(os.environ.get("MIN_TRAINING_SAMPLES", "14"))

# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context: Any) -> dict:
    logger.info("Anomaly Detector started. event=%s", json.dumps(event))
    anomaly_run_id = str(uuid.uuid4())

    try:
        # ── 1. Load history from DynamoDB ────────────────────────────────
        history = load_cost_history(lookback_days=90)
        if not history:
            logger.warning("No cost history found. Skipping inference.")
            return {"statusCode": 204, "body": "No history data."}

        # ── 2. Per-service modelling ──────────────────────────────────────
        services = list(history.keys())
        all_anomalies: list[dict] = []

        for service in services:
            records = history[service]
            if len(records) < MIN_TRAINING_SAMPLES:
                logger.info("Skipping %s — only %d samples.", service, len(records))
                continue

            anomalies = detect_anomalies_for_service(service, records)
            all_anomalies.extend(anomalies)

        # ── 3. Persist results ────────────────────────────────────────────
        summary = persist_anomaly_results(anomaly_run_id, all_anomalies)

        # ── 4. Alert if anomalies exist ───────────────────────────────────
        if all_anomalies:
            send_anomaly_alert(anomaly_run_id, all_anomalies, summary)

        logger.info("Anomaly detection complete. run_id=%s anomalies=%d",
                    anomaly_run_id, len(all_anomalies))
        return {
            "statusCode": 200,
            "body": json.dumps({
                "run_id":          anomaly_run_id,
                "anomalies_found": len(all_anomalies),
                "services_checked": len(services),
            }),
        }

    except Exception as exc:
        logger.exception("Anomaly Detector failed: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_cost_history(lookback_days: int = 90) -> dict[str, list[dict]]:
    """
    Scan the cost history table and group records by ServiceName.
    Returns { "Amazon EC2": [ {Date, CostUSD, UsageQty}, ...], ... }
    """
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    table = dynamodb.Table(COST_HISTORY_TABLE)

    history: dict[str, list[dict]] = defaultdict(list)
    scan_kwargs: dict[str, Any] = {
        "FilterExpression": Attr("Date").gte(cutoff_date.isoformat()),
        "ProjectionExpression": "#d, ServiceName, CostUSD, UsageQty",
        "ExpressionAttributeNames": {"#d": "Date"},
    }

    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            history[item["ServiceName"]].append({
                "date":      item["Date"],
                "cost_usd":  float(item.get("CostUSD", 0)),
                "usage_qty": float(item.get("UsageQty", 0)),
            })
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    logger.info("Loaded history for %d services.", len(history))
    return dict(history)


# ---------------------------------------------------------------------------
# ML — Isolation Forest
# ---------------------------------------------------------------------------

def build_feature_matrix(records: list[dict]) -> tuple[np.ndarray, list[str]]:
    """
    Convert raw records into a numeric feature matrix.
    Features: [cost_usd, usage_qty, day_of_week (0-6), month (1-12),
               cost_usd_log1p, rolling_mean_7d_deviation]
    """
    sorted_recs = sorted(records, key=lambda r: r["date"])
    dates       = [r["date"] for r in sorted_recs]
    costs       = np.array([r["cost_usd"]  for r in sorted_recs], dtype=float)
    usages      = np.array([r["usage_qty"] for r in sorted_recs], dtype=float)

    # Temporal features
    day_of_week = np.array(
        [datetime.strptime(d, "%Y-%m-%d").weekday() for d in dates], dtype=float
    )
    month = np.array(
        [datetime.strptime(d, "%Y-%m-%d").month for d in dates], dtype=float
    )

    # Log transform (stabilise variance)
    log_costs = np.log1p(costs)

    # Rolling 7-day mean deviation
    rolling_mean = np.convolve(costs, np.ones(7) / 7, mode="same")
    deviation    = costs - rolling_mean

    X = np.column_stack([costs, usages, day_of_week, month, log_costs, deviation])
    return X, dates


def detect_anomalies_for_service(service: str, records: list[dict]) -> list[dict]:
    """
    Train an IsolationForest on *records* and return a list of anomalous
    data points for *service*.
    """
    X, dates = build_feature_matrix(records)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=ISOLATION_FOREST_ESTIMATORS,
        contamination=ISOLATION_FOREST_CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    predictions = model.predict(X_scaled)   # 1=normal, -1=anomaly
    scores      = model.decision_function(X_scaled)   # lower = more anomalous

    anomalies: list[dict] = []
    for i, (pred, score) in enumerate(zip(predictions, scores)):
        if pred == -1:
            anomalies.append({
                "service":    service,
                "date":       dates[i],
                "cost_usd":   round(X[i, 0], 4),
                "usage_qty":  round(X[i, 1], 4),
                "anomaly_score": round(float(score), 6),   # negative = more anomalous
                "severity":   _classify_severity(score),
                "recommendation": _generate_recommendation(service, X[i, 0], score),
            })

    logger.info("Service '%s': %d anomalies out of %d points.",
                service, len(anomalies), len(records))
    return anomalies


def _classify_severity(score: float) -> str:
    """Map the IF decision function score to a human-readable severity."""
    if score < -0.15:
        return "CRITICAL"
    if score < -0.08:
        return "HIGH"
    if score < -0.02:
        return "MEDIUM"
    return "LOW"


def _generate_recommendation(service: str, cost: float, score: float) -> str:
    """Generate a service-specific optimisation recommendation."""
    service_lower = service.lower()

    if "ec2" in service_lower:
        return (
            f"Unusual EC2 spend (${cost:.2f}). Check for forgotten instances, "
            "Auto Scaling misconfiguration, or unexpected instance types."
        )
    if "rds" in service_lower or "database" in service_lower:
        return (
            f"Unusual RDS spend (${cost:.2f}). Review Multi-AZ, instance class, "
            "and automated backup retention."
        )
    if "s3" in service_lower:
        return (
            f"Unusual S3 spend (${cost:.2f}). Audit storage classes, lifecycle "
            "policies, and data transfer out charges."
        )
    if "lambda" in service_lower:
        return (
            f"Unusual Lambda spend (${cost:.2f}). Check for runaway recursions "
            "or high invocation rates."
        )
    if "cloudfront" in service_lower:
        return (
            f"Unusual CloudFront spend (${cost:.2f}). Inspect data transfer and "
            "request metrics for traffic spikes."
        )
    if "sagemaker" in service_lower:
        return (
            f"Unusual SageMaker spend (${cost:.2f}). Check for idle notebook "
            "instances or long-running training jobs."
        )
    if "data transfer" in service_lower:
        return (
            f"High data transfer cost (${cost:.2f}). Review inter-region or "
            "internet-bound traffic and consider VPC endpoints."
        )
    return (
        f"Anomalous {service} spend (${cost:.2f}). Review recent deployments "
        "and usage patterns for this service."
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_anomaly_results(run_id: str, anomalies: list[dict]) -> dict:
    """Store anomaly run summary + individual anomaly records in DynamoDB."""
    table = dynamodb.Table(ANOMALY_TABLE)

    severity_counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    top_anomalies   = sorted(anomalies, key=lambda a: a["anomaly_score"])[:5]

    with table.batch_writer() as batch:
        for anomaly in anomalies:
            severity_counts[anomaly["severity"]] = (
                severity_counts.get(anomaly["severity"], 0) + 1
            )
            batch.put_item(Item={
                "AnomalyId":      f"{run_id}#{anomaly['service']}#{anomaly['date']}",
                "RunId":          run_id,
                "Timestamp":      datetime.now(timezone.utc).isoformat(),
                "Service":        anomaly["service"],
                "Date":           anomaly["date"],
                "CostUSD":        str(anomaly["cost_usd"]),
                "AnomalyScore":   str(anomaly["anomaly_score"]),
                "Severity":       anomaly["severity"],
                "Recommendation": anomaly["recommendation"],
            })

        # Run-level summary record
        batch.put_item(Item={
            "AnomalyId":       f"SUMMARY#{run_id}",
            "RunId":           run_id,
            "Timestamp":       datetime.now(timezone.utc).isoformat(),
            "RecordType":      "SUMMARY",
            "TotalAnomalies":  str(len(anomalies)),
            "SeverityCounts":  json.dumps(severity_counts),
            "TopAnomalies":    json.dumps(top_anomalies),
        })

    return {"severity_counts": severity_counts, "top_anomalies": top_anomalies}


# ---------------------------------------------------------------------------
# SNS alerts
# ---------------------------------------------------------------------------

def send_anomaly_alert(run_id: str, anomalies: list[dict], summary: dict) -> None:
    """Publish a structured anomaly alert to SNS."""
    if not SNS_TOPIC_ARN:
        return

    sc = summary["severity_counts"]
    lines = [
        "=" * 64,
        "  ⚠️  AWS COST ANOMALY ALERT  ⚠️",
        "=" * 64,
        f"  Run ID    : {run_id}",
        f"  Detected  : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"  Anomalies : {len(anomalies)}",
        "",
        "  Severity Breakdown",
        "  ------------------",
        f"  🔴 CRITICAL : {sc.get('CRITICAL', 0)}",
        f"  🟠 HIGH     : {sc.get('HIGH',     0)}",
        f"  🟡 MEDIUM   : {sc.get('MEDIUM',   0)}",
        f"  🟢 LOW      : {sc.get('LOW',      0)}",
        "",
        "  Top Anomalies (by severity score)",
        "  ---------------------------------",
    ]
    for a in summary["top_anomalies"]:
        lines.append(
            f"  [{a['severity']:8s}] {a['service'][:35]:<35} "
            f"${a['cost_usd']:>10.2f}  ({a['date']})"
        )
        lines.append(f"    → {a['recommendation']}")
        lines.append("")

    lines += [
        "  Full results stored in DynamoDB (AnomalyDetectionResults table).",
        "=" * 64,
    ]

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[AWS] 🚨 Cost Anomalies Detected — {len(anomalies)} Issues Found",
        Message="\n".join(lines),
    )
    logger.info("Anomaly alert sent. anomalies=%d", len(anomalies))
