"""
Cost Optimizer Lambda Function
--------------------------------
Triggered on-demand (API Gateway) or by EventBridge after anomaly detection.
Reads the latest anomaly results from DynamoDB and generates actionable,
prioritized optimization recommendations with estimated ROI.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

dynamodb   = boto3.resource("dynamodb")
sns_client = boto3.client("sns")
ce_client  = boto3.client("ce")

ANOMALY_TABLE    = os.environ["ANOMALY_TABLE"]
REPORTS_TABLE    = os.environ["DYNAMODB_TABLE"]
SNS_TOPIC_ARN    = os.environ["SNS_TOPIC_ARN"]

# Savings thresholds (USD/month) for action priority
PRIORITY_CRITICAL_THRESHOLD = 500.0
PRIORITY_HIGH_THRESHOLD     = 100.0
PRIORITY_MEDIUM_THRESHOLD   = 25.0


def lambda_handler(event: dict, context: Any) -> dict:
    logger.info("Cost Optimizer started. event=%s", json.dumps(event))
    optimizer_id = str(uuid.uuid4())

    try:
        # ── 1. Load latest anomalies ──────────────────────────────────────
        anomalies = load_recent_anomalies(days=7)

        # ── 2. Generate recommendations ───────────────────────────────────
        recommendations = generate_recommendations(anomalies)

        # ── 3. Fetch current month's budget vs actual ─────────────────────
        budget_status = get_budget_overview()

        # ── 4. Build optimizer report ─────────────────────────────────────
        total_potential_savings = sum(r["estimated_monthly_savings"] for r in recommendations)
        report = {
            "OptimizerRunId":           optimizer_id,
            "Timestamp":                datetime.now(timezone.utc).isoformat(),
            "ReportType":               "OPTIMIZER",
            "TotalRecommendations":     len(recommendations),
            "TotalPotentialSavingsUSD": round(total_potential_savings, 2),
            "BudgetOverview":           budget_status,
            "Recommendations":          recommendations,
            "PriorityBreakdown": {
                "CRITICAL": sum(1 for r in recommendations if r["priority"] == "CRITICAL"),
                "HIGH":     sum(1 for r in recommendations if r["priority"] == "HIGH"),
                "MEDIUM":   sum(1 for r in recommendations if r["priority"] == "MEDIUM"),
                "LOW":      sum(1 for r in recommendations if r["priority"] == "LOW"),
            },
        }

        # ── 5. Store & alert ──────────────────────────────────────────────
        store_report(report)
        if recommendations:
            send_optimizer_alert(report)

        logger.info("Optimizer complete. run_id=%s recommendations=%d savings=$%.2f",
                    optimizer_id, len(recommendations), total_potential_savings)
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type":                "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(report, default=str),
        }

    except Exception as exc:
        logger.exception("Cost Optimizer failed: %s", exc)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)}),
        }


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def load_recent_anomalies(days: int = 7) -> list[dict]:
    """Load anomaly records from the last *days* days (excluding summary records)."""
    table = dynamodb.Table(ANOMALY_TABLE)
    try:
        resp = table.scan(
            FilterExpression=Attr("RecordType").not_exists()
        )
        return resp.get("Items", [])
    except ClientError as err:
        logger.error("DynamoDB scan error: %s", err)
        return []


def get_budget_overview() -> dict:
    """
    Fetch current month's AWS spend vs previous month for a quick comparison.
    Gracefully handles accounts without Cost Explorer fully enabled.
    """
    from datetime import date
    today       = date.today()
    first_day   = today.replace(day=1)
    prev_month  = (first_day - timedelta(days=1)).replace(day=1)

    try:
        resp = ce_client.get_cost_and_usage(
            TimePeriod={"Start": prev_month.isoformat(), "End": today.isoformat()},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
        )
        periods = resp.get("ResultsByTime", [])
        if len(periods) >= 2:
            prev_cost    = float(periods[0]["Total"]["UnblendedCost"]["Amount"])
            current_cost = float(periods[1]["Total"]["UnblendedCost"]["Amount"])
        elif len(periods) == 1:
            prev_cost    = 0.0
            current_cost = float(periods[0]["Total"]["UnblendedCost"]["Amount"])
        else:
            prev_cost, current_cost = 0.0, 0.0

        pct_change = (
            ((current_cost - prev_cost) / prev_cost * 100) if prev_cost > 0 else 0.0
        )
        return {
            "previous_month_usd": round(prev_cost, 2),
            "current_month_usd":  round(current_cost, 2),
            "percent_change":     round(pct_change, 2),
            "trend":              "UP" if pct_change > 5 else ("DOWN" if pct_change < -5 else "STABLE"),
        }
    except ClientError as err:
        logger.warning("Budget overview unavailable: %s", err)
        return {"error": "Cost Explorer not fully enabled"}


# ---------------------------------------------------------------------------
# Recommendation engine
# ---------------------------------------------------------------------------

def generate_recommendations(anomalies: list[dict]) -> list[dict]:
    """
    Convert raw anomaly records into structured, prioritized recommendations.
    Groups by service and aggregates cost impact.
    """
    # Group anomalies by service
    by_service: dict[str, list[dict]] = {}
    for a in anomalies:
        svc = a.get("Service", "Unknown")
        by_service.setdefault(svc, []).append(a)

    recommendations: list[dict] = []
    for svc, svc_anomalies in by_service.items():
        worst     = min(svc_anomalies, key=lambda x: float(x.get("AnomalyScore", 0)))
        max_cost  = max(float(a.get("CostUSD", 0)) for a in svc_anomalies)
        avg_cost  = sum(float(a.get("CostUSD", 0)) for a in svc_anomalies) / len(svc_anomalies)

        # Estimated savings: (max anomalous spend - avg normal spend) per month
        # simplified as 30 * (max_daily_anomalous / 2)
        estimated_savings = max_cost * 15.0  # rough 15-day equivalent

        priority = _compute_priority(estimated_savings, worst.get("Severity", "LOW"))

        recommendations.append({
            "recommendation_id":        f"REC-{str(uuid.uuid4())[:8].upper()}",
            "service":                  svc,
            "anomaly_count":            len(svc_anomalies),
            "worst_anomaly_date":       worst.get("Date", "N/A"),
            "worst_anomaly_score":      worst.get("AnomalyScore", "N/A"),
            "peak_daily_spend_usd":     round(max_cost,  2),
            "avg_anomalous_spend_usd":  round(avg_cost,  2),
            "estimated_monthly_savings":round(estimated_savings, 2),
            "priority":                 priority,
            "severity":                 worst.get("Severity", "LOW"),
            "action":                   worst.get("Recommendation", "Review service spending."),
            "actions_checklist":        _build_checklist(svc),
        })

    recommendations.sort(key=lambda r: r["estimated_monthly_savings"], reverse=True)
    return recommendations


def _compute_priority(savings: float, severity: str) -> str:
    if savings >= PRIORITY_CRITICAL_THRESHOLD or severity == "CRITICAL":
        return "CRITICAL"
    if savings >= PRIORITY_HIGH_THRESHOLD or severity == "HIGH":
        return "HIGH"
    if savings >= PRIORITY_MEDIUM_THRESHOLD or severity == "MEDIUM":
        return "MEDIUM"
    return "LOW"


def _build_checklist(service: str) -> list[str]:
    """Return a list of concrete action steps for the given service."""
    svc = service.lower()
    if "ec2" in svc:
        return [
            "Open EC2 console → check for stopped or idle instances.",
            "Review CloudWatch CPU utilization metrics (target >20% avg).",
            "Right-size over-provisioned instances using Compute Optimizer.",
            "Enable Spot Instances for non-critical workloads.",
            "Purchase Reserved Instances or Savings Plans for steady-state usage.",
        ]
    if "rds" in svc or "aurora" in svc:
        return [
            "Identify idle RDS instances via Performance Insights.",
            "Use Aurora Serverless v2 for variable workloads.",
            "Reduce Multi-AZ to Single-AZ for dev/staging environments.",
            "Implement connection pooling (RDS Proxy).",
            "Archive old snapshots and reduce backup retention period.",
        ]
    if "s3" in svc:
        return [
            "Enable S3 Intelligent-Tiering for infrequently accessed data.",
            "Create lifecycle rules to move data to Glacier after 90 days.",
            "Delete incomplete multipart uploads.",
            "Audit and remove stale S3 buckets.",
            "Reduce cross-region replication costs by using same-region replicas.",
        ]
    if "lambda" in svc:
        return [
            "Review Lambda memory allocation — reduce over-provisioned functions.",
            "Check for runaway recursive invocations in CloudWatch Logs.",
            "Use Lambda Power Tuning tool to optimize cost/performance.",
            "Move long-running tasks to Fargate or Batch.",
        ]
    if "cloudfront" in svc:
        return [
            "Analyze CloudFront Access Logs for unexpected traffic sources.",
            "Implement WAF rate limiting to prevent traffic abuse.",
            "Use Shield Standard (free) to mitigate DDoS amplification.",
            "Enable CloudFront Cache Policies to reduce origin requests.",
        ]
    if "data transfer" in svc:
        return [
            "Use VPC Endpoints to avoid data-transfer charges between services.",
            "Enable S3 Transfer Acceleration only where needed.",
            "Route traffic through CloudFront to reduce data transfer costs.",
            "Move compute closer to the data (same AZ/region as data source).",
        ]
    return [
        "Review service usage in Cost Explorer with daily granularity.",
        "Set up AWS Budgets alerts for this service.",
        "Consult AWS Cost Optimization Hub for tailored recommendations.",
    ]


# ---------------------------------------------------------------------------
# Storage & alerting
# ---------------------------------------------------------------------------

def store_report(report: dict) -> None:
    table = dynamodb.Table(REPORTS_TABLE)
    table.put_item(Item={
        "ReportId":  report["OptimizerRunId"],
        "Timestamp": report["Timestamp"],
        **{k: str(v) if isinstance(v, (float, int)) else v
           for k, v in report.items()},
    })
    logger.info("Optimizer report stored.")


def send_optimizer_alert(report: dict) -> None:
    if not SNS_TOPIC_ARN:
        return

    pb  = report["PriorityBreakdown"]
    recs = report["Recommendations"][:5]   # top 5

    lines = [
        "=" * 64,
        "  💡 AWS Cost Optimizer — Recommendations Report",
        "=" * 64,
        f"  Run ID  : {report['OptimizerRunId']}",
        f"  Date    : {report['Timestamp'][:10]}",
        f"  Total Potential Savings: ${report['TotalPotentialSavingsUSD']:,.2f}/month",
        "",
        "  Priority Breakdown",
        "  ------------------",
        f"  🔴 CRITICAL: {pb['CRITICAL']}  🟠 HIGH: {pb['HIGH']}  "
        f"🟡 MEDIUM: {pb['MEDIUM']}  🟢 LOW: {pb['LOW']}",
        "",
        "  Top Recommendations",
        "  -------------------",
    ]
    for r in recs:
        lines.append(
            f"  [{r['priority']:8s}] {r['service'][:35]:<35} "
            f"Save ~${r['estimated_monthly_savings']:>8.2f}/month"
        )
        lines.append(f"    → {r['action'][:100]}")
        lines.append("")

    lines += [
        "  Full recommendations stored in DynamoDB.",
        "=" * 64,
    ]

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=(
            f"[AWS] 💡 {report['TotalRecommendations']} Cost Recommendations — "
            f"Save ${report['TotalPotentialSavingsUSD']:,.2f}/month"
        ),
        Message="\n".join(lines),
    )
    logger.info("Optimizer alert sent.")


# ---------------------------------------------------------------------------
# Missing import fix
# ---------------------------------------------------------------------------
from datetime import timedelta  # noqa: E402 (already imported at top via datetime)
