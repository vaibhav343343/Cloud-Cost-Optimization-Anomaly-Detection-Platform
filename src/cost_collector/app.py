"""
Cost Collector Lambda Function
-------------------------------
Triggered daily by EventBridge. Fetches granular daily cost data per
AWS service from Cost Explorer API and stores each record in DynamoDB.
This historical data is used by the anomaly-detection (ML) Lambda.
"""

import json
import os
import uuid
import logging
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# ---------------------------------------------------------------------------
# AWS clients  (initialised outside handler for Lambda container re-use)
# ---------------------------------------------------------------------------
ce_client  = boto3.client("ce")
ec2_client = boto3.client("ec2")
dynamodb   = boto3.resource("dynamodb")
sns_client = boto3.client("sns")

COST_HISTORY_TABLE = os.environ["COST_HISTORY_TABLE"]   # raw daily spend
REPORTS_TABLE      = os.environ["DYNAMODB_TABLE"]        # optimization reports
SNS_TOPIC_ARN      = os.environ["SNS_TOPIC_ARN"]

# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Entry point.  Collects cost & resource data, stores history, generates
    an optimization report, and sends an SNS alert.
    """
    logger.info("Cost Collector started. event=%s", json.dumps(event))

    try:
        # ── 1. Fetch & store daily cost data (last 30 days) ──────────────
        cost_records = collect_daily_costs()
        store_cost_history(cost_records)

        # ── 2. Analyze idle / wasted resources ───────────────────────────
        ec2_data = analyze_ec2_instances()
        ebs_data = analyze_ebs_volumes()
        eip_data = analyze_elastic_ips()
        rds_data = analyze_rds_instances()

        # ── 3. Build & persist optimization report ───────────────────────
        total_savings = (
            ec2_data["potential_monthly_savings_usd"]
            + ebs_data["potential_monthly_savings_usd"]
            + eip_data["potential_monthly_savings_usd"]
            + rds_data["potential_monthly_savings_usd"]
        )

        report = {
            "ReportId":   str(uuid.uuid4()),
            "Timestamp":  datetime.now(timezone.utc).isoformat(),
            "ReportType": "COLLECTION",
            "CostSummary30d":         _summarize_costs(cost_records),
            "EC2Optimization":        ec2_data,
            "EBSOptimization":        ebs_data,
            "ElasticIPOptimization":  eip_data,
            "RDSOptimization":        rds_data,
            "TotalPotentialMonthlySavingsUSD": str(total_savings),
            "RawCostRecordsStored":   len(cost_records),
        }

        store_report(report)
        send_collection_alert(report)

        logger.info("Cost collection complete. report_id=%s savings=$%.2f",
                    report["ReportId"], total_savings)
        return {"statusCode": 200, "body": json.dumps(report)}

    except Exception as exc:
        logger.exception("Cost Collector failed: %s", exc)
        raise

# ---------------------------------------------------------------------------
# Cost Explorer helpers
# ---------------------------------------------------------------------------

def collect_daily_costs(lookback_days: int = 30) -> list[dict]:
    """
    Fetch per-service daily costs for the last *lookback_days* days.
    Returns a flat list of records ready for DynamoDB insertion.
    """
    end_date   = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=lookback_days)

    records: list[dict] = []
    try:
        paginator = ce_client.get_paginator("get_cost_and_usage")
        pages = paginator.paginate(
            TimePeriod={
                "Start": start_date.isoformat(),
                "End":   end_date.isoformat(),
            },
            Granularity="DAILY",
            Metrics=["UnblendedCost", "UsageQuantity"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )

        for page in pages:
            for time_period in page["ResultsByTime"]:
                date_str = time_period["TimePeriod"]["Start"]
                for group in time_period["Groups"]:
                    service_name = group["Keys"][0]
                    cost_amount  = float(group["Metrics"]["UnblendedCost"]["Amount"])
                    usage_qty    = float(group["Metrics"]["UsageQuantity"]["Amount"])

                    records.append({
                        "RecordId":    f"{date_str}#{service_name}",
                        "Date":        date_str,
                        "ServiceName": service_name,
                        "CostUSD":     str(round(cost_amount, 6)),
                        "UsageQty":    str(round(usage_qty,   6)),
                        "CollectedAt": datetime.now(timezone.utc).isoformat(),
                    })

    except ClientError as err:
        logger.error("Cost Explorer error: %s", err)

    logger.info("Collected %d cost records.", len(records))
    return records


def _summarize_costs(records: list[dict]) -> dict:
    """Roll up all records into total-per-service for the report."""
    totals: dict[str, float] = {}
    for rec in records:
        svc = rec["ServiceName"]
        totals[svc] = totals.get(svc, 0.0) + float(rec["CostUSD"])
    return {k: round(v, 2) for k, v in sorted(totals.items(), key=lambda x: -x[1])}


# ---------------------------------------------------------------------------
# Resource analysis helpers
# ---------------------------------------------------------------------------

def analyze_ec2_instances() -> dict:
    """Find stopped & potentially underutilized EC2 instances."""
    try:
        stopped = ec2_client.describe_instances(
            Filters=[{"Name": "instance-state-name", "Values": ["stopped"]}]
        )
        stopped_ids = [
            inst["InstanceId"]
            for r in stopped["Reservations"]
            for inst in r["Instances"]
        ]

        # Very rough per-instance savings estimate ($10/month for EBS gp2 storage)
        return {
            "stopped_instance_ids":          stopped_ids,
            "stopped_count":                 len(stopped_ids),
            "potential_monthly_savings_usd": len(stopped_ids) * 10.0,
            "recommendation": (
                "Review stopped instances; terminate those no longer needed "
                "to avoid EBS and EIP charges."
            ),
        }
    except ClientError as err:
        logger.error("EC2 analysis error: %s", err)
        return {"stopped_instance_ids": [], "stopped_count": 0,
                "potential_monthly_savings_usd": 0.0, "recommendation": "N/A"}


def analyze_ebs_volumes() -> dict:
    """Find unattached (available) EBS volumes."""
    try:
        resp = ec2_client.describe_volumes(
            Filters=[{"Name": "status", "Values": ["available"]}]
        )
        volumes = [
            {
                "VolumeId": v["VolumeId"],
                "SizeGiB":  v["Size"],
                "Type":     v["VolumeType"],
            }
            for v in resp["Volumes"]
        ]
        # gp2: ~$0.10/GiB-month  gp3: ~$0.08
        savings = sum(v["SizeGiB"] * 0.10 for v in volumes)
        return {
            "unattached_volumes":             volumes,
            "unattached_count":               len(volumes),
            "potential_monthly_savings_usd":  round(savings, 2),
            "recommendation": (
                "Snapshot & delete unattached EBS volumes to eliminate "
                "storage charges."
            ),
        }
    except ClientError as err:
        logger.error("EBS analysis error: %s", err)
        return {"unattached_volumes": [], "unattached_count": 0,
                "potential_monthly_savings_usd": 0.0, "recommendation": "N/A"}


def analyze_elastic_ips() -> dict:
    """Find Elastic IPs not associated with any instance or network interface."""
    try:
        resp = ec2_client.describe_addresses()
        idle_eips = [
            addr["PublicIp"]
            for addr in resp["Addresses"]
            if "InstanceId"        not in addr
            and "NetworkInterfaceId" not in addr
        ]
        # AWS charges $0.005/hour per idle EIP ≈ $3.60/month
        return {
            "idle_elastic_ips":              idle_eips,
            "idle_count":                    len(idle_eips),
            "potential_monthly_savings_usd": round(len(idle_eips) * 3.60, 2),
            "recommendation": (
                "Release idle Elastic IPs immediately; AWS charges for "
                "unassociated EIPs."
            ),
        }
    except ClientError as err:
        logger.error("EIP analysis error: %s", err)
        return {"idle_elastic_ips": [], "idle_count": 0,
                "potential_monthly_savings_usd": 0.0, "recommendation": "N/A"}


def analyze_rds_instances() -> dict:
    """
    Find RDS instances that are stopped (still incur storage costs).
    Requires ec2:DescribeDBInstances permission on the RDS service endpoint.
    """
    try:
        rds = boto3.client("rds")
        paginator = rds.get_paginator("describe_db_instances")
        stopped_dbs: list[str] = []
        for page in paginator.paginate():
            for db in page["DBInstances"]:
                if db["DBInstanceStatus"] == "stopped":
                    stopped_dbs.append(db["DBInstanceIdentifier"])

        return {
            "stopped_db_instances":          stopped_dbs,
            "stopped_count":                 len(stopped_dbs),
            "potential_monthly_savings_usd": len(stopped_dbs) * 25.0,
            "recommendation": (
                "Delete or snapshot stopped RDS instances. AWS still "
                "charges for storage even when the instance is stopped."
            ),
        }
    except ClientError as err:
        logger.error("RDS analysis error: %s", err)
        return {"stopped_db_instances": [], "stopped_count": 0,
                "potential_monthly_savings_usd": 0.0, "recommendation": "N/A"}


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def store_cost_history(records: list[dict]) -> None:
    """Batch-write cost records to the history table."""
    if not records:
        return
    table = dynamodb.Table(COST_HISTORY_TABLE)
    with table.batch_writer() as batch:
        for rec in records:
            batch.put_item(Item=rec)
    logger.info("Stored %d cost history records.", len(records))


def store_report(report: dict) -> None:
    """Persist the optimization report."""
    table = dynamodb.Table(REPORTS_TABLE)
    table.put_item(Item=report)
    logger.info("Report stored. report_id=%s", report["ReportId"])


# ---------------------------------------------------------------------------
# SNS helper
# ---------------------------------------------------------------------------

def send_collection_alert(report: dict) -> None:
    """Send a summary SNS notification after each collection run."""
    if not SNS_TOPIC_ARN:
        return

    savings = float(report["TotalPotentialMonthlySavingsUSD"])
    lines = [
        "=" * 60,
        "  AWS Cost Optimization — Daily Collection Report",
        "=" * 60,
        f"  Report ID : {report['ReportId']}",
        f"  Generated : {report['Timestamp']}",
        "",
        f"  Potential Monthly Savings : ${savings:,.2f}",
        "",
        "  Resource Summary",
        "  ----------------",
        f"  Stopped EC2 Instances : {report['EC2Optimization']['stopped_count']}",
        f"  Unattached EBS Volumes: {report['EBSOptimization']['unattached_count']}",
        f"  Idle Elastic IPs      : {report['ElasticIPOptimization']['idle_count']}",
        f"  Stopped RDS Instances : {report['RDSOptimization']['stopped_count']}",
        "",
        "  Check DynamoDB for the full report.",
        "=" * 60,
    ]

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="[AWS] Cost Optimization — Daily Report",
        Message="\n".join(lines),
    )
    logger.info("Collection alert sent to SNS.")
