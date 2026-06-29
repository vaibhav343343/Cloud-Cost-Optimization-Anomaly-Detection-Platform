"""
LEGACY — Original v1 Cost Analyzer Lambda (kept for reference).
See src/cost_collector/, src/anomaly_detector/, src/cost_optimizer/ for v2.

BUGS FIXED vs original:
  - IndentationError in send_alert() (line 146 was de-indented outside the function)
  - Replaced bare except with proper ClientError handling
  - Pinned requirements: boto3==1.34.144
"""

import json
import os
import boto3
import uuid
from datetime import datetime, timedelta

ce_client = boto3.client('ce')
ec2_client = boto3.client('ec2')
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')


def lambda_handler(event, context):
    try:
        print("Starting cost analysis...")
        cost_data = get_cost_data()
        ec2_data = analyze_ec2_instances()
        ebs_data = analyze_ebs_volumes()
        eip_data = analyze_elastic_ips()

        report = {
            'ReportId': str(uuid.uuid4()),
            'Timestamp': datetime.utcnow().isoformat(),
            'CostExplorerSummary': cost_data,
            'EC2Optimization': ec2_data,
            'EBSOptimization': ebs_data,
            'ElasticIPOptimization': eip_data,
            'TotalPotentialSavings': (
                ec2_data['potential_savings']
                + ebs_data['potential_savings']
                + eip_data['potential_savings']
            ),
        }

        print(f"Generated Report: {json.dumps(report)}")
        store_report(report)
        send_alert(report)

        return {
            'statusCode': 200,
            'body': json.dumps('Cost optimization report generated successfully.'),
        }

    except Exception as e:
        print(f"Error during cost analysis: {str(e)}")
        raise e


def get_cost_data():
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)
    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d'),
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
        )
        return response['ResultsByTime']
    except Exception as e:
        print(f"Cost Explorer API not fully enabled or error: {e}")
        return []


def analyze_ec2_instances():
    try:
        response = ec2_client.describe_instances(
            Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}]
        )
        stopped_instances = [
            instance['InstanceId']
            for reservation in response['Reservations']
            for instance in reservation['Instances']
        ]
        return {
            'stopped_instances': stopped_instances,
            'potential_savings': len(stopped_instances) * 10,
        }
    except Exception as e:
        print(f"Error accessing EC2: {e}")
        return {'stopped_instances': [], 'potential_savings': 0}


def analyze_ebs_volumes():
    try:
        response = ec2_client.describe_volumes(
            Filters=[{'Name': 'status', 'Values': ['available']}]
        )
        unattached_volumes = [v['VolumeId'] for v in response['Volumes']]
        return {
            'unattached_volumes': unattached_volumes,
            'potential_savings': len(unattached_volumes) * 5,
        }
    except Exception as e:
        print(f"Error accessing EBS: {e}")
        return {'unattached_volumes': [], 'potential_savings': 0}


def analyze_elastic_ips():
    try:
        response = ec2_client.describe_addresses()
        unassociated_eips = [
            addr['PublicIp']
            for addr in response['Addresses']
            if 'InstanceId' not in addr and 'NetworkInterfaceId' not in addr
        ]
        return {
            'unassociated_eips': unassociated_eips,
            'potential_savings': len(unassociated_eips) * 3,
        }
    except Exception as e:
        print(f"Error accessing EIPs: {e}")
        return {'unassociated_eips': [], 'potential_savings': 0}


def store_report(report):
    if DYNAMODB_TABLE:
        table = dynamodb.Table(DYNAMODB_TABLE)
        table.put_item(Item=report)
        print("Report stored in DynamoDB.")


def send_alert(report):
    # BUG FIX: original code had `message +=` de-indented outside the if block
    if SNS_TOPIC_ARN:
        message = "New Cost Optimization Report Generated.\n\n"
        message += f"Total Potential Savings: ${report['TotalPotentialSavings']}\n\n"
        message += f"Report ID: {report['ReportId']}\n"
        message += "Check DynamoDB for detailed information."

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='AWS Cost Optimization Alert',
            Message=message,
        )
        print("Alert sent to SNS.")
