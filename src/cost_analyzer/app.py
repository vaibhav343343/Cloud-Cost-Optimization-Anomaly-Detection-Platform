import json
import os
import boto3
import uuid
from datetime import datetime, timedelta

# Initialize AWS clients
ce_client = boto3.client('ce')
ec2_client = boto3.client('ec2')
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def lambda_handler(event, context):
    try:
        print("Starting cost analysis...")
        
        # 1. Fetch data from Cost Explorer API
        cost_data = get_cost_data()
        
        # 2. Analyze EC2 Instances
        ec2_data = analyze_ec2_instances()
        
        # 3. Analyze EBS Volumes
        ebs_data = analyze_ebs_volumes()
        
        # 4. Analyze Elastic IPs
        eip_data = analyze_elastic_ips()
        
        # Generate Report
        report = {
            'ReportId': str(uuid.uuid4()),
            'Timestamp': datetime.utcnow().isoformat(),
            'CostExplorerSummary': cost_data,
            'EC2Optimization': ec2_data,
            'EBSOptimization': ebs_data,
            'ElasticIPOptimization': eip_data,
            'TotalPotentialSavings': ec2_data['potential_savings'] + ebs_data['potential_savings'] + eip_data['potential_savings']
        }
        
        print(f"Generated Report: {json.dumps(report)}")
        
        # Store Report in DynamoDB
        store_report(report)
        
        # Send Email Alert via SNS
        send_alert(report)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Cost optimization report generated successfully.')
        }
        
    except Exception as e:
        print(f"Error during cost analysis: {str(e)}")
        raise e

def get_cost_data():
    # Example implementation for getting last 30 days cost
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)
    
    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d')
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost']
        )
        return response['ResultsByTime']
    except Exception as e:
        print(f"Cost Explorer API not fully enabled or error: {e}")
        return []

def analyze_ec2_instances():
    # Identify underutilized or stopped instances
    try:
        response = ec2_client.describe_instances(
            Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}]
        )
        stopped_instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                stopped_instances.append(instance['InstanceId'])
                
        return {
            'stopped_instances': stopped_instances,
            'potential_savings': len(stopped_instances) * 10 # Example estimate
        }
    except Exception as e:
        print(f"Error accessing EC2: {e}")
        return {'stopped_instances': [], 'potential_savings': 0}

def analyze_ebs_volumes():
    # Identify unattached volumes
    try:
        response = ec2_client.describe_volumes(
            Filters=[{'Name': 'status', 'Values': ['available']}]
        )
        unattached_volumes = []
        for volume in response['Volumes']:
            unattached_volumes.append(volume['VolumeId'])
            
        return {
            'unattached_volumes': unattached_volumes,
            'potential_savings': len(unattached_volumes) * 5 # Example estimate
        }
    except Exception as e:
        print(f"Error accessing EBS: {e}")
        return {'unattached_volumes': [], 'potential_savings': 0}

def analyze_elastic_ips():
    # Identify unassociated Elastic IPs
    try:
        response = ec2_client.describe_addresses()
        unassociated_eips = []
        for address in response['Addresses']:
            if 'InstanceId' not in address and 'NetworkInterfaceId' not in address:
                unassociated_eips.append(address['PublicIp'])
                
        return {
            'unassociated_eips': unassociated_eips,
            'potential_savings': len(unassociated_eips) * 3 # Example estimate
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
    if SNS_TOPIC_ARN:
        message = f"New Cost Optimization Report Generated.\n\n"
        message += f"Total Potential Savings: ${report['TotalPotentialSavings']}\n\n"
        message += f"Report ID: {report['ReportId']}\n"
        message += "Check DynamoDB for detailed information."
        
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='AWS Cost Optimization Alert',
            Message=message
        )
        print("Alert sent to SNS.")
