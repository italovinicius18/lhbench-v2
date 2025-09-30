import json
import boto3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to manage EC2 instances for LHBench system.
    
    Handles:
    - Scheduled start/stop of instances
    - Cost optimization
    - Status monitoring
    - Automatic scaling based on workload
    
    Event structure:
    {
        "action": "start|stop|status|scale",
        "instance_ids": ["i-1234567890abcdef0"],
        "project_name": "lhbench-tpch",
        "environment": "prod"
    }
    """
    
    try:
        # Initialize AWS clients
        ec2 = boto3.client('ec2')
        cloudwatch = boto3.client('cloudwatch')
        ssm = boto3.client('ssm')
        
        # Extract event parameters
        action = event.get('action', 'status')
        instance_ids = event.get('instance_ids', [])
        project_name = event.get('project_name', 'lhbench-tpch')
        environment = event.get('environment', 'prod')
        
        logger.info(f"Processing action: {action} for project: {project_name}")
        
        # If no instance IDs provided, find instances by tags
        if not instance_ids:
            instance_ids = find_instances_by_tags(ec2, project_name, environment)
            
        if not instance_ids:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'No instances found for the specified project and environment',
                    'project_name': project_name,
                    'environment': environment
                })
            }
        
        # Process action
        result = {}
        
        if action == 'start':
            result = start_instances(ec2, instance_ids, project_name)
        elif action == 'stop':
            result = stop_instances(ec2, instance_ids, project_name)
        elif action == 'status':
            result = get_instances_status(ec2, cloudwatch, instance_ids)
        elif action == 'scale':
            result = handle_scaling(ec2, cloudwatch, event, project_name, environment)
        elif action == 'cost_report':
            result = generate_cost_report(ec2, cloudwatch, instance_ids, project_name)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Unknown action: {action}',
                    'valid_actions': ['start', 'stop', 'status', 'scale', 'cost_report']
                })
            }
        
        # Log metrics to CloudWatch
        log_metrics_to_cloudwatch(cloudwatch, action, len(instance_ids), project_name)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': action,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'project_name': project_name,
                'environment': environment,
                'instance_count': len(instance_ids),
                'result': result
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

def find_instances_by_tags(ec2, project_name: str, environment: str) -> List[str]:
    """Find EC2 instances by project and environment tags."""
    try:
        response = ec2.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Project',
                    'Values': [project_name]
                },
                {
                    'Name': 'tag:Environment', 
                    'Values': [environment]
                },
                {
                    'Name': 'instance-state-name',
                    'Values': ['pending', 'running', 'stopping', 'stopped']
                }
            ]
        )
        
        instance_ids = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_ids.append(instance['InstanceId'])
                
        logger.info(f"Found {len(instance_ids)} instances for project {project_name}")
        return instance_ids
        
    except Exception as e:
        logger.error(f"Error finding instances: {str(e)}")
        return []

def start_instances(ec2, instance_ids: List[str], project_name: str) -> Dict[str, Any]:
    """Start EC2 instances."""
    try:
        # Check current state
        current_states = {}
        response = ec2.describe_instances(InstanceIds=instance_ids)
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                current_states[instance['InstanceId']] = instance['State']['Name']
        
        # Start stopped instances
        instances_to_start = [
            iid for iid, state in current_states.items() 
            if state == 'stopped'
        ]
        
        started_instances = []
        if instances_to_start:
            start_response = ec2.start_instances(InstanceIds=instances_to_start)
            started_instances = [
                inst['InstanceId'] for inst in start_response['StartingInstances']
            ]
            logger.info(f"Started {len(started_instances)} instances")
        
        return {
            'action': 'start',
            'total_instances': len(instance_ids),
            'already_running': len([s for s in current_states.values() if s == 'running']),
            'started': len(started_instances),
            'started_instances': started_instances,
            'current_states': current_states
        }
        
    except Exception as e:
        logger.error(f"Error starting instances: {str(e)}")
        raise

def stop_instances(ec2, instance_ids: List[str], project_name: str) -> Dict[str, Any]:
    """Stop EC2 instances."""
    try:
        # Check current state
        current_states = {}
        response = ec2.describe_instances(InstanceIds=instance_ids)
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                current_states[instance['InstanceId']] = instance['State']['Name']
        
        # Stop running instances
        instances_to_stop = [
            iid for iid, state in current_states.items() 
            if state == 'running'
        ]
        
        stopped_instances = []
        if instances_to_stop:
            stop_response = ec2.stop_instances(InstanceIds=instances_to_stop)
            stopped_instances = [
                inst['InstanceId'] for inst in stop_response['StoppingInstances']
            ]
            logger.info(f"Stopped {len(stopped_instances)} instances")
        
        return {
            'action': 'stop',
            'total_instances': len(instance_ids),
            'already_stopped': len([s for s in current_states.values() if s == 'stopped']),
            'stopped': len(stopped_instances),
            'stopped_instances': stopped_instances,
            'current_states': current_states
        }
        
    except Exception as e:
        logger.error(f"Error stopping instances: {str(e)}")
        raise

def get_instances_status(ec2, cloudwatch, instance_ids: List[str]) -> Dict[str, Any]:
    """Get detailed status of EC2 instances."""
    try:
        # Get instance details
        response = ec2.describe_instances(InstanceIds=instance_ids)
        
        instances_info = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                # Get CPU utilization from CloudWatch
                cpu_utilization = get_cpu_utilization(
                    cloudwatch, 
                    instance['InstanceId']
                )
                
                instance_info = {
                    'instance_id': instance['InstanceId'],
                    'instance_type': instance['InstanceType'],
                    'state': instance['State']['Name'],
                    'launch_time': instance.get('LaunchTime'),
                    'public_ip': instance.get('PublicIpAddress'),
                    'private_ip': instance.get('PrivateIpAddress'),
                    'cpu_utilization': cpu_utilization,
                    'tags': {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                }
                instances_info.append(instance_info)
        
        # Calculate summary
        states_summary = {}
        for info in instances_info:
            state = info['state']
            states_summary[state] = states_summary.get(state, 0) + 1
        
        return {
            'action': 'status',
            'total_instances': len(instance_ids),
            'states_summary': states_summary,
            'instances': instances_info
        }
        
    except Exception as e:
        logger.error(f"Error getting instances status: {str(e)}")
        raise

def get_cpu_utilization(cloudwatch, instance_id: str) -> float:
    """Get average CPU utilization for the last hour."""
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time.replace(hour=end_time.hour-1)
        
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='CPUUtilization',
            Dimensions=[
                {
                    'Name': 'InstanceId',
                    'Value': instance_id
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,  # 1 hour
            Statistics=['Average']
        )
        
        if response['Datapoints']:
            return round(response['Datapoints'][0]['Average'], 2)
        return 0.0
        
    except Exception as e:
        logger.warning(f"Could not get CPU utilization for {instance_id}: {str(e)}")
        return 0.0

def handle_scaling(ec2, cloudwatch, event: Dict[str, Any], project_name: str, environment: str) -> Dict[str, Any]:
    """Handle auto-scaling based on workload."""
    try:
        # Get scaling parameters
        min_instances = event.get('min_instances', 1)
        max_instances = event.get('max_instances', 3)
        cpu_threshold_up = event.get('cpu_threshold_up', 80.0)
        cpu_threshold_down = event.get('cpu_threshold_down', 20.0)
        
        # Get current instances
        instance_ids = find_instances_by_tags(ec2, project_name, environment)
        running_instances = []
        
        if instance_ids:
            status = get_instances_status(ec2, cloudwatch, instance_ids)
            running_instances = [
                inst for inst in status['instances'] 
                if inst['state'] == 'running'
            ]
        
        current_count = len(running_instances)
        avg_cpu = 0.0
        
        if running_instances:
            avg_cpu = sum(inst['cpu_utilization'] for inst in running_instances) / len(running_instances)
        
        action_taken = "none"
        message = f"Current instances: {current_count}, Average CPU: {avg_cpu:.1f}%"
        
        # Scaling logic
        if avg_cpu > cpu_threshold_up and current_count < max_instances:
            # Scale up - start stopped instances or launch new ones
            stopped_instances = [
                inst['instance_id'] for inst in status.get('instances', [])
                if inst['state'] == 'stopped'
            ]
            
            if stopped_instances:
                # Start one stopped instance
                start_result = start_instances(ec2, stopped_instances[:1], project_name)
                action_taken = "scale_up_existing"
                message += f" - Started 1 stopped instance"
            else:
                # Would need to launch new instance - not implemented here
                action_taken = "scale_up_needed"
                message += f" - Need to launch new instance (not implemented)"
                
        elif avg_cpu < cpu_threshold_down and current_count > min_instances:
            # Scale down - stop least utilized instance
            if running_instances:
                # Find instance with lowest CPU utilization
                least_utilized = min(running_instances, key=lambda x: x['cpu_utilization'])
                stop_result = stop_instances(ec2, [least_utilized['instance_id']], project_name)
                action_taken = "scale_down"
                message += f" - Stopped instance {least_utilized['instance_id']}"
        
        return {
            'action': 'scale',
            'current_instances': current_count,
            'average_cpu': avg_cpu,
            'action_taken': action_taken,
            'message': message,
            'thresholds': {
                'cpu_up': cpu_threshold_up,
                'cpu_down': cpu_threshold_down,
                'min_instances': min_instances,
                'max_instances': max_instances
            }
        }
        
    except Exception as e:
        logger.error(f"Error handling scaling: {str(e)}")
        raise

def generate_cost_report(ec2, cloudwatch, instance_ids: List[str], project_name: str) -> Dict[str, Any]:
    """Generate cost report for instances."""
    try:
        # Instance pricing (approximate USD per hour)
        pricing = {
            'm6gd.large': 0.0864,
            'm6gd.xlarge': 0.1728,
            'm6gd.2xlarge': 0.3456,
            'm6gd.4xlarge': 0.6912,
            'm6gd.8xlarge': 1.3824,
            'm6gd.12xlarge': 2.0736,
            'm6gd.16xlarge': 2.7648
        }
        
        status = get_instances_status(ec2, cloudwatch, instance_ids)
        
        total_hourly_cost = 0.0
        instance_costs = []
        
        for instance in status['instances']:
            instance_type = instance['instance_type']
            hourly_cost = pricing.get(instance_type, 0.0)
            total_hourly_cost += hourly_cost
            
            # Calculate uptime if running
            uptime_hours = 0
            if instance['state'] == 'running' and instance.get('launch_time'):
                launch_time = instance['launch_time']
                if isinstance(launch_time, str):
                    launch_time = datetime.fromisoformat(launch_time.replace('Z', '+00:00'))
                uptime_hours = (datetime.now(timezone.utc) - launch_time).total_seconds() / 3600
            
            instance_costs.append({
                'instance_id': instance['instance_id'],
                'instance_type': instance_type,
                'state': instance['state'],
                'hourly_cost': hourly_cost,
                'uptime_hours': round(uptime_hours, 2),
                'cost_today': round(hourly_cost * min(uptime_hours, 24), 2)
            })
        
        return {
            'action': 'cost_report',
            'project_name': project_name,
            'total_instances': len(instance_ids),
            'total_hourly_cost': round(total_hourly_cost, 2),
            'estimated_daily_cost': round(total_hourly_cost * 24, 2),
            'estimated_monthly_cost': round(total_hourly_cost * 24 * 30, 2),
            'instance_breakdown': instance_costs,
            'report_time': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error generating cost report: {str(e)}")
        raise

def log_metrics_to_cloudwatch(cloudwatch, action: str, instance_count: int, project_name: str):
    """Log custom metrics to CloudWatch."""
    try:
        cloudwatch.put_metric_data(
            Namespace=f'LHBench/{project_name}',
            MetricData=[
                {
                    'MetricName': 'LambdaInvocations',
                    'Dimensions': [
                        {
                            'Name': 'Action',
                            'Value': action
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(timezone.utc)
                },
                {
                    'MetricName': 'ManagedInstances',
                    'Dimensions': [
                        {
                            'Name': 'Action',
                            'Value': action
                        }
                    ],
                    'Value': instance_count,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(timezone.utc)
                }
            ]
        )
    except Exception as e:
        logger.warning(f"Could not log metrics to CloudWatch: {str(e)}")