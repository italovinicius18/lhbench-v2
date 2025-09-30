import json
import boto3
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to schedule and manage LHBench workloads.
    
    Handles:
    - Triggering Airflow DAGs via API
    - Managing benchmark execution schedules
    - Monitoring job progress
    - Coordinating multi-stage pipelines
    
    Event structure:
    {
        "action": "trigger_dag|monitor_dag|schedule_benchmark|status",
        "dag_id": "master_orchestrator_dag",
        "airflow_endpoint": "http://ec2-instance:8080",
        "auth": {"username": "admin", "password": "admin123"},
        "config": {...}  // DAG configuration
    }
    """
    
    try:
        # Extract event parameters
        action = event.get('action', 'status')
        dag_id = event.get('dag_id', 'master_orchestrator_dag')
        airflow_endpoint = event.get('airflow_endpoint', '')
        auth = event.get('auth', {})
        config = event.get('config', {})
        
        logger.info(f"Processing action: {action} for DAG: {dag_id}")
        
        # Process action
        result = {}
        
        if action == 'trigger_dag':
            result = trigger_airflow_dag(airflow_endpoint, dag_id, auth, config)
        elif action == 'monitor_dag':
            result = monitor_dag_execution(airflow_endpoint, dag_id, auth, event)
        elif action == 'schedule_benchmark':
            result = schedule_benchmark_workflow(event)
        elif action == 'status':
            result = get_system_status(airflow_endpoint, auth)
        elif action == 'cost_optimization':
            result = handle_cost_optimization(event)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Unknown action: {action}',
                    'valid_actions': ['trigger_dag', 'monitor_dag', 'schedule_benchmark', 'status', 'cost_optimization']
                })
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': action,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'dag_id': dag_id,
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

def trigger_airflow_dag(endpoint: str, dag_id: str, auth: Dict[str, str], config: Dict[str, Any]) -> Dict[str, Any]:
    """Trigger an Airflow DAG via REST API."""
    import requests
    from requests.auth import HTTPBasicAuth
    
    try:
        if not endpoint:
            raise ValueError("Airflow endpoint not provided")
        
        # Prepare API request
        url = f"{endpoint}/api/v1/dags/{dag_id}/dagRuns"
        
        auth_obj = None
        if auth.get('username') and auth.get('password'):
            auth_obj = HTTPBasicAuth(auth['username'], auth['password'])
        
        # Prepare DAG run configuration
        dag_run_data = {
            "dag_run_id": f"lambda_trigger_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            "conf": config
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Make API request
        response = requests.post(
            url,
            json=dag_run_data,
            headers=headers,
            auth=auth_obj,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            dag_run_info = response.json()
            logger.info(f"Successfully triggered DAG {dag_id}")
            
            return {
                'status': 'success',
                'dag_run_id': dag_run_info.get('dag_run_id'),
                'execution_date': dag_run_info.get('execution_date'),
                'state': dag_run_info.get('state'),
                'message': f'DAG {dag_id} triggered successfully'
            }
        else:
            logger.error(f"Failed to trigger DAG: {response.status_code} - {response.text}")
            return {
                'status': 'error',
                'status_code': response.status_code,
                'message': response.text
            }
            
    except Exception as e:
        logger.error(f"Error triggering DAG: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def monitor_dag_execution(endpoint: str, dag_id: str, auth: Dict[str, str], event: Dict[str, Any]) -> Dict[str, Any]:
    """Monitor the execution of a DAG run."""
    import requests
    from requests.auth import HTTPBasicAuth
    
    try:
        dag_run_id = event.get('dag_run_id')
        if not dag_run_id:
            # Get the latest DAG run
            dag_run_id = get_latest_dag_run(endpoint, dag_id, auth)
            
        if not dag_run_id:
            return {
                'status': 'error',
                'message': 'No DAG run found to monitor'
            }
        
        # Get DAG run details
        url = f"{endpoint}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        
        auth_obj = None
        if auth.get('username') and auth.get('password'):
            auth_obj = HTTPBasicAuth(auth['username'], auth['password'])
        
        response = requests.get(url, auth=auth_obj, timeout=30)
        
        if response.status_code == 200:
            dag_run_info = response.json()
            
            # Get task instances
            tasks_url = f"{endpoint}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
            tasks_response = requests.get(tasks_url, auth=auth_obj, timeout=30)
            
            tasks_info = []
            if tasks_response.status_code == 200:
                tasks_data = tasks_response.json()
                for task in tasks_data.get('task_instances', []):
                    tasks_info.append({
                        'task_id': task.get('task_id'),
                        'state': task.get('state'),
                        'start_date': task.get('start_date'),
                        'end_date': task.get('end_date'),
                        'duration': task.get('duration')
                    })
            
            return {
                'status': 'success',
                'dag_run_id': dag_run_id,
                'state': dag_run_info.get('state'),
                'start_date': dag_run_info.get('start_date'),
                'end_date': dag_run_info.get('end_date'),
                'execution_date': dag_run_info.get('execution_date'),
                'tasks': tasks_info,
                'task_summary': {
                    'total': len(tasks_info),
                    'success': len([t for t in tasks_info if t['state'] == 'success']),
                    'failed': len([t for t in tasks_info if t['state'] == 'failed']),
                    'running': len([t for t in tasks_info if t['state'] == 'running']),
                    'queued': len([t for t in tasks_info if t['state'] == 'queued'])
                }
            }
        else:
            return {
                'status': 'error',
                'status_code': response.status_code,
                'message': response.text
            }
            
    except Exception as e:
        logger.error(f"Error monitoring DAG: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def get_latest_dag_run(endpoint: str, dag_id: str, auth: Dict[str, str]) -> Optional[str]:
    """Get the latest DAG run ID."""
    import requests
    from requests.auth import HTTPBasicAuth
    
    try:
        url = f"{endpoint}/api/v1/dags/{dag_id}/dagRuns"
        
        auth_obj = None
        if auth.get('username') and auth.get('password'):
            auth_obj = HTTPBasicAuth(auth['username'], auth['password'])
        
        params = {
            'limit': 1,
            'order_by': '-execution_date'
        }
        
        response = requests.get(url, auth=auth_obj, params=params, timeout=30)
        
        if response.status_code == 200:
            dag_runs = response.json().get('dag_runs', [])
            if dag_runs:
                return dag_runs[0].get('dag_run_id')
        
        return None
        
    except Exception as e:
        logger.warning(f"Could not get latest DAG run: {str(e)}")
        return None

def schedule_benchmark_workflow(event: Dict[str, Any]) -> Dict[str, Any]:
    """Schedule a complete benchmark workflow."""
    try:
        # Get scheduling parameters
        benchmark_type = event.get('benchmark_type', 'full')  # full, delta_only, iceberg_only, hudi_only
        scale_factor = event.get('scale_factor', 1)
        formats = event.get('formats', ['delta', 'iceberg', 'hudi'])
        
        # Create EventBridge client for scheduling
        eventbridge = boto3.client('events')
        
        # Create unique workflow ID
        workflow_id = f"lhbench_workflow_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        # Define workflow stages
        workflow_stages = []
        
        # Stage 1: Generate bronze data
        workflow_stages.append({
            'stage': 'bronze_generation',
            'dag_id': '1_generate_bronze_tpch',
            'config': {
                'scale_factor': scale_factor
            },
            'delay_minutes': 0
        })
        
        # Stage 2: Convert to silver (parallel for each format)
        stage2_delay = 30  # Wait 30 minutes after bronze generation
        for fmt in formats:
            workflow_stages.append({
                'stage': f'silver_{fmt}',
                'dag_id': f'3_bronze_to_{fmt}',
                'config': {
                    'format': fmt,
                    'scale_factor': scale_factor
                },
                'delay_minutes': stage2_delay
            })
        
        # Stage 3: Benchmark execution
        workflow_stages.append({
            'stage': 'benchmark',
            'dag_id': '6_tpch_benchmark_gold',
            'config': {
                'formats': formats,
                'scale_factor': scale_factor,
                'benchmark_type': benchmark_type
            },
            'delay_minutes': 120  # Wait 2 hours for silver conversion
        })
        
        # Schedule each stage
        scheduled_events = []
        base_time = datetime.now(timezone.utc)
        
        for stage in workflow_stages:
            # Calculate execution time
            execution_time = base_time + timedelta(minutes=stage['delay_minutes'])
            
            # Create EventBridge rule
            rule_name = f"{workflow_id}_{stage['stage']}"
            
            # Schedule expression (one-time execution)
            schedule_expression = f"at({execution_time.strftime('%Y-%m-%dT%H:%M:%S')})"
            
            # Create rule
            eventbridge.put_rule(
                Name=rule_name,
                ScheduleExpression=schedule_expression,
                Description=f"LHBench workflow stage: {stage['stage']}",
                State='ENABLED'
            )
            
            # Add target (this Lambda function)
            target_input = {
                'action': 'trigger_dag',
                'dag_id': stage['dag_id'],
                'airflow_endpoint': event.get('airflow_endpoint', ''),
                'auth': event.get('auth', {}),
                'config': stage['config'],
                'workflow_id': workflow_id,
                'stage': stage['stage']
            }
            
            eventbridge.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': event.get('lambda_arn', ''),
                        'Input': json.dumps(target_input)
                    }
                ]
            )
            
            scheduled_events.append({
                'rule_name': rule_name,
                'stage': stage['stage'],
                'dag_id': stage['dag_id'],
                'execution_time': execution_time.isoformat(),
                'delay_minutes': stage['delay_minutes']
            })
        
        return {
            'status': 'success',
            'workflow_id': workflow_id,
            'benchmark_type': benchmark_type,
            'scale_factor': scale_factor,
            'formats': formats,
            'total_stages': len(workflow_stages),
            'scheduled_events': scheduled_events,
            'estimated_completion': (base_time + timedelta(hours=6)).isoformat(),
            'message': f'Scheduled {len(workflow_stages)} stages for workflow {workflow_id}'
        }
        
    except Exception as e:
        logger.error(f"Error scheduling benchmark workflow: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def get_system_status(endpoint: str, auth: Dict[str, str]) -> Dict[str, Any]:
    """Get overall system status from Airflow."""
    import requests
    from requests.auth import HTTPBasicAuth
    
    try:
        if not endpoint:
            return {
                'status': 'error',
                'message': 'Airflow endpoint not provided'
            }
        
        auth_obj = None
        if auth.get('username') and auth.get('password'):
            auth_obj = HTTPBasicAuth(auth['username'], auth['password'])
        
        # Get system health
        health_url = f"{endpoint}/health"
        health_response = requests.get(health_url, timeout=30)
        
        # Get DAGs status
        dags_url = f"{endpoint}/api/v1/dags"
        dags_response = requests.get(dags_url, auth=auth_obj, timeout=30)
        
        system_status = {
            'airflow_health': 'unknown',
            'api_accessible': False,
            'total_dags': 0,
            'active_dags': 0,
            'recent_dag_runs': []
        }
        
        # Check health
        if health_response.status_code == 200:
            system_status['airflow_health'] = 'healthy'
        
        # Check API and DAGs
        if dags_response.status_code == 200:
            system_status['api_accessible'] = True
            dags_data = dags_response.json()
            
            if 'dags' in dags_data:
                system_status['total_dags'] = len(dags_data['dags'])
                system_status['active_dags'] = len([
                    dag for dag in dags_data['dags'] 
                    if not dag.get('is_paused', True)
                ])
                
                # Get recent DAG runs for key DAGs
                key_dags = [
                    'master_orchestrator_dag',
                    '1_generate_bronze_tpch',
                    '6_tpch_benchmark_gold'
                ]
                
                for dag_id in key_dags:
                    try:
                        latest_run = get_latest_dag_run(endpoint, dag_id, auth)
                        if latest_run:
                            run_details = monitor_dag_execution(endpoint, dag_id, auth, {'dag_run_id': latest_run})
                            if run_details.get('status') == 'success':
                                system_status['recent_dag_runs'].append({
                                    'dag_id': dag_id,
                                    'dag_run_id': latest_run,
                                    'state': run_details.get('state'),
                                    'start_date': run_details.get('start_date')
                                })
                    except Exception as e:
                        logger.warning(f"Could not get status for DAG {dag_id}: {str(e)}")
        
        return {
            'status': 'success',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'system_status': system_status
        }
        
    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def handle_cost_optimization(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle cost optimization actions."""
    try:
        optimization_type = event.get('optimization_type', 'schedule_shutdown')
        
        if optimization_type == 'schedule_shutdown':
            # Schedule instance shutdown after benchmark completion
            return schedule_instance_shutdown(event)
        elif optimization_type == 'cleanup_resources':
            # Clean up old benchmark results and temporary files
            return cleanup_old_resources(event)
        elif optimization_type == 'optimize_storage':
            # Optimize S3 storage costs
            return optimize_s3_storage(event)
        else:
            return {
                'status': 'error',
                'message': f'Unknown optimization type: {optimization_type}'
            }
            
    except Exception as e:
        logger.error(f"Error handling cost optimization: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def schedule_instance_shutdown(event: Dict[str, Any]) -> Dict[str, Any]:
    """Schedule EC2 instance shutdown after benchmark completion."""
    try:
        # Get parameters
        delay_hours = event.get('delay_hours', 8)  # Default 8 hours
        instance_ids = event.get('instance_ids', [])
        
        if not instance_ids:
            return {
                'status': 'error',
                'message': 'No instance IDs provided for shutdown scheduling'
            }
        
        # Create EventBridge client
        eventbridge = boto3.client('events')
        
        # Calculate shutdown time
        shutdown_time = datetime.now(timezone.utc) + timedelta(hours=delay_hours)
        
        # Create shutdown rule
        rule_name = f"lhbench_shutdown_{int(shutdown_time.timestamp())}"
        schedule_expression = f"at({shutdown_time.strftime('%Y-%m-%dT%H:%M:%S')})"
        
        eventbridge.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            Description=f"LHBench automatic instance shutdown",
            State='ENABLED'
        )
        
        # Add target (instance manager Lambda)
        target_input = {
            'action': 'stop',
            'instance_ids': instance_ids,
            'reason': 'scheduled_shutdown_after_benchmark'
        }
        
        eventbridge.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': event.get('instance_manager_arn', ''),
                    'Input': json.dumps(target_input)
                }
            ]
        )
        
        return {
            'status': 'success',
            'rule_name': rule_name,
            'shutdown_time': shutdown_time.isoformat(),
            'delay_hours': delay_hours,
            'instance_count': len(instance_ids),
            'message': f'Scheduled shutdown of {len(instance_ids)} instances in {delay_hours} hours'
        }
        
    except Exception as e:
        logger.error(f"Error scheduling instance shutdown: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def cleanup_old_resources(event: Dict[str, Any]) -> Dict[str, Any]:
    """Clean up old benchmark results and temporary files."""
    try:
        # Get parameters
        retention_days = event.get('retention_days', 30)
        buckets = event.get('buckets', [])
        
        if not buckets:
            return {
                'status': 'error',
                'message': 'No S3 buckets specified for cleanup'
            }
        
        s3 = boto3.client('s3')
        cleanup_summary = []
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        
        for bucket in buckets:
            try:
                # List objects older than retention period
                response = s3.list_objects_v2(Bucket=bucket)
                
                objects_to_delete = []
                for obj in response.get('Contents', []):
                    if obj['LastModified'] < cutoff_date:
                        objects_to_delete.append({'Key': obj['Key']})
                
                # Delete old objects
                deleted_count = 0
                if objects_to_delete:
                    # Delete in batches of 1000
                    for i in range(0, len(objects_to_delete), 1000):
                        batch = objects_to_delete[i:i+1000]
                        s3.delete_objects(
                            Bucket=bucket,
                            Delete={'Objects': batch}
                        )
                        deleted_count += len(batch)
                
                cleanup_summary.append({
                    'bucket': bucket,
                    'deleted_objects': deleted_count,
                    'status': 'success'
                })
                
            except Exception as e:
                cleanup_summary.append({
                    'bucket': bucket,
                    'deleted_objects': 0,
                    'status': 'error',
                    'error': str(e)
                })
        
        total_deleted = sum(item['deleted_objects'] for item in cleanup_summary)
        
        return {
            'status': 'success',
            'retention_days': retention_days,
            'buckets_processed': len(buckets),
            'total_deleted_objects': total_deleted,
            'cleanup_summary': cleanup_summary,
            'message': f'Cleaned up {total_deleted} objects from {len(buckets)} buckets'
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up resources: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def optimize_s3_storage(event: Dict[str, Any]) -> Dict[str, Any]:
    """Optimize S3 storage costs by applying lifecycle policies."""
    try:
        buckets = event.get('buckets', [])
        
        if not buckets:
            return {
                'status': 'error',
                'message': 'No S3 buckets specified for optimization'
            }
        
        s3 = boto3.client('s3')
        optimization_summary = []
        
        # Define lifecycle policy
        lifecycle_policy = {
            'Rules': [
                {
                    'ID': 'LHBenchOptimization',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},
                    'Transitions': [
                        {
                            'Days': 30,
                            'StorageClass': 'STANDARD_IA'
                        },
                        {
                            'Days': 90,
                            'StorageClass': 'GLACIER'
                        },
                        {
                            'Days': 365,
                            'StorageClass': 'DEEP_ARCHIVE'
                        }
                    ],
                    'Expiration': {
                        'Days': 2555  # 7 years
                    }
                }
            ]
        }
        
        for bucket in buckets:
            try:
                # Apply lifecycle policy
                s3.put_bucket_lifecycle_configuration(
                    Bucket=bucket,
                    LifecycleConfiguration=lifecycle_policy
                )
                
                optimization_summary.append({
                    'bucket': bucket,
                    'status': 'success',
                    'message': 'Lifecycle policy applied successfully'
                })
                
            except Exception as e:
                optimization_summary.append({
                    'bucket': bucket,
                    'status': 'error',
                    'error': str(e)
                })
        
        return {
            'status': 'success',
            'buckets_processed': len(buckets),
            'optimization_summary': optimization_summary,
            'lifecycle_policy': lifecycle_policy,
            'message': f'Applied lifecycle policies to {len(buckets)} buckets'
        }
        
    except Exception as e:
        logger.error(f"Error optimizing S3 storage: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }