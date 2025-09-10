#!/usr/bin/env python3
"""
Script to generate TPC-DS data directly to MinIO
"""

import os
import sys
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lhbench.data import TPCDSGenerator

def check_existing_data_s3(scale_factor, minio_config):
    """Check if data already exists in MinIO"""
    bucket_name = minio_config['bucket']
    
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=minio_config['endpoint_url'],
            aws_access_key_id=minio_config['access_key'],
            aws_secret_access_key=minio_config['secret_key']
        )
        
        # Check if scale marker exists in MinIO
        try:
            s3.head_object(Bucket=bucket_name, Key=f'tpcds-data/sf_{scale_factor}/.scale_{scale_factor}')
            
            # Count data files
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f'tpcds-data/sf_{scale_factor}/',
                Delimiter='/'
            )
            
            if 'Contents' in response:
                data_files = [obj for obj in response['Contents'] 
                            if obj['Key'].endswith('.parquet')]
                if len(data_files) == 24:
                    print(f"✓ MinIO data exists for scale factor {scale_factor}")
                    print(f"  Bucket: {bucket_name}")
                    print(f"  Files: {len(data_files)} tables")
                    return True
                        
        except ClientError:
            pass  # Scale marker doesn't exist
                
    except Exception as e:
        print(f"⚠️  Could not check MinIO data: {e}")
    
    return False

def create_scale_marker_s3(bucket_name, scale_factor, minio_config):
    """Upload scale marker to MinIO"""
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=minio_config['endpoint_url'],
            aws_access_key_id=minio_config['access_key'],
            aws_secret_access_key=minio_config['secret_key']
        )
        
        marker_content = f"scale_factor={scale_factor}\ngenerated_at={os.environ.get('USER', 'unknown')}@{os.uname().nodename}\n"
        
        s3.put_object(
            Bucket=bucket_name,
            Key=f'tpcds-data/sf_{scale_factor}/.scale_{scale_factor}',
            Body=marker_content
        )
        print(f"✓ Created scale marker in MinIO")
        
    except Exception as e:
        print(f"⚠️  Could not create scale marker: {e}")

def main():
    # Get configuration from environment or command line
    scale_factor = float(os.getenv('SCALE_FACTOR', '1'))
    force_regenerate = os.getenv('FORCE_REGENERATE', 'false').lower() == 'true'
    
    # MinIO configuration
    minio_config = {
        'endpoint_url': os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'bucket': os.getenv('BUCKET_NAME', 'lakehouse')
    }
    bucket_name = minio_config['bucket']
    
    # Override with command line args if provided
    if len(sys.argv) > 1:
        scale_factor = float(sys.argv[1])
    if len(sys.argv) > 2:
        force_regenerate = sys.argv[2].lower() in ['true', 'force', 'yes']
    
    print(f"TPC-DS Data Generation (Direct MinIO):")
    print(f"  Scale factor: {scale_factor}")
    print(f"  Force regenerate: {force_regenerate}")
    print(f"  MinIO endpoint: {minio_config['endpoint_url']}")
    print(f"  Bucket: {bucket_name}")
    print()
    
    # Check if data already exists (unless force regenerate)
    if not force_regenerate:
        print("Checking existing data in MinIO...")
        if check_existing_data_s3(scale_factor, minio_config):
            print(f"\n✅ Data already exists for scale factor {scale_factor}")
            print("   Use FORCE_REGENERATE=true to regenerate")
            return 0
    
    # Generate data directly to MinIO
    return generate_data_direct_s3(scale_factor, minio_config)

def generate_data_direct_s3(scale_factor, minio_config):
    """Generate data directly to S3/MinIO"""
    bucket_name = minio_config['bucket']
    s3_path = f"s3://{bucket_name}/tpcds-data/sf_{scale_factor}"
    
    print(f"🚀 Generating TPC-DS data directly to MinIO...")
    print(f"  S3 Path: {s3_path}")
    print(f"  Endpoint: {minio_config['endpoint_url']}")
    
    try:
        # Generate data directly to S3
        with TPCDSGenerator(s3_path, s3_config=minio_config) as generator:
            data_files = generator.generate_data(scale_factor=scale_factor)
        
        # Create scale marker in S3
        create_scale_marker_s3(bucket_name, scale_factor, minio_config)
        
        print('✅ TPC-DS data generated directly to MinIO')
        print(f'📊 Generated {len(data_files)} tables at scale factor {scale_factor}')
        return 0
        
    except Exception as e:
        print(f'❌ Error generating data directly to MinIO: {e}')
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
