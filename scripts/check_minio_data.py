#!/usr/bin/env python3
"""
Script to check MinIO data status
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def main():
    # Get MinIO configuration
    endpoint_url = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    bucket_name = os.getenv('BUCKET_NAME', 'lakehouse')
    
    print("MinIO Data Status:")
    print(f"  Endpoint: {endpoint_url}")
    print(f"  Bucket: {bucket_name}")
    print()
    
    try:
        # Create S3 client
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"✓ Bucket '{bucket_name}' exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"✗ Bucket '{bucket_name}' not found")
                print("  Run: make minio-bucket")
                sys.exit(1)
            else:
                raise e
        
        # List objects in tpcds-data prefix
        try:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix='tpcds-data/'
            )
            
            if 'Contents' in response:
                objects = response['Contents']
                total_size = sum(obj['Size'] for obj in objects)
                
                print(f"✓ Found {len(objects)} TPC-DS files")
                print(f"  Total size: {total_size / (1024*1024):.1f} MB")
                print("  Files:")
                
                for obj in sorted(objects, key=lambda x: x['Key']):
                    filename = obj['Key'].replace('tpcds-data/', '')
                    size_mb = obj['Size'] / (1024*1024)
                    print(f"    {filename:<25} {size_mb:>6.1f} MB")
                    
            else:
                print("✗ No TPC-DS data found in MinIO")
                print("  Run: make data-generate")
                
        except ClientError as e:
            print(f"✗ Error listing objects: {e}")
            sys.exit(1)
            
    except NoCredentialsError:
        print("✗ MinIO credentials not found")
        print("  Check MINIO_ACCESS_KEY and MINIO_SECRET_KEY")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error connecting to MinIO: {e}")
        print("  Check if MinIO is running: make minio-status")
        sys.exit(1)

if __name__ == '__main__':
    main()
