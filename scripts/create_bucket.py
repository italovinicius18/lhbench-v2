#!/usr/bin/env python3
"""
Script to create MinIO bucket
"""

import boto3
from botocore.exceptions import ClientError
import sys
import os

def main():
    # Get configuration from environment
    endpoint_url = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    bucket_name = os.getenv('BUCKET_NAME', 'lakehouse')
    
    try:
        # Create S3 client
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        # Try to create bucket
        s3.create_bucket(Bucket=bucket_name)
        print(f'✓ Bucket created: {bucket_name}')
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            print(f'✓ Bucket already exists: {bucket_name}')
        elif error_code == 'BucketAlreadyExists':
            print(f'✓ Bucket already exists: {bucket_name}')
        else:
            print(f'✗ Error creating bucket: {e}')
            sys.exit(1)
    except Exception as e:
        print(f'✗ Connection error: {e}')
        print('Make sure MinIO is running: make minio-start')
        sys.exit(1)

if __name__ == '__main__':
    main()
