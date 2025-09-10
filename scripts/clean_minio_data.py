#!/usr/bin/env python3
"""
Script to clean MinIO data
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
    
    print("Cleaning MinIO TPC-DS data:")
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
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"✓ Bucket '{bucket_name}' not found (nothing to clean)")
                return 0
            else:
                raise e
        
        # List and delete objects in tpcds-data prefix
        try:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix='tpcds-data/'
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                if objects_to_delete:
                    print(f"Deleting {len(objects_to_delete)} objects...")
                    
                    # Delete objects in batches
                    for i in range(0, len(objects_to_delete), 1000):
                        batch = objects_to_delete[i:i+1000]
                        s3.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': batch}
                        )
                    
                    print(f"✓ Deleted {len(objects_to_delete)} TPC-DS files from MinIO")
                else:
                    print("✓ No TPC-DS data found in MinIO (nothing to clean)")
            else:
                print("✓ No TPC-DS data found in MinIO (nothing to clean)")
                
        except ClientError as e:
            print(f"✗ Error cleaning MinIO data: {e}")
            return 1
            
    except NoCredentialsError:
        print("✗ MinIO credentials not found")
        print("  Check MINIO_ACCESS_KEY and MINIO_SECRET_KEY")
        return 1
    except Exception as e:
        print(f"✗ Error connecting to MinIO: {e}")
        print("  MinIO may not be running")
        return 1
    
    return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
