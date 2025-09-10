"""
MinIO client for S3-compatible storage operations
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import structlog

logger = structlog.get_logger(__name__)


class MinIOClient:
    """
    Client for interacting with MinIO S3-compatible storage
    """
    
    def __init__(self, config: Dict):
        """
        Initialize MinIO client
        
        Args:
            config: Configuration dictionary with MinIO settings
        """
        self.config = config
        self.endpoint_url = config.get("endpoint_url", "http://localhost:9000")
        self.access_key = config.get("access_key", "admin")
        self.secret_key = config.get("secret_key", "password123")
        self.region = config.get("region", "us-east-1")
        
        # Initialize S3 client
        self.s3_client = self._create_s3_client()
        
    def _create_s3_client(self):
        """Create boto3 S3 client for MinIO"""
        try:
            client = boto3.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                use_ssl=False,
                config=boto3.session.Config(
                    signature_version='s3v4',
                    s3={
                        'addressing_style': 'path'
                    }
                )
            )
            
            # Test connection
            client.list_buckets()
            logger.info("MinIO client connected successfully", endpoint=self.endpoint_url)
            
            return client
            
        except Exception as e:
            logger.error("Failed to connect to MinIO", endpoint=self.endpoint_url, error=str(e))
            raise
            
    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a bucket if it doesn't exist
        
        Args:
            bucket_name: Name of the bucket to create
            
        Returns:
            True if bucket was created or already exists
        """
        try:
            # Check if bucket already exists
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info("Bucket already exists", bucket=bucket_name)
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(Bucket=bucket_name)
                    logger.info("Created bucket", bucket=bucket_name)
                    return True
                    
                except ClientError as create_error:
                    logger.error("Failed to create bucket", 
                               bucket=bucket_name, 
                               error=str(create_error))
                    return False
                    
            else:
                logger.error("Error checking bucket", 
                           bucket=bucket_name, 
                           error=str(e))
                return False
                
    def upload_file(self, local_file: str, bucket_name: str, s3_key: str) -> bool:
        """
        Upload a file to MinIO
        
        Args:
            local_file: Path to local file
            bucket_name: Target bucket name
            s3_key: S3 key (path) for the file
            
        Returns:
            True if successful
        """
        try:
            local_path = Path(local_file)
            if not local_path.exists():
                logger.error("Local file not found", file=local_file)
                return False
                
            self.s3_client.upload_file(str(local_path), bucket_name, s3_key)
            logger.info("Uploaded file", 
                       local_file=local_file, 
                       bucket=bucket_name, 
                       s3_key=s3_key)
            return True
            
        except Exception as e:
            logger.error("Failed to upload file", 
                        local_file=local_file,
                        bucket=bucket_name,
                        s3_key=s3_key,
                        error=str(e))
            return False
            
    def download_file(self, bucket_name: str, s3_key: str, local_file: str) -> bool:
        """
        Download a file from MinIO
        
        Args:
            bucket_name: Source bucket name
            s3_key: S3 key (path) of the file
            local_file: Local destination path
            
        Returns:
            True if successful
        """
        try:
            local_path = Path(local_file)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(bucket_name, s3_key, str(local_path))
            logger.info("Downloaded file",
                       bucket=bucket_name,
                       s3_key=s3_key,
                       local_file=local_file)
            return True
            
        except Exception as e:
            logger.error("Failed to download file",
                        bucket=bucket_name,
                        s3_key=s3_key,
                        local_file=local_file,
                        error=str(e))
            return False
            
    def upload_directory(self, local_dir: str, bucket_name: str, s3_prefix: str = "") -> bool:
        """
        Upload a directory recursively to MinIO
        
        Args:
            local_dir: Path to local directory
            bucket_name: Target bucket name
            s3_prefix: S3 prefix (directory path)
            
        Returns:
            True if successful
        """
        try:
            local_path = Path(local_dir)
            if not local_path.is_dir():
                logger.error("Local directory not found", dir=local_dir)
                return False
                
            success_count = 0
            total_files = 0
            
            for file_path in local_path.rglob("*"):
                if file_path.is_file():
                    total_files += 1
                    
                    # Calculate relative path for S3 key
                    relative_path = file_path.relative_to(local_path)
                    s3_key = f"{s3_prefix}/{relative_path}".lstrip("/")
                    
                    if self.upload_file(str(file_path), bucket_name, s3_key):
                        success_count += 1
                        
            logger.info("Directory upload completed",
                       local_dir=local_dir,
                       bucket=bucket_name,
                       successful=success_count,
                       total=total_files)
                       
            return success_count == total_files
            
        except Exception as e:
            logger.error("Failed to upload directory",
                        local_dir=local_dir,
                        bucket=bucket_name,
                        error=str(e))
            return False
            
    def list_objects(self, bucket_name: str, prefix: str = "") -> List[Dict]:
        """
        List objects in a bucket with optional prefix
        
        Args:
            bucket_name: Bucket name
            prefix: S3 prefix to filter objects
            
        Returns:
            List of object metadata dictionaries
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            objects = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag'].strip('"')
                        })
                        
            logger.info("Listed objects",
                       bucket=bucket_name,
                       prefix=prefix,
                       count=len(objects))
                       
            return objects
            
        except Exception as e:
            logger.error("Failed to list objects",
                        bucket=bucket_name,
                        prefix=prefix,
                        error=str(e))
            return []
            
    def delete_object(self, bucket_name: str, s3_key: str) -> bool:
        """
        Delete an object from MinIO
        
        Args:
            bucket_name: Bucket name
            s3_key: S3 key of object to delete
            
        Returns:
            True if successful
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            logger.info("Deleted object", bucket=bucket_name, s3_key=s3_key)
            return True
            
        except Exception as e:
            logger.error("Failed to delete object",
                        bucket=bucket_name,
                        s3_key=s3_key,
                        error=str(e))
            return False
            
    def delete_objects_by_prefix(self, bucket_name: str, prefix: str) -> int:
        """
        Delete all objects with a given prefix
        
        Args:
            bucket_name: Bucket name
            prefix: S3 prefix of objects to delete
            
        Returns:
            Number of objects deleted
        """
        try:
            objects_to_delete = self.list_objects(bucket_name, prefix)
            
            if not objects_to_delete:
                logger.info("No objects found to delete", bucket=bucket_name, prefix=prefix)
                return 0
                
            # Delete in batches of 1000 (AWS S3 limit)
            deleted_count = 0
            batch_size = 1000
            
            for i in range(0, len(objects_to_delete), batch_size):
                batch = objects_to_delete[i:i + batch_size]
                delete_keys = [{'Key': obj['key']} for obj in batch]
                
                response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        'Objects': delete_keys,
                        'Quiet': True
                    }
                )
                
                deleted_count += len(delete_keys)
                
            logger.info("Deleted objects by prefix",
                       bucket=bucket_name,
                       prefix=prefix,
                       deleted_count=deleted_count)
                       
            return deleted_count
            
        except Exception as e:
            logger.error("Failed to delete objects by prefix",
                        bucket=bucket_name,
                        prefix=prefix,
                        error=str(e))
            return 0
            
    def get_bucket_size(self, bucket_name: str, prefix: str = "") -> Dict[str, int]:
        """
        Get size information for a bucket or prefix
        
        Args:
            bucket_name: Bucket name
            prefix: S3 prefix to filter objects
            
        Returns:
            Dictionary with size and count information
        """
        try:
            objects = self.list_objects(bucket_name, prefix)
            
            total_size = sum(obj['size'] for obj in objects)
            object_count = len(objects)
            
            result = {
                'total_size_bytes': total_size,
                'total_size_mb': total_size / (1024 * 1024),
                'total_size_gb': total_size / (1024 * 1024 * 1024),
                'object_count': object_count
            }
            
            logger.info("Calculated bucket size",
                       bucket=bucket_name,
                       prefix=prefix,
                       **result)
                       
            return result
            
        except Exception as e:
            logger.error("Failed to get bucket size",
                        bucket=bucket_name,
                        prefix=prefix,
                        error=str(e))
            return {
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'total_size_gb': 0,
                'object_count': 0
            }
            
    def health_check(self) -> Dict[str, bool]:
        """
        Perform health check on MinIO connection
        
        Returns:
            Dictionary with health check results
        """
        health = {
            'connection': False,
            'bucket_operations': False,
            'object_operations': False
        }
        
        try:
            # Test connection
            self.s3_client.list_buckets()
            health['connection'] = True
            
            # Test bucket operations
            test_bucket = "health-check-bucket"
            self.create_bucket(test_bucket)
            health['bucket_operations'] = True
            
            # Test object operations  
            test_key = "health-check.txt"
            test_content = b"health check content"
            
            # Upload test object
            self.s3_client.put_object(
                Bucket=test_bucket,
                Key=test_key,
                Body=test_content
            )
            
            # Download test object
            response = self.s3_client.get_object(Bucket=test_bucket, Key=test_key)
            downloaded_content = response['Body'].read()
            
            if downloaded_content == test_content:
                health['object_operations'] = True
                
            # Clean up test objects
            self.s3_client.delete_object(Bucket=test_bucket, Key=test_key)
            
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            
        logger.info("Health check completed", **health)
        return health