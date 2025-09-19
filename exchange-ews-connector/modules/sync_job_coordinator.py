"""
Sync Job Coordinator Module
Manages distributed Q Business sync jobs across multiple containers using DynamoDB
"""

import boto3
import time
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from botocore.exceptions import ClientError
from .security_utils import sanitize_for_logging, handle_error_securely

logger = logging.getLogger(__name__)

class SyncJobCoordinator:
    """
    Coordinates Q Business sync jobs across multiple containers using DynamoDB.
    Only one sync job can run at a time, but multiple containers can register
    their intent to process data within that single sync job.
    """
    
    def __init__(self, config, qbusiness_client):
        self.config = config
        self.qbusiness_client = qbusiness_client
        self.dynamodb = boto3.resource('dynamodb')
        
        # Use a separate table for sync job coordination
        self.sync_table_name = f"{config.table_name}-sync-jobs"
        self.sync_table = None
        
        # Container identification
        self.container_id = str(uuid.uuid4())
        self.container_name = f"container-{self.container_id[:8]}"
        
        # Current sync job tracking
        self.current_sync_job_id = None
        self.is_sync_job_owner = False
        
        logger.info(f"Initialized SyncJobCoordinator with container ID: {self.container_name}")
    
    def _ensure_sync_table_exists(self):
        """Ensure the sync coordination table exists"""
        if self.sync_table is None:
            try:
                table = self.dynamodb.Table(self.sync_table_name)
                table.load()
                logger.info(f"Using existing sync coordination table: {self.sync_table_name}")
                self.sync_table = table
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ResourceNotFoundException':
                    logger.info(f"Creating sync coordination table: {self.sync_table_name}")
                    self.sync_table = self._create_sync_table()
                else:
                    error_msg = handle_error_securely(e, f"accessing sync table '{self.sync_table_name}'")
                    raise Exception(error_msg)
    
    def _create_sync_table(self):
        """Create the sync coordination DynamoDB table"""
        try:
            logger.info(f"Creating sync coordination table: {self.sync_table_name}")
            
            table = self.dynamodb.create_table(
                TableName=self.sync_table_name,
                KeySchema=[
                    {
                        'AttributeName': 'job_type',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'job_id',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'job_type',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'job_id',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST',
                Tags=[
                    {
                        'Key': 'Application',
                        'Value': 'ExchangeConnector'
                    },
                    {
                        'Key': 'Environment',
                        'Value': self.config.environment
                    },
                    {
                        'Key': 'Purpose',
                        'Value': 'SyncJobCoordination'
                    }
                ]
            )
            
            logger.info(f"Waiting for sync table {self.sync_table_name} to become active...")
            table.wait_until_exists()
            time.sleep(2)  # Additional wait for full readiness
            
            logger.info(f"✅ Sync coordination table {self.sync_table_name} created successfully")
            return table
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceInUseException':
                # Table already exists (race condition)
                logger.info(f"Sync table {self.sync_table_name} already exists")
                table = self.dynamodb.Table(self.sync_table_name)
                table.load()
                return table
            else:
                raise Exception(f"Error creating sync table '{self.sync_table_name}': {e}")
    
    def register_container(self, sync_job_id: str) -> bool:
        """
        Register this container as actively processing within a sync job.
        
        Args:
            sync_job_id: The Q Business sync job ID
            
        Returns:
            bool: True if registration successful
        """
        try:
            self._ensure_sync_table_exists()
            
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Register container as active in this sync job
            self.sync_table.put_item(
                Item={
                    'job_type': 'CONTAINER',
                    'job_id': f"{sync_job_id}#{self.container_id}",
                    'sync_job_id': sync_job_id,
                    'container_id': self.container_id,
                    'container_name': self.container_name,
                    'status': 'ACTIVE',
                    'registered_at': current_time,
                    'last_heartbeat': current_time,
                    'ttl': int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())
                }
            )
            
            logger.info(f"✅ Container {self.container_name} registered for sync job {sync_job_id}")
            return True
            
        except ClientError as e:
            error_msg = handle_error_securely(e, f"registering container {self.container_name}")
            logger.error(error_msg)
            return False
    
    def update_heartbeat(self, sync_job_id: str) -> bool:
        """
        Update heartbeat for this container to show it's still active.
        
        Args:
            sync_job_id: The Q Business sync job ID
            
        Returns:
            bool: True if heartbeat updated successfully
        """
        try:
            self._ensure_sync_table_exists()
            
            current_time = datetime.now(timezone.utc).isoformat()
            
            self.sync_table.update_item(
                Key={
                    'job_type': 'CONTAINER',
                    'job_id': f"{sync_job_id}#{self.container_id}"
                },
                UpdateExpression='SET last_heartbeat = :heartbeat, #ttl = :ttl',
                ExpressionAttributeNames={
                    '#ttl': 'ttl'
                },
                ExpressionAttributeValues={
                    ':heartbeat': current_time,
                    ':ttl': int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())
                }
            )
            
            return True
            
        except ClientError as e:
            # Don't log heartbeat failures as errors - they're expected during normal operation
            logger.debug(f"Heartbeat update failed for container {self.container_name}: {e}")
            return False
    
    def unregister_container(self, sync_job_id: str) -> bool:
        """
        Unregister this container from the sync job.
        
        Args:
            sync_job_id: The Q Business sync job ID
            
        Returns:
            bool: True if unregistration successful
        """
        try:
            self._ensure_sync_table_exists()
            
            self.sync_table.delete_item(
                Key={
                    'job_type': 'CONTAINER',
                    'job_id': f"{sync_job_id}#{self.container_id}"
                }
            )
            
            logger.info(f"✅ Container {self.container_name} unregistered from sync job {sync_job_id}")
            return True
            
        except ClientError as e:
            error_msg = handle_error_securely(e, f"unregistering container {self.container_name}")
            logger.error(error_msg)
            return False
    
    def get_active_containers(self, sync_job_id: str) -> List[Dict[str, Any]]:
        """
        Get list of active containers for a sync job.
        
        Args:
            sync_job_id: The Q Business sync job ID
            
        Returns:
            List of active container records
        """
        try:
            self._ensure_sync_table_exists()
            
            # Query for all containers in this sync job
            response = self.sync_table.query(
                KeyConditionExpression='job_type = :job_type AND begins_with(job_id, :sync_job_prefix)',
                ExpressionAttributeValues={
                    ':job_type': 'CONTAINER',
                    ':sync_job_prefix': f"{sync_job_id}#"
                }
            )
            
            active_containers = []
            current_time = datetime.now(timezone.utc)
            
            for item in response['Items']:
                # Check if container is still active (heartbeat within last 10 minutes)
                last_heartbeat_str = item.get('last_heartbeat', '')
                if last_heartbeat_str:
                    try:
                        last_heartbeat = datetime.fromisoformat(last_heartbeat_str.replace('Z', '+00:00'))
                        if (current_time - last_heartbeat).total_seconds() < 600:  # 10 minutes
                            active_containers.append(item)
                        else:
                            # Container is stale, remove it
                            logger.info(f"Removing stale container registration: {item.get('container_name', 'unknown')}")
                            self._remove_stale_container(item)
                    except ValueError:
                        # Invalid timestamp, consider container stale
                        logger.warning(f"Invalid heartbeat timestamp for container: {item.get('container_name', 'unknown')}")
                        self._remove_stale_container(item)
            
            return active_containers
            
        except ClientError as e:
            error_msg = handle_error_securely(e, f"getting active containers for sync job {sync_job_id}")
            logger.error(error_msg)
            return []
    
    def _remove_stale_container(self, container_item: Dict[str, Any]):
        """Remove a stale container registration"""
        try:
            self.sync_table.delete_item(
                Key={
                    'job_type': container_item['job_type'],
                    'job_id': container_item['job_id']
                }
            )
        except ClientError:
            # Ignore errors when cleaning up stale containers
            pass
    
    def register_sync_job(self, sync_job_id: str) -> bool:
        """
        Register a new Q Business sync job as the active job.
        
        Args:
            sync_job_id: The Q Business sync job ID
            
        Returns:
            bool: True if registration successful (this container owns the sync job)
        """
        try:
            self._ensure_sync_table_exists()
            
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Try to register as the sync job owner
            self.sync_table.put_item(
                Item={
                    'job_type': 'SYNC_JOB',
                    'job_id': sync_job_id,
                    'owner_container_id': self.container_id,
                    'owner_container_name': self.container_name,
                    'status': 'ACTIVE',
                    'created_at': current_time,
                    'last_heartbeat': current_time,
                    'ttl': int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())
                },
                ConditionExpression='attribute_not_exists(job_id)'  # Only create if doesn't exist
            )
            
            self.current_sync_job_id = sync_job_id
            self.is_sync_job_owner = True
            
            logger.info(f"✅ Container {self.container_name} registered as owner of sync job {sync_job_id}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ConditionalCheckFailedException':
                # Sync job already exists, we're not the owner
                logger.info(f"Sync job {sync_job_id} already registered by another container")
                self.current_sync_job_id = sync_job_id
                self.is_sync_job_owner = False
                return False
            else:
                error_msg = handle_error_securely(e, f"registering sync job {sync_job_id}")
                logger.error(error_msg)
                return False
    
    def get_active_sync_job(self) -> Optional[Dict[str, Any]]:
        """
        Get the currently active sync job.
        
        Returns:
            Dict with sync job info or None if no active job
        """
        try:
            self._ensure_sync_table_exists()
            
            # Query for active sync jobs
            response = self.sync_table.query(
                KeyConditionExpression='job_type = :job_type',
                ExpressionAttributeValues={
                    ':job_type': 'SYNC_JOB'
                }
            )
            
            current_time = datetime.now(timezone.utc)
            
            for item in response['Items']:
                # Check if sync job is still active (heartbeat within last 10 minutes)
                last_heartbeat_str = item.get('last_heartbeat', '')
                if last_heartbeat_str:
                    try:
                        last_heartbeat = datetime.fromisoformat(last_heartbeat_str.replace('Z', '+00:00'))
                        if (current_time - last_heartbeat).total_seconds() < 600:  # 10 minutes
                            return item
                        else:
                            # Sync job is stale, remove it
                            logger.info(f"Removing stale sync job: {item.get('job_id', 'unknown')}")
                            self._remove_stale_sync_job(item)
                    except ValueError:
                        # Invalid timestamp, consider sync job stale
                        logger.warning(f"Invalid heartbeat timestamp for sync job: {item.get('job_id', 'unknown')}")
                        self._remove_stale_sync_job(item)
            
            return None
            
        except ClientError as e:
            error_msg = handle_error_securely(e, "getting active sync job")
            logger.error(error_msg)
            return None
    
    def _remove_stale_sync_job(self, sync_job_item: Dict[str, Any]):
        """Remove a stale sync job registration"""
        try:
            self.sync_table.delete_item(
                Key={
                    'job_type': sync_job_item['job_type'],
                    'job_id': sync_job_item['job_id']
                }
            )
        except ClientError:
            # Ignore errors when cleaning up stale sync jobs
            pass
    
    def unregister_sync_job(self, sync_job_id: str) -> bool:
        """
        Unregister a sync job (only if this container owns it).
        
        Args:
            sync_job_id: The Q Business sync job ID
            
        Returns:
            bool: True if unregistration successful
        """
        try:
            self._ensure_sync_table_exists()
            
            # Only unregister if we own the sync job
            if not self.is_sync_job_owner:
                logger.info(f"Container {self.container_name} is not owner of sync job {sync_job_id}, skipping unregistration")
                return True
            
            self.sync_table.delete_item(
                Key={
                    'job_type': 'SYNC_JOB',
                    'job_id': sync_job_id
                },
                ConditionExpression='owner_container_id = :container_id',
                ExpressionAttributeValues={
                    ':container_id': self.container_id
                }
            )
            
            self.current_sync_job_id = None
            self.is_sync_job_owner = False
            
            logger.info(f"✅ Container {self.container_name} unregistered sync job {sync_job_id}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ConditionalCheckFailedException':
                logger.warning(f"Cannot unregister sync job {sync_job_id} - not owned by this container")
                return False
            else:
                error_msg = handle_error_securely(e, f"unregistering sync job {sync_job_id}")
                logger.error(error_msg)
                return False
    
    def start_or_join_sync_job(self) -> Optional[str]:
        """
        Start a new sync job or join an existing one.
        
        Returns:
            str: Sync job ID if successful, None if failed
        """
        try:
            # Check if there's already an active sync job
            active_job = self.get_active_sync_job()
            
            if active_job:
                sync_job_id = active_job['job_id']
                logger.info(f"Joining existing sync job: {sync_job_id}")
                
                # Register this container for the existing sync job
                if self.register_container(sync_job_id):
                    self.current_sync_job_id = sync_job_id
                    return sync_job_id
                else:
                    logger.error("Failed to register container for existing sync job")
                    return None
            else:
                # No active sync job, try to start a new one
                logger.info("No active sync job found, starting new sync job...")
                
                # Start Q Business sync job
                sync_job_id = self.qbusiness_client.start_sync_job()
                if not sync_job_id:
                    logger.error("Failed to start Q Business sync job")
                    return None
                
                # Register the sync job in coordination table
                if self.register_sync_job(sync_job_id):
                    # We own the sync job, also register as a container
                    if self.register_container(sync_job_id):
                        logger.info(f"✅ Started and joined new sync job: {sync_job_id}")
                        return sync_job_id
                    else:
                        logger.error("Failed to register container for new sync job")
                        return None
                else:
                    # Another container started the sync job, try to join it
                    logger.info("Another container started the sync job, attempting to join...")
                    if self.register_container(sync_job_id):
                        self.current_sync_job_id = sync_job_id
                        return sync_job_id
                    else:
                        logger.error("Failed to join sync job started by another container")
                        return None
                        
        except Exception as e:
            logger.error(f"Error starting or joining sync job: {e}")
            return None
    
    def stop_sync_job_if_owner(self) -> bool:
        """
        Stop the sync job if this container owns it and no other containers are active.
        
        Returns:
            bool: True if sync job was stopped or if not the owner
        """
        try:
            if not self.current_sync_job_id:
                logger.info("No current sync job to stop")
                return True
            
            logger.info(f"Container {self.container_name} attempting to stop sync job {self.current_sync_job_id}")
            
            # Unregister this container first
            logger.info("Unregistering this container from sync job...")
            self.unregister_container(self.current_sync_job_id)
            
            # If we're not the owner, we're done
            if not self.is_sync_job_owner:
                logger.info(f"Container {self.container_name} is not owner of sync job, leaving it running")
                self.current_sync_job_id = None
                return True
            
            # Check if any other containers are still active
            logger.info("Checking for other active containers...")
            active_containers = self.get_active_containers(self.current_sync_job_id)
            
            if active_containers:
                logger.info(f"Sync job {self.current_sync_job_id} has {len(active_containers)} active containers:")
                for container in active_containers:
                    logger.info(f"  - {container.get('container_name', 'unknown')} (last heartbeat: {container.get('last_heartbeat', 'unknown')})")
                
                logger.info("Leaving sync job running for other containers")
                # Don't update heartbeat - let the other containers manage it
                self.current_sync_job_id = None
                self.is_sync_job_owner = False
                return True
            else:
                # No other containers active, safe to stop the sync job
                logger.info(f"No other containers active, stopping sync job {self.current_sync_job_id}")
                
                # Stop the Q Business sync job directly (avoid recursion)
                success = self._direct_stop_qbusiness_sync_job()
                
                # Unregister the sync job from coordination table
                logger.info("Unregistering sync job from coordination table...")
                self.unregister_sync_job(self.current_sync_job_id)
                
                return success
                
        except Exception as e:
            logger.error(f"Error stopping sync job: {e}")
            return False
    
    def _direct_stop_qbusiness_sync_job(self) -> bool:
        """Directly stop the Q Business sync job via AWS API to avoid recursion"""
        try:
            logger.info(f"Directly stopping Q Business sync job {self.current_sync_job_id}")
            
            # Use the qbusiness client's direct stop method
            if hasattr(self.qbusiness_client, '_direct_stop_sync_job'):
                return self.qbusiness_client._direct_stop_sync_job()
            else:
                # Fallback to AWS API call
                response = self.qbusiness_client.client.stop_data_source_sync_job(
                    applicationId=self.qbusiness_client.config.application_id,
                    indexId=self.qbusiness_client.config.index_id,
                    dataSourceId=self.qbusiness_client.config.data_source_id
                )
                logger.info("Q Business sync job stopped successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error directly stopping Q Business sync job: {e}")
            return False
    
    def cleanup_stale_registrations(self):
        """Clean up stale container and sync job registrations"""
        try:
            self._ensure_sync_table_exists()
            
            current_time = datetime.now(timezone.utc)
            stale_threshold = current_time - timedelta(minutes=10)
            
            # Scan for stale registrations
            response = self.sync_table.scan()
            
            stale_items = []
            for item in response['Items']:
                last_heartbeat_str = item.get('last_heartbeat', '')
                if last_heartbeat_str:
                    try:
                        last_heartbeat = datetime.fromisoformat(last_heartbeat_str.replace('Z', '+00:00'))
                        if last_heartbeat < stale_threshold:
                            stale_items.append(item)
                    except ValueError:
                        # Invalid timestamp, consider stale
                        stale_items.append(item)
            
            # Remove stale items
            for item in stale_items:
                try:
                    self.sync_table.delete_item(
                        Key={
                            'job_type': item['job_type'],
                            'job_id': item['job_id']
                        }
                    )
                    logger.info(f"Cleaned up stale registration: {item.get('job_type', 'unknown')} - {item.get('job_id', 'unknown')}")
                except ClientError:
                    # Ignore errors during cleanup
                    pass
                    
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")