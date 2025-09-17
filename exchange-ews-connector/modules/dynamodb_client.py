"""
DynamoDB Client Module
Handles DynamoDB operations for tracking processed emails
"""

import boto3
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set
from botocore.exceptions import ClientError
from .security_utils import sanitize_for_logging, handle_error_securely

logger = logging.getLogger(__name__)

class DynamoDBClient:
    """DynamoDB client for tracking processed emails"""
    
    def __init__(self, config):
        self.config = config
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self._initialize_table()
    
    def _initialize_table(self):
        """Initialize DynamoDB table reference"""
        # Return None initially - table will be initialized on first use
        # The table should already exist via CloudFormation deployment
        return None
    
    def _ensure_table_exists(self):
        """Ensure table exists and is initialized"""
        if self.table is None:
            try:
                table = self.dynamodb.Table(self.config.table_name)
                table.load()
                print(f"Using existing DynamoDB table: {self.config.table_name}")
                self.table = table
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ResourceNotFoundException':
                    logger.info(f"DynamoDB table '{self.config.table_name}' not found. Creating it automatically...")
                    self.table = self._create_table()
                else:
                    error_msg = handle_error_securely(e, f"accessing DynamoDB table '{self.config.table_name}'")
                    raise Exception(error_msg)
    
    def _create_table(self):
        """Create DynamoDB table with required structure"""
        try:
            print(f"Creating DynamoDB table: {self.config.table_name}")
            
            table = self.dynamodb.create_table(
                TableName=self.config.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'email_id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'email_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'account_email',
                        'AttributeType': 'S'
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'account-email-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'account_email',
                                'KeyType': 'HASH'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        }
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
                        'Key': 'CreatedBy',
                        'Value': 'ExchangeEWSConnector'
                    }
                ]
            )
            
            print(f"Waiting for table {self.config.table_name} to become active...")
            table.wait_until_exists()
            
            # Wait a bit more to ensure the table is fully ready
            time.sleep(5)
            
            print(f"✅ DynamoDB table {self.config.table_name} created successfully")
            return table
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceInUseException':
                # Table already exists (race condition), try to use it
                print(f"Table {self.config.table_name} already exists (created by another process)")
                table = self.dynamodb.Table(self.config.table_name)
                table.load()
                return table
            else:
                raise Exception(f"Error creating DynamoDB table '{self.config.table_name}': {e}")
        except Exception as e:
            raise Exception(f"Unexpected error creating DynamoDB table '{self.config.table_name}': {e}")
    
    def is_email_processed(self, email_id: str) -> bool:
        """Check if email ID has been processed"""
        try:
            self._ensure_table_exists()
            response = self.table.get_item(Key={'email_id': str(email_id)})
            return 'Item' in response
        except ClientError:
            return False
    
    def get_email_processing_info(self, email_id: str) -> dict:
        """Get processing information for an email"""
        try:
            self._ensure_table_exists()
            response = self.table.get_item(Key={'email_id': str(email_id)})
            return response.get('Item', {})
        except ClientError:
            return {}
    
    def needs_update(self, email_id: str, last_modified_time) -> bool:
        """Check if an email needs to be updated based on last modified time"""
        try:
            processing_info = self.get_email_processing_info(email_id)
            if not processing_info:
                return True  # Not processed yet, needs processing
            
            # Get the stored datetime_created (when we last processed it)
            stored_datetime = processing_info.get('datetime_created', '')
            if not stored_datetime:
                return True  # No stored datetime, needs processing
            
            # Compare with current last_modified_time
            if last_modified_time and hasattr(last_modified_time, 'isoformat'):
                current_modified = last_modified_time.isoformat()
                # If the email was modified after we last processed it, it needs update
                return current_modified > stored_datetime
            
            return False  # Can't determine, assume no update needed
            
        except Exception as e:
            error_msg = handle_error_securely(e, "checking if email needs update")
            logger.error(error_msg)
            return True  # On error, assume it needs processing
    
    def mark_email_processed(self, email_id: str, folder_name: str, datetime_created, 
                           status: str = 'processed', account_email: str = None) -> bool:
        """Mark email ID as processed or failed - updates existing item if it exists"""
        try:
            self._ensure_table_exists()
            email_id_str = str(email_id)
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Use update_item to handle both new items and updates to existing items
            response = self.table.update_item(
                Key={'email_id': email_id_str},
                UpdateExpression='SET folder_name = :folder, datetime_created = :created, processed_at = :processed, #status = :status, account_email = :account, attempt_count = if_not_exists(attempt_count, :zero) + :one',
                ExpressionAttributeNames={
                    '#status': 'status'  # 'status' is a reserved word in DynamoDB
                },
                ExpressionAttributeValues={
                    ':folder': folder_name,
                    ':created': str(datetime_created),
                    ':processed': current_time,
                    ':status': status,
                    ':account': account_email or 'unknown',
                    ':zero': 0,
                    ':one': 1
                },
                ReturnValues='ALL_NEW'
            )
            
            # Log the update
            updated_item = response.get('Attributes', {})
            attempt_count = updated_item.get('attempt_count', 1)
            
            if attempt_count == 1:
                print(f"  DynamoDB: Created new record for email {email_id_str[:20]}... with status '{status}'")
            else:
                print(f"  DynamoDB: Updated existing record for email {email_id_str[:20]}... with status '{status}' (attempt #{attempt_count})")
            
            return True
            
        except ClientError as e:
            error_msg = handle_error_securely(e, f"marking email as {status}")
            logger.error(error_msg)
            return False
    
    def get_processed_email_ids_for_folder(self, folder_name: str) -> Set[str]:
        """Get email IDs from DynamoDB that have been processed for a specific folder"""
        try:
            self._ensure_table_exists()
            processed_ids = set()
            
            response = self.table.scan(
                FilterExpression='folder_name = :folder_name',
                ExpressionAttributeValues={':folder_name': folder_name}
            )
            
            for item in response['Items']:
                processed_ids.add(item['email_id'])
            
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression='folder_name = :folder_name',
                    ExpressionAttributeValues={':folder_name': folder_name},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response['Items']:
                    processed_ids.add(item['email_id'])
            
            return processed_ids
        except ClientError as e:
            error_msg = handle_error_securely(e, f"scanning DynamoDB table for folder {sanitize_for_logging(folder_name)}")
            logger.error(error_msg)
            return set()
    
    def get_all_processed_email_ids(self) -> Set[str]:
        """Get all email IDs from DynamoDB that have been processed"""
        try:
            self._ensure_table_exists()
            processed_ids = set()
            response = self.table.scan()
            
            for item in response['Items']:
                processed_ids.add(item['email_id'])
            
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                for item in response['Items']:
                    processed_ids.add(item['email_id'])
            
            return processed_ids
        except ClientError as e:
            error_msg = handle_error_securely(e, "scanning DynamoDB table")
            logger.error(error_msg)
            return set()
    
    def get_processed_emails_by_account(self, account_email: str) -> List[Dict[str, Any]]:
        """Get all processed emails for a specific account"""
        try:
            self._ensure_table_exists()
            processed_emails = []
            
            # Try to use the index first
            try:
                response = self.table.query(
                    IndexName='account-email-index',
                    KeyConditionExpression='account_email = :account',
                    ExpressionAttributeValues={
                        ':account': account_email
                    }
                )
                
                processed_emails.extend(response['Items'])
                
                while 'LastEvaluatedKey' in response:
                    response = self.table.query(
                        IndexName='account-email-index',
                        KeyConditionExpression='account_email = :account',
                        ExpressionAttributeValues={
                            ':account': account_email
                        },
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    processed_emails.extend(response['Items'])
                
                return processed_emails
                
            except ClientError as index_error:
                # If index doesn't exist, fall back to scan
                if 'ValidationException' in str(index_error) and 'index' in str(index_error).lower():
                    print(f"Index 'account-email-index' not found, falling back to scan for account {account_email}")
                    return self._scan_by_account(account_email)
                else:
                    raise index_error
            
        except ClientError as e:
            print(f"Error querying DynamoDB table for account {account_email}: {e}")
            return []
    
    def _scan_by_account(self, account_email: str) -> List[Dict[str, Any]]:
        """Fallback method to scan table by account when index is not available"""
        try:
            processed_emails = []
            response = self.table.scan(
                FilterExpression='account_email = :account',
                ExpressionAttributeValues={
                    ':account': account_email
                }
            )
            
            processed_emails.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression='account_email = :account',
                    ExpressionAttributeValues={
                        ':account': account_email
                    },
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                processed_emails.extend(response['Items'])
            
            return processed_emails
            
        except ClientError as e:
            print(f"Error scanning DynamoDB table for account {account_email}: {e}")
            return []
    
    def get_account_processing_stats(self) -> Dict[str, Dict[str, int]]:
        """Get processing statistics by account"""
        try:
            self._ensure_table_exists()
            stats = {}
            response = self.table.scan()
            
            for item in response['Items']:
                account = item.get('account_email', 'unknown')
                status = item.get('status', 'processed')
                
                if account not in stats:
                    stats[account] = {'processed': 0, 'failed': 0, 'total': 0}
                
                stats[account][status] = stats[account].get(status, 0) + 1
                stats[account]['total'] += 1
            
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                for item in response['Items']:
                        
                    account = item.get('account_email', 'unknown')
                    status = item.get('status', 'processed')
                    
                    if account not in stats:
                        stats[account] = {'processed': 0, 'failed': 0, 'total': 0}
                    
                    stats[account][status] = stats[account].get(status, 0) + 1
                    stats[account]['total'] += 1
            
            return stats
        except ClientError as e:
            print(f"Error getting account processing stats: {e}")
            return {}
    
    def delete_email_record(self, email_id: str) -> bool:
        """Delete an email ID from DynamoDB"""
        try:
            self._ensure_table_exists()
            self.table.delete_item(Key={'email_id': str(email_id)})
            return True
        except ClientError as e:
            error_msg = handle_error_securely(e, f"deleting email {sanitize_for_logging(str(email_id))} from DynamoDB")
            logger.error(error_msg)
            return False
    
    def record_sync_execution(self, sync_mode: str, account_email: str, 
                            emails_processed: int, emails_attempted: int) -> bool:
        """Record sync execution details"""
        try:
            self._ensure_table_exists()
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Create a unique sync record ID
            sync_record_id = f"sync_{account_email}_{current_time}"
            
            self.table.put_item(
                Item={
                    'email_id': sync_record_id,
                    'record_type': 'sync_execution',
                    'account_email': account_email,
                    'sync_mode': sync_mode,
                    'emails_processed': emails_processed,
                    'emails_attempted': emails_attempted,
                    'execution_time': current_time,
                    'processed_at': current_time
                }
            )
            
            print(f"  DynamoDB: Recorded sync execution for {account_email} ({sync_mode} mode)")
            return True
            
        except ClientError as e:
            print(f"Error recording sync execution: {e}")
            return False
    
    def get_last_full_sync(self, account_email: str) -> Optional[str]:
        """Get the timestamp of the last full sync for an account"""
        try:
            self._ensure_table_exists()
            
            # Scan for sync execution records for this account
            response = self.table.scan(
                FilterExpression='account_email = :account AND record_type = :type AND sync_mode = :mode',
                ExpressionAttributeValues={
                    ':account': account_email,
                    ':type': 'sync_execution',
                    ':mode': 'full'
                }
            )
            
            sync_records = response.get('Items', [])
            
            # Continue scanning if there are more items
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression='account_email = :account AND record_type = :type AND sync_mode = :mode',
                    ExpressionAttributeValues={
                        ':account': account_email,
                        ':type': 'sync_execution',
                        ':mode': 'full'
                    },
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                sync_records.extend(response.get('Items', []))
            
            if sync_records:
                # Sort by execution time and get the most recent
                sync_records.sort(key=lambda x: x.get('execution_time', ''), reverse=True)
                return sync_records[0].get('execution_time')
            
            return None
            
        except ClientError as e:
            print(f"Error getting last full sync for {account_email}: {e}")
            return None
    
    def clear_processed_emails_for_account(self, account_email: str) -> bool:
        """Clear all processed email records for an account (for full sync)"""
        try:
            self._ensure_table_exists()
            
            # Get all processed emails for this account
            processed_emails = self.get_processed_emails_by_account(account_email)
            
            if not processed_emails:
                print(f"No processed emails found for account {account_email}")
                return True
            
            print(f"Clearing {len(processed_emails)} processed email records for {account_email}")
            
            # Delete in batches
            batch_size = 25  # DynamoDB batch write limit
            
            for i in range(0, len(processed_emails), batch_size):
                batch = processed_emails[i:i + batch_size]
                
                with self.table.batch_writer() as batch_writer:
                    for email in batch:
                        batch_writer.delete_item(Key={'email_id': email['email_id']})
            
            print(f"Cleared processed email records for {account_email}")
            return True
            
        except ClientError as e:
            print(f"Error clearing processed emails for {account_email}: {e}")
            return False
    
    def check_table_structure(self) -> bool:
        """Check if table has the required index structure"""
        try:
            self._ensure_table_exists()
            table_description = self.table.meta.client.describe_table(TableName=self.config.table_name)
            
            # Check if account-email-index exists
            gsi_list = table_description.get('Table', {}).get('GlobalSecondaryIndexes', [])
            has_account_index = any(gsi.get('IndexName') == 'account-email-index' for gsi in gsi_list)
            
            if has_account_index:
                print(f"✅ Table {self.config.table_name} has required account-email-index")
                return True
            else:
                print(f"⚠️  Table {self.config.table_name} is missing account-email-index")
                return False
                
        except ClientError as e:
            print(f"Error checking table structure: {e}")
            return False
    
    def verify_table_ready(self) -> bool:
        """Verify that the table is ready for use"""
        try:
            self._ensure_table_exists()
            
            # Check table status
            table_description = self.table.meta.client.describe_table(TableName=self.config.table_name)
            table_status = table_description.get('Table', {}).get('TableStatus', 'UNKNOWN')
            
            if table_status != 'ACTIVE':
                print(f"⚠️  Table {self.config.table_name} is not active (status: {table_status})")
                return False
            
            # Check GSI status
            gsi_list = table_description.get('Table', {}).get('GlobalSecondaryIndexes', [])
            for gsi in gsi_list:
                gsi_status = gsi.get('IndexStatus', 'UNKNOWN')
                if gsi_status != 'ACTIVE':
                    print(f"⚠️  GSI {gsi.get('IndexName')} is not active (status: {gsi_status})")
                    return False
            
            print(f"✅ Table {self.config.table_name} is ready for use")
            return True
            
        except ClientError as e:
            print(f"Error verifying table readiness: {e}")
            return False