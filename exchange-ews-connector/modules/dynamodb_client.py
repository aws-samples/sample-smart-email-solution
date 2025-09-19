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
                        'AttributeName': 'account_email',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'folder_email_key',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'account_email',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'folder_email_key',
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
    
    def _create_folder_email_key(self, folder_path: str, email_id: str) -> str:
        """Create sort key from folder_path and email_id"""
        return f"{folder_path}#{email_id}"
    
    def _extract_email_id_from_folder_email_key(self, folder_email_key: str) -> str:
        """Extract email_id from folder_email_key"""
        parts = folder_email_key.split('#')
        return parts[-1] if len(parts) >= 2 else ''
    
    def _extract_folder_from_folder_email_key(self, folder_email_key: str) -> str:
        """Extract folder_path from folder_email_key"""
        parts = folder_email_key.split('#')
        return parts[0] if len(parts) >= 2 else ''
    
    def is_email_processed(self, email_id: str, account_email: str, folder_path: str) -> bool:
        """Check if email ID has been processed"""
        try:
            self._ensure_table_exists()
            folder_email_key = self._create_folder_email_key(folder_path, email_id)
            response = self.table.get_item(Key={
                'account_email': account_email,
                'folder_email_key': folder_email_key
            })
            return 'Item' in response
        except ClientError:
            return False
    
    def get_email_processing_info(self, email_id: str, account_email: str, folder_path: str) -> dict:
        """Get processing information for an email"""
        try:
            self._ensure_table_exists()
            folder_email_key = self._create_folder_email_key(folder_path, email_id)
            response = self.table.get_item(Key={
                'account_email': account_email,
                'folder_email_key': folder_email_key
            })
            return response.get('Item', {})
        except ClientError:
            return {}
    
    def mark_email_processed(self, email_id: str, folder_name: str, datetime_created, 
                           status: str = 'processed', account_email: str = None) -> bool:
        """Mark email ID as processed or failed - updates existing item if it exists"""
        try:
            self._ensure_table_exists()
            email_id_str = str(email_id)
            current_time = datetime.now(timezone.utc).isoformat()
            
            if not account_email:
                raise ValueError("account_email is required")
            
            # Create folder_email_key
            folder_email_key = self._create_folder_email_key(folder_name, email_id_str)
            
            # Use update_item to handle both new items and updates to existing items
            response = self.table.update_item(
                Key={
                    'account_email': account_email,
                    'folder_email_key': folder_email_key
                },
                UpdateExpression='SET datetime_created = :created, processed_at = :processed, #status = :status, attempt_count = if_not_exists(attempt_count, :zero) + :one',
                ExpressionAttributeNames={
                    '#status': 'status'  # 'status' is a reserved word in DynamoDB
                },
                ExpressionAttributeValues={
                    ':created': str(datetime_created),
                    ':processed': current_time,
                    ':status': status,
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
    
    def get_processed_email_ids_for_folder(self, folder_name: str, account_email: str) -> Set[str]:
        """Get email IDs from DynamoDB that have been processed for a specific folder"""
        try:
            self._ensure_table_exists()
            processed_ids = set()
            
            # Create prefix for folder_email_key: folder_name#
            folder_prefix = f"{folder_name}#"
            
            # Use efficient query with begins_with on sort key
            response = self.table.query(
                KeyConditionExpression='account_email = :account AND begins_with(folder_email_key, :folder_prefix)',
                ExpressionAttributeValues={
                    ':account': account_email,
                    ':folder_prefix': folder_prefix
                }
            )
            
            for item in response['Items']:
                folder_email_key = item.get('folder_email_key', '')
                email_id = self._extract_email_id_from_folder_email_key(folder_email_key)
                if email_id:
                    processed_ids.add(email_id)
            
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    KeyConditionExpression='account_email = :account AND begins_with(folder_email_key, :folder_prefix)',
                    ExpressionAttributeValues={
                        ':account': account_email,
                        ':folder_prefix': folder_prefix
                    },
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response['Items']:
                    folder_email_key = item.get('folder_email_key', '')
                    email_id = self._extract_email_id_from_folder_email_key(folder_email_key)
                    if email_id:
                        processed_ids.add(email_id)
            
            return processed_ids
        except ClientError as e:
            error_msg = handle_error_securely(e, f"querying DynamoDB table for folder {sanitize_for_logging(folder_name)}")
            logger.error(error_msg)
            return set()
    
    def get_all_processed_email_ids(self) -> Set[str]:
        """Get all email IDs from DynamoDB that have been processed"""
        try:
            self._ensure_table_exists()
            processed_ids = set()
            response = self.table.scan()
            
            for item in response['Items']:
                folder_email_key = item.get('folder_email_key', '')
                email_id = self._extract_email_id_from_folder_email_key(folder_email_key)
                if email_id:
                    processed_ids.add(email_id)
            
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                for item in response['Items']:
                    folder_email_key = item.get('folder_email_key', '')
                    email_id = self._extract_email_id_from_folder_email_key(folder_email_key)
                    if email_id:
                        processed_ids.add(email_id)
            
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
            
            # Use efficient query on partition key
            response = self.table.query(
                KeyConditionExpression='account_email = :account',
                ExpressionAttributeValues={
                    ':account': account_email
                }
            )
            
            processed_emails.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    KeyConditionExpression='account_email = :account',
                    ExpressionAttributeValues={
                        ':account': account_email
                    },
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                processed_emails.extend(response['Items'])
            
            return processed_emails
            
        except ClientError as e:
            print(f"Error querying DynamoDB table for account {account_email}: {e}")
            return []
    
    def delete_email_record(self, email_id: str, account_email: str, folder_path: str) -> bool:
        """Delete an email ID from DynamoDB"""
        try:
            self._ensure_table_exists()
            folder_email_key = self._create_folder_email_key(folder_path, email_id)
            self.table.delete_item(Key={
                'account_email': account_email,
                'folder_email_key': folder_email_key
            })
            return True
        except ClientError as e:
            error_msg = handle_error_securely(e, f"deleting email {sanitize_for_logging(str(email_id))} from DynamoDB")
            logger.error(error_msg)
            return False
    
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
                        account_email_key = email.get('account_email')
                        folder_email_key = email.get('folder_email_key')
                        if account_email_key and folder_email_key:
                            batch_writer.delete_item(Key={
                                'account_email': account_email_key,
                                'folder_email_key': folder_email_key
                            })
            
            print(f"Cleared processed email records for {account_email}")
            return True
            
        except ClientError as e:
            print(f"Error clearing processed emails for {account_email}: {e}")
            return False
    
    def check_table_structure(self) -> bool:
        """Check if table has the required structure"""
        try:
            self._ensure_table_exists()
            table_description = self.table.meta.client.describe_table(TableName=self.config.table_name)
            
            # Check primary key structure
            key_schema = table_description.get('Table', {}).get('KeySchema', [])
            has_partition_key = any(key.get('AttributeName') == 'account_email' and key.get('KeyType') == 'HASH' for key in key_schema)
            has_sort_key = any(key.get('AttributeName') == 'folder_email_key' and key.get('KeyType') == 'RANGE' for key in key_schema)
            
            if has_partition_key and has_sort_key:
                print(f"✅ Table {self.config.table_name} has required partition key (account_email) and sort key (folder_email_key)")
                return True
            else:
                print(f"⚠️  Table {self.config.table_name} is missing required key structure")
                print(f"    Has partition key (account_email): {has_partition_key}")
                print(f"    Has sort key (folder_email_key): {has_sort_key}")
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
            
            print(f"✅ Table {self.config.table_name} is ready for use")
            return True
            
        except ClientError as e:
            print(f"Error verifying table readiness: {e}")
            return False