"""
Configuration Module
Handles configuration management, parameter store integration, and email validation
"""

import os
import re
import boto3
import logging
from typing import List
from botocore.exceptions import ClientError
from .security_utils import sanitize_for_logging, handle_error_securely, validate_aws_response

# Email validation library (optional but recommended)
try:
    from email_validator import validate_email, EmailNotValidError
    EMAIL_VALIDATOR_AVAILABLE = True
except ImportError:
    EMAIL_VALIDATOR_AVAILABLE = False
    print("Warning: email-validator library not available, using basic validation")

logger = logging.getLogger(__name__)

def get_parameter_from_store(parameter_name: str, default_value: str = None) -> str:
    """Get parameter from AWS Systems Manager Parameter Store"""
    environment = os.environ.get('ENVIRONMENT', 'dev')
    parameter_store_prefix = os.environ.get('PARAMETER_STORE_PREFIX', f'/exchange-connector/{environment}')
    parameter_path = f"{parameter_store_prefix}/{parameter_name.lower().replace('_', '-')}"
    
    try:
        ssm = boto3.client('ssm')
        response = ssm.get_parameter(Name=parameter_path, WithDecryption=True)
        
        # Validate AWS response
        if not validate_aws_response(response, ['Parameter']):
            logger.error(f"Invalid response structure from Parameter Store for {parameter_name}")
            return default_value
        
        return response['Parameter']['Value']
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'ParameterNotFound':
            logger.info(f"Parameter {sanitize_for_logging(parameter_name)} not found in Parameter Store")
            # Fallback to environment variable for local development
            env_value = os.environ.get(parameter_name, default_value)
            if env_value:
                logger.info(f"Using environment variable fallback for {sanitize_for_logging(parameter_name)}")
                return env_value
            return default_value
        else:
            error_msg = handle_error_securely(e, f"retrieving parameter {parameter_name}")
            logger.error(error_msg)
            return default_value
    except Exception as e:
        error_msg = handle_error_securely(e, f"retrieving parameter {parameter_name}")
        logger.error(error_msg)
        # Fallback to environment variable for local development
        env_value = os.environ.get(parameter_name, default_value)
        if env_value:
            logger.info(f"Using environment variable fallback for {sanitize_for_logging(parameter_name)}")
            return env_value
        return default_value

def parse_email_addresses(addresses_raw: str) -> List[str]:
    """Parse and validate email addresses from configuration"""
    if not addresses_raw:
        return []
    
    # Split by comma and clean up whitespace
    addresses = [addr.strip() for addr in addresses_raw.split(',') if addr.strip()]
    valid_addresses = []
    
    for addr in addresses:
        if EMAIL_VALIDATOR_AVAILABLE:
            # Use proper email validation library
            try:
                validated = validate_email(addr)
                valid_addresses.append(validated.email)
            except EmailNotValidError as e:
                sanitized_addr = sanitize_for_logging(addr)
                logger.warning(f"Invalid email address format: {sanitized_addr} - {e}")
        else:
            # Fallback to improved regex validation
            # More restrictive pattern that follows RFC 5322 more closely
            email_pattern = re.compile(
                r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
            )
            if email_pattern.match(addr) and len(addr) <= 254:  # RFC 5321 length limit
                valid_addresses.append(addr)
            else:
                sanitized_addr = sanitize_for_logging(addr)
                logger.warning(f"Invalid email address format: {sanitized_addr}")
    
    return valid_addresses

class Config:
    """Configuration class for Exchange EWS Connector"""
    
    # Default configuration values
    DEFAULT_VALUES = {
        # Environment Configuration
        'ENVIRONMENT': 'dev',
        
        # DynamoDB Configuration
        'DYNAMODB_TABLE_NAME': 'processed-emails',
        
        # Processing Configuration
        'EMAIL_PROCESSING_LIMIT': '1',  # 0 means no limit - process all emails
        
        # Sync Mode Configuration
        'SYNC_MODE': 'delta',  # 'delta' or 'full'
        
        # AWS Region
        'AWS_DEFAULT_REGION': 'us-east-1',
        
        # HTML Processing Performance Configuration
        'HTML_PROCESSING_THRESHOLD': '100000',  # 100KB
        'HTML_CHUNK_SIZE': '500000',  # 500KB chunks
        'MAX_CONTENT_SIZE_MB': '10',  # 10MB max
        
        # Q Business Sync Job Conflict Resolution
        'AUTO_RESOLVE_SYNC_CONFLICTS': 'true',
        'MAX_SYNC_CONFLICT_RETRIES': '3',
        
        # Document Processing Configuration
        'DOCUMENT_BATCH_SIZE': '10',  # Submit every 10 documents
        
        # Folder Processing Configuration
        'PROCESS_MAIN_MAILBOX': 'true',  # Process main mailbox folders (default: false, only archive)
        
        # Threading Configuration
        'ENABLE_THREADING': 'true',  # Enable parallel processing
        'MAX_WORKER_THREADS': '4',   # Maximum number of worker threads
        'THREAD_BATCH_SIZE': '50',   # Number of emails per thread batch
        
        # Distributed Sync Job Configuration
        'SYNC_JOB_HEARTBEAT_INTERVAL': '30',  # Heartbeat interval in seconds
        'SYNC_JOB_STALE_THRESHOLD': '600',    # Consider sync job stale after 10 minutes
    }
    
    def __init__(self):
        # Environment Configuration
        self.environment = os.environ.get('ENVIRONMENT', self.DEFAULT_VALUES['ENVIRONMENT'])
        
        # DynamoDB configuration
        self.table_name = os.environ.get('DYNAMODB_TABLE_NAME', self.DEFAULT_VALUES['DYNAMODB_TABLE_NAME'])
        
        # AWS Region
        self.aws_region = os.environ.get('AWS_DEFAULT_REGION', self.DEFAULT_VALUES['AWS_DEFAULT_REGION'])
        
        # Parameter Store prefix - dynamically based on environment
        self.parameter_store_prefix = os.environ.get('PARAMETER_STORE_PREFIX', f'/exchange-connector/{self.environment}')
        
        # Q Business configuration (from Parameter Store)
        self.application_id = get_parameter_from_store('QBUSINESS_APPLICATION_ID')
        self.index_id = get_parameter_from_store('QBUSINESS_INDEX_ID')
        self.data_source_id = get_parameter_from_store('QBUSINESS_DATASOURCE_ID')
        
        # Exchange configuration (from Parameter Store)
        self.client_id = get_parameter_from_store('EXCHANGE_CLIENT_ID')
        self.client_secret = get_parameter_from_store('EXCHANGE_CLIENT_SECRET')
        self.tenant_id = get_parameter_from_store('EXCHANGE_TENANT_ID')
        self.exchange_server = get_parameter_from_store('EXCHANGE_SERVER', 'outlook.office365.com')
        
        # Parse email addresses (from Parameter Store)
        primary_smtp_addresses_raw = get_parameter_from_store('EXCHANGE_PRIMARY_SMTP_ADDRESS')
        self.primary_smtp_addresses = parse_email_addresses(primary_smtp_addresses_raw)
        
        # Processing limits (0 means no limit - process all emails)
        limit_str = os.environ.get('EMAIL_PROCESSING_LIMIT', '0')
        
        self.testing_email_limit = int(limit_str) if limit_str != '0' else None
        
        # Sync mode configuration
        self.sync_mode = os.environ.get('SYNC_MODE', self.DEFAULT_VALUES['SYNC_MODE']).lower()
        

        
        # HTML processing configuration
        self.html_processing_threshold = int(os.environ.get('HTML_PROCESSING_THRESHOLD', self.DEFAULT_VALUES['HTML_PROCESSING_THRESHOLD']))
        self.html_chunk_size = int(os.environ.get('HTML_CHUNK_SIZE', self.DEFAULT_VALUES['HTML_CHUNK_SIZE']))
        self.max_content_size_mb = int(os.environ.get('MAX_CONTENT_SIZE_MB', self.DEFAULT_VALUES['MAX_CONTENT_SIZE_MB']))
        
        # Q Business sync job configuration
        self.auto_resolve_sync_conflicts = os.environ.get('AUTO_RESOLVE_SYNC_CONFLICTS', self.DEFAULT_VALUES['AUTO_RESOLVE_SYNC_CONFLICTS']).lower() == 'true'
        self.max_sync_conflict_retries = int(os.environ.get('MAX_SYNC_CONFLICT_RETRIES', self.DEFAULT_VALUES['MAX_SYNC_CONFLICT_RETRIES']))
        
        # Document processing batch configuration
        self.document_batch_size = int(os.environ.get('DOCUMENT_BATCH_SIZE', self.DEFAULT_VALUES['DOCUMENT_BATCH_SIZE']))
        
        # Folder processing configuration
        self.process_main_mailbox = os.environ.get('PROCESS_MAIN_MAILBOX', self.DEFAULT_VALUES['PROCESS_MAIN_MAILBOX']).lower() == 'true'
        
        # Threading configuration
        self.enable_threading = os.environ.get('ENABLE_THREADING', self.DEFAULT_VALUES['ENABLE_THREADING']).lower() == 'true'
        self.max_worker_threads = int(os.environ.get('MAX_WORKER_THREADS', self.DEFAULT_VALUES['MAX_WORKER_THREADS']))
        self.thread_batch_size = int(os.environ.get('THREAD_BATCH_SIZE', self.DEFAULT_VALUES['THREAD_BATCH_SIZE']))
        
        # Distributed sync job configuration
        self.sync_job_heartbeat_interval = int(os.environ.get('SYNC_JOB_HEARTBEAT_INTERVAL', self.DEFAULT_VALUES['SYNC_JOB_HEARTBEAT_INTERVAL']))
        self.sync_job_stale_threshold = int(os.environ.get('SYNC_JOB_STALE_THRESHOLD', self.DEFAULT_VALUES['SYNC_JOB_STALE_THRESHOLD']))
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validate required configuration parameters"""
        if not self.primary_smtp_addresses:
            logger.error("No valid email addresses configured. Please check EXCHANGE_PRIMARY_SMTP_ADDRESS")
        else:
            # Log count only, not actual addresses for security
            logger.info(f"Configured {len(self.primary_smtp_addresses)} email address(es)")
    
    def get_required_vars(self) -> dict:
        """Get dictionary of required variables for validation"""
        return {
            'EXCHANGE_CLIENT_ID': self.client_id,
            'EXCHANGE_CLIENT_SECRET': self.client_secret,
            'EXCHANGE_TENANT_ID': self.tenant_id,
            'EXCHANGE_PRIMARY_SMTP_ADDRESS': self.primary_smtp_addresses,
            'QBUSINESS_APPLICATION_ID': self.application_id,
            'QBUSINESS_INDEX_ID': self.index_id,
            'QBUSINESS_DATASOURCE_ID': self.data_source_id
        }
    
    def display_config(self) -> None:
        """Display current configuration (excluding sensitive values)"""
        logger.info("Current Configuration:")
        logger.info(f"  Environment: {self.environment}")
        logger.info(f"  DynamoDB Table: {self.table_name}")
        logger.info(f"  AWS Region: {self.aws_region}")
        logger.info(f"  Parameter Store Prefix: {sanitize_for_logging(self.parameter_store_prefix)}")
        logger.info(f"  Email Processing Limit: {self.testing_email_limit or 'No limit'}")
        logger.info(f"  Sync Mode: {self.sync_mode}")
        logger.info(f"  Exchange Server: {self.exchange_server}")
        logger.info(f"  Email Addresses: {len(self.primary_smtp_addresses)} configured")
        logger.info(f"  HTML Processing Threshold: {self.html_processing_threshold:,} chars")
        logger.info(f"  HTML Chunk Size: {self.html_chunk_size:,} chars")
        logger.info(f"  Max Content Size: {self.max_content_size_mb} MB")
        logger.info(f"  Auto Resolve Sync Conflicts: {self.auto_resolve_sync_conflicts}")
        logger.info(f"  Max Sync Conflict Retries: {self.max_sync_conflict_retries}")
        logger.info(f"  Document Batch Size: {self.document_batch_size}")
        logger.info(f"  Process Main Mailbox: {self.process_main_mailbox}")
        logger.info(f"  Threading Enabled: {self.enable_threading}")
        logger.info(f"  Max Worker Threads: {self.max_worker_threads}")
        logger.info(f"  Sync Job Heartbeat Interval: {self.sync_job_heartbeat_interval}s")
        logger.info(f"  Sync Job Stale Threshold: {self.sync_job_stale_threshold}s")
