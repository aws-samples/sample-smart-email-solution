"""
Exchange Online Archive Connector - Modular Version
All functionality organized into separate modules for better maintainability
"""

# Standard library imports
import os
import sys
import json
import logging
from datetime import datetime, timezone

# Load environment variables from .env file for local development only
if not os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

# Import modular components
from modules.config import Config
from modules.email_processor import EmailProcessor
from modules.qbusiness_client import QBusinessClient
from modules.security_utils import sanitize_for_logging

# Configure logging
logger = logging.getLogger(__name__)

def run_exchange_connector(sync_mode='delta', is_lambda=False, event=None, context=None):
    """
    Shared logic for running the Exchange connector.
    Used by both main() and lambda_handler() functions.
    
    Args:
        sync_mode: 'delta' or 'full' sync mode
        is_lambda: True if running in Lambda, False for local execution
        event: Lambda event (only used in Lambda)
        context: Lambda context (only used in Lambda)
    
    Returns:
        For Lambda: dict with statusCode and body
        For local: int exit code (0 for success, non-zero for failure)
    """
    # Input validation
    if sync_mode not in ['delta', 'full']:
        error_msg = f"Invalid sync_mode: {sync_mode}. Must be 'delta' or 'full'"
        logger.error(error_msg)
        if is_lambda:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': error_msg,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        else:
            return 1
    
    # Validate Lambda context if provided
    if is_lambda and context:
        remaining_time = getattr(context, 'get_remaining_time_in_millis', lambda: 900000)()
        if remaining_time < 60000:  # Less than 1 minute remaining
            error_msg = "Insufficient time remaining for execution"
            logger.error(error_msg)
            return {
                'statusCode': 408,
                'body': json.dumps({
                    'error': error_msg,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
    
    execution_start_time = datetime.now(timezone.utc)
    
    # Initialize configuration and components
    config = Config()
    email_processor = EmailProcessor(config)
    qbusiness_client = QBusinessClient(config)
    
    # Set execution start time for timeout tracking (Lambda only)
    if is_lambda:
        email_processor.execution_start_time = execution_start_time
    
    # Verify DynamoDB table is ready
    if not email_processor.dynamodb_client.verify_table_ready():
        error_msg = f'DynamoDB table {config.table_name} is not ready for use'
        if is_lambda:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': error_msg,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        else:
            print(f"❌ {error_msg}")
            return 1
    
    if is_lambda:
        print(f"Lambda execution started at: {execution_start_time.isoformat()}")
        print(f"Event: {sanitize_for_logging(json.dumps(event, default=str))}")
    
    try:
        # Handle force-stop for local execution
        if not is_lambda and len(sys.argv) > 1 and '--force-stop' in sys.argv:
            print("🛑 Force stopping all running sync jobs...")
            if qbusiness_client.force_stop_all_sync_jobs():
                print("✅ All sync jobs stopped successfully")
            else:
                print("⚠️  Some sync jobs may still be running")
            print()
        
        # Check for existing running sync jobs
        if qbusiness_client.has_running_sync_jobs():
            if config.auto_resolve_sync_conflicts:
                if not is_lambda:
                    print("⚠️  Detected existing running sync jobs. Auto-resolve is enabled - will stop existing jobs when sync job is needed.")
            else:
                error_msg = "Cannot start new sync job while another is running. Set AUTO_RESOLVE_SYNC_CONFLICTS=true to automatically stop existing jobs."
                if is_lambda:
                    print(f"⚠️  {error_msg}")
                else:
                    print("⚠️  Detected existing running sync jobs. Auto-resolve is disabled.")
                    print(f"❌ {error_msg}")
                    print("💡 Alternatively, use --force-stop flag to stop all running jobs before starting.")
                    return 1 if not is_lambda else {
                        'statusCode': 500,
                        'body': json.dumps({
                            'error': error_msg,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                    }
        
        # Start sync job once at the beginning if we expect to process any data
        sync_job_id = None
        if not is_lambda:
            print("🚀 Starting Q Business sync job for the full sync process...")
        sync_job_id = qbusiness_client.start_sync_job()
        if not sync_job_id:
            error_msg = "Failed to start Q Business sync job"
            print(f"❌ {error_msg}")
            if is_lambda:
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'error': error_msg,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                }
            else:
                return 1
        
        # Process all configured Exchange accounts
        if is_lambda and email_processor.is_timeout_approaching():
            print("Lambda timeout approaching. Skipping account processing...")
            success = True
            changes = {'processed_count': 0, 'failed_count': 0}
        else:
            if not is_lambda:
                print(f"\n📧 Processing Exchange accounts ({sync_mode} sync)...")
            success, changes = email_processor.process_all_accounts(sync_mode, sync_job_id)
            if not success:
                error_msg = "Failed to process any Exchange accounts"
                print(f"❌ {error_msg}")
                if is_lambda:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'error': error_msg,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                    }
                else:
                    return 1
        
        # Clean up orphaned items
        if is_lambda and email_processor.is_timeout_approaching():
            print("Lambda timeout approaching. Skipping cleanup of orphaned items...")
            changes['orphaned_ids'] = []
        else:
            if not is_lambda:
                print(f"\n🔍 Comprehensive orphaned document detection...")
            orphaned_ids = email_processor.find_orphaned_items_with_sync(sync_mode, sync_job_id)
            changes['orphaned_ids'] = orphaned_ids
        
        # Final processing summary
        if not is_lambda:
            print(f"\n✅ All processing completed successfully with sync job: {qbusiness_client.current_sync_job_id}")
            print(f"📊 Final Summary: {changes.get('processed_count', 0)} documents processed, {changes.get('failed_count', 0)} failed, {len(changes.get('orphaned_ids', []))} orphaned items deleted")
        
        # Stop sync job at the end
        if not is_lambda:
            print("\n🛑 Stopping Q Business sync job...")
        qbusiness_client.stop_sync_job()
        
        # Print final summary
        print(f"Total emails attempted: {email_processor.emails_attempted_count}")
        print(f"Total emails successfully processed: {email_processor.emails_processed_count}")
        
        if is_lambda:
            # Lambda response
            final_status = qbusiness_client.get_sync_job_status()
            execution_time = (datetime.now(timezone.utc) - execution_start_time).total_seconds()
            timeout_reached = email_processor.is_timeout_approaching()
            
            response = {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Exchange connector execution completed successfully',
                    'attempted_emails': email_processor.emails_attempted_count,
                    'processed_emails': email_processor.emails_processed_count,
                    'sync_job_id': qbusiness_client.current_sync_job_id,
                    'sync_job_status': final_status,
                    'sync_job_started': qbusiness_client.sync_job_started,
                    'application_id': config.application_id,
                    'index_id': config.index_id,
                    'data_source_id': config.data_source_id,
                    'execution_time_seconds': round(execution_time, 2),
                    'timeout_reached': timeout_reached,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
            
            print(f"Lambda execution completed successfully: {response}")
            return response
        else:
            # Local execution summary
            print("\n" + "=" * 80)
            print(f"EXECUTION SUMMARY - {sync_mode.upper()} SYNC")
            print("=" * 80)
            print(f"✅ Sync Mode: {sync_mode.upper()}")
            print(f"✅ Total emails attempted: {email_processor.emails_attempted_count}")
            print(f"✅ Total emails successfully processed: {email_processor.emails_processed_count}")
            print(f"✅ Documents processed: {changes.get('processed_count', 0)}")
            print(f"✅ Documents failed: {changes.get('failed_count', 0)}")
            print(f"✅ Orphaned items cleaned: {len(changes.get('orphaned_ids', []))}")
            print(f"✅ Completed at: {datetime.now(timezone.utc).isoformat()}")
            
            # Additional local-only summaries
            if sync_mode == 'delta' and len(changes.get('orphaned_ids', [])) > 0:
                print(f"\n🧹 ORPHANED DOCUMENT CLEANUP SUMMARY:")
                print(f"  📧 Detected {len(changes.get('orphaned_ids', []))} emails that no longer exist in Exchange")
                print(f"  🗑️  These documents were removed from both Q Business and DynamoDB tracking")
                print(f"  ✅ This ensures Q Business index stays synchronized with current Exchange content")
            elif sync_mode == 'full':
                print(f"\n🔄 FULL SYNC CLEANUP SUMMARY:")
                print(f"  🧹 DynamoDB tracking table was cleared before reprocessing")
                print(f"  📧 All current emails were reprocessed")
                print(f"  ✅ This ensures complete synchronization and removes any orphaned documents")
            
            # Account-specific statistics
            print("\n📊 ACCOUNT PROCESSING STATISTICS")
            print("-" * 50)
            account_stats = email_processor.dynamodb_client.get_account_processing_stats()
            for account, stats in account_stats.items():
                if account != 'unknown':
                    processed = stats.get('processed', 0)
                    failed = stats.get('failed', 0)
                    total = stats.get('total', 0)
                    print(f"📧 {account}:")
                    print(f"   ✅ Processed: {processed}")
                    if failed > 0:
                        print(f"   ❌ Failed: {failed}")
                    print(f"   📊 Total: {total}")
            
            total_processed = changes.get('processed_count', 0)
            total_orphaned = len(changes.get('orphaned_ids', []))
            total_changes = total_processed + total_orphaned
            
            if total_changes > 0:
                if sync_mode == 'full':
                    print(f"\n🎉 Full sync completed! Processed {total_processed} documents and cleaned {total_orphaned} orphaned items!")
                else:
                    print(f"\n🎉 Delta sync completed! Processed {total_processed} documents and cleaned {total_orphaned} orphaned items!")
            else:
                if sync_mode == 'full':
                    print(f"\nℹ️  Full sync completed - no emails found to process")
                else:
                    print(f"\nℹ️  Delta sync completed - no changes detected, all emails already indexed and up to date")
            
            return 0
        
    except KeyboardInterrupt:
        if not is_lambda:
            print("\n\n⚠️  Execution interrupted by user")
        try:
            if qbusiness_client.sync_job_started:
                if not is_lambda:
                    print("🛑 Attempting to stop any active sync job...")
                qbusiness_client.stop_sync_job()
        except:
            pass
        
        if is_lambda:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Execution interrupted',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        else:
            return 130
        
    except Exception as e:
        error_message = f"Execution failed: {str(e)}"
        print(f"\n❌ {error_message}")
        
        if not is_lambda:
            import traceback
            traceback.print_exc()
        
        # Try to stop sync job in case of error
        try:
            if qbusiness_client.sync_job_started:
                if not is_lambda:
                    print("\n🛑 Stopping sync job due to error...")
                qbusiness_client.stop_sync_job()
        except:
            pass
        
        if is_lambda:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': error_message,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            }
        else:
            return 1


def lambda_handler(event, context):
    """
    Lambda handler function that runs the Exchange Online Archive connector
    """
    return run_exchange_connector(sync_mode='delta', is_lambda=True, event=event, context=context)


def main():
    """
    Main method for running the Exchange connector locally.
    This allows for local testing and development without Lambda.
    """
    execution_start_time = datetime.now(timezone.utc)
    
    # Initialize configuration (after environment variables are set)
    config = Config()
    
    # Initialize main processor
    email_processor = EmailProcessor(config)
    
    # Initialize Q Business client for sync operations
    qbusiness_client = QBusinessClient(config)
    
    # Get sync mode from config
    sync_mode = config.sync_mode
    
    print("=" * 80)
    print(f"Exchange Online Archive Connector - {sync_mode.upper()} SYNC (Modular)")
    print("=" * 80)
    print(f"Started at: {execution_start_time.isoformat()}")
    print(f"Sync Mode: {sync_mode.upper()}")
    print(f"SYNC_MODE env var: {os.environ.get('SYNC_MODE', 'not set')}")
    print()
    
    # Check required environment variables
    required_vars = config.get_required_vars()
    
    missing_vars = []
    for var_name, var_value in required_vars.items():
        if not var_value:
            missing_vars.append(var_name)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these environment variables before running.")
        return 1
    
    print("✅ Configuration validated")
    print(f"Q Business Application: {config.application_id}")
    if config.testing_email_limit is None:
        print("Processing Limit: No limit (process all emails) - Local execution default")
    else:
        print(f"Processing Limit: {config.testing_email_limit} emails")
    print(f"DynamoDB Table: {config.table_name}")
    print(f"Email Addresses: {', '.join(config.primary_smtp_addresses)}")
    print()
    
    print("🔍 Verifying DynamoDB table...")
    print("📋 Q Business sync job will be started once at the beginning and reused throughout the process...")
    print()
    
    # Use the shared logic
    return run_exchange_connector(sync_mode=sync_mode, is_lambda=False)


if __name__ == "__main__":
    """
    Entry point for local execution.
    Set environment variables and run: python qbusiness_ews_sync_modular.py
    """
    import sys
    
    # Check for help flag or force-stop flag
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("Exchange Online Archive Connector - Local Execution (Modular)")
        print("=" * 60)
        print()
        print("Usage: python qbusiness_ews_sync_modular.py [--force-stop]")
        print()
        print("Options:")
        print("  --force-stop                - Force stop all running sync jobs before starting")
        print()
        print("Required Environment Variables:")
        print("  EXCHANGE_CLIENT_ID           - Azure AD Application Client ID")
        print("  EXCHANGE_CLIENT_SECRET       - Azure AD Application Client Secret")
        print("  EXCHANGE_TENANT_ID          - Azure AD Tenant ID")
        print("  EXCHANGE_PRIMARY_SMTP_ADDRESS - Exchange mailbox email address(es)")
        print("                                 Single: user@domain.com")
        print("                                 Multiple: user1@domain.com,user2@domain.com")
        print("  QBUSINESS_APPLICATION_ID     - Amazon Q Business Application ID")
        print("  QBUSINESS_INDEX_ID          - Amazon Q Business Index ID")
        print("  QBUSINESS_DATA_SOURCE_ID    - Amazon Q Business Data Source ID")
        print()
        print("Optional Environment Variables:")
        print("  EXCHANGE_SERVER             - Exchange server URL (default: outlook.office365.com)")
        print("  DYNAMODB_TABLE_NAME         - DynamoDB table name (default: processed-emails)")
        print("  EMAIL_PROCESSING_LIMIT      - Max emails to process (default: 1 for Lambda, no limit for local)")
        print("  SYNC_MODE                   - Sync mode: 'delta' or 'full' (default: delta)")
        print("  AWS_DEFAULT_REGION          - AWS region (default: us-east-1)")
        print()
        print("Sync Modes:")
        print("  delta                       - Only process new/changed emails (default)")
        print("  full                        - Reprocess all emails, clearing existing records")
        print()
        print("Examples:")
        print("  # Single email address")
        print("  export EXCHANGE_PRIMARY_SMTP_ADDRESS='user@domain.com'")
        print()
        print("  # Multiple email addresses")
        print("  export EXCHANGE_PRIMARY_SMTP_ADDRESS='user1@domain.com,user2@domain.com'")
        print()
        print("  # Full example")
        print("  export EXCHANGE_CLIENT_ID='your-client-id'")
        print("  export EXCHANGE_CLIENT_SECRET='your-client-secret'")
        print("  export EXCHANGE_TENANT_ID='your-tenant-id'")
        print("  export EXCHANGE_PRIMARY_SMTP_ADDRESS='user1@domain.com,user2@domain.com'")
        print("  export QBUSINESS_APPLICATION_ID='your-app-id'")
        print("  export QBUSINESS_INDEX_ID='your-index-id'")
        print("  export QBUSINESS_DATA_SOURCE_ID='your-datasource-id'")
        print("  python qbusiness_ews_sync_modular.py")
        sys.exit(0)
    
    # Run the main function
    exit_code = main()
    sys.exit(exit_code)