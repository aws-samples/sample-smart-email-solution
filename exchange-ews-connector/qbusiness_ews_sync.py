"""
Exchange Online Archive Connector - Modular Version
All functionality organized into separate modules for better maintainability
"""

# Standard library imports
import os
import sys
import json
import logging
import time
import threading
import signal
from datetime import datetime, timezone

# Load environment variables from .env file for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Import modular components
from modules.config import Config
from modules.email_processor import EmailProcessor
from modules.qbusiness_client import QBusinessClient
from modules.sync_job_coordinator import SyncJobCoordinator
from modules.security_utils import sanitize_for_logging
from health_server import start_health_server, stop_health_server

# Configure logging
logger = logging.getLogger(__name__)

# Suppress verbose exchangelib logging for naive datetime warnings
exchangelib_fields_logger = logging.getLogger('exchangelib.fields')
exchangelib_fields_logger.setLevel(logging.WARNING)  # Only show WARNING and above, suppress INFO

def get_assigned_accounts(all_accounts, container_index=None, total_containers=None):
    """
    Split email accounts across multiple containers for parallel processing.
    
    Args:
        all_accounts: List of all email accounts to process
        container_index: Current container index (0-based)
        total_containers: Total number of containers
    
    Returns:
        List of accounts assigned to this container
    """
    if container_index is None or total_containers is None:
        # No splitting - return all accounts
        return all_accounts
    
    if total_containers <= 1:
        return all_accounts
    
    if container_index >= total_containers:
        logger.warning(f"Container index {container_index} >= total containers {total_containers}")
        return []
    
    # Round-robin assignment of accounts to containers
    assigned_accounts = []
    for i, account in enumerate(all_accounts):
        if i % total_containers == container_index:
            assigned_accounts.append(account)
    
    logger.info(f"Container {container_index + 1}/{total_containers} assigned {len(assigned_accounts)} accounts: {assigned_accounts}")
    return assigned_accounts

def run_exchange_connector(sync_mode='delta', container_index=None, total_containers=None):
    """
    Main logic for running the Exchange connector.
    
    Args:
        sync_mode: 'delta' or 'full' sync mode
        container_index: Current container index for account splitting (0-based)
        total_containers: Total number of containers for account splitting
    
    Returns:
        int exit code (0 for success, non-zero for failure)
    """
    # Input validation
    if sync_mode not in ['delta', 'full']:
        error_msg = f"Invalid sync_mode: {sync_mode}. Must be 'delta' or 'full'"
        logger.error(error_msg)
        return 1
    
    execution_start_time = datetime.now(timezone.utc)
    
    # Initialize configuration and components
    config = Config()
    
    # Apply account splitting if specified
    if container_index is not None and total_containers is not None:
        original_accounts = config.primary_smtp_addresses.copy()
        assigned_accounts = get_assigned_accounts(original_accounts, container_index, total_containers)
        config.primary_smtp_addresses = assigned_accounts
        
        if not assigned_accounts:
            print(f"ℹ️  Container {container_index + 1}/{total_containers} has no accounts assigned - exiting gracefully")
            return 0
        
        print(f"🔀 Account splitting enabled: Container {container_index + 1}/{total_containers}")
        print(f"📧 Processing {len(assigned_accounts)} of {len(original_accounts)} total accounts")
        print(f"📋 Assigned accounts: {', '.join(assigned_accounts)}")
        print()
    
    email_processor = EmailProcessor(config)
    qbusiness_client = QBusinessClient(config)
    
    # Initialize sync job coordinator for distributed sync job management
    sync_coordinator = SyncJobCoordinator(config, qbusiness_client)
    qbusiness_client.set_sync_coordinator(sync_coordinator)
    
    # Verify DynamoDB table is ready
    if not email_processor.dynamodb_client.verify_table_ready():
        error_msg = f'DynamoDB table {config.table_name} is not ready for use'
        print(f"❌ {error_msg}")
        return 1
    
    try:
        # Handle force-stop for execution
        if len(sys.argv) > 1 and '--force-stop' in sys.argv:
            print("🛑 Force stopping all running sync jobs...")
            if qbusiness_client.force_stop_all_sync_jobs():
                print("✅ All sync jobs stopped successfully")
            else:
                print("⚠️  Some sync jobs may still be running")
            print()
        
        # Check for existing running sync jobs
        if qbusiness_client.has_running_sync_jobs():
            if config.auto_resolve_sync_conflicts:
                print("⚠️  Detected existing running sync jobs. Auto-resolve is enabled - will stop existing jobs when sync job is needed.")
            else:
                error_msg = "Cannot start new sync job while another is running. Set AUTO_RESOLVE_SYNC_CONFLICTS=true to automatically stop existing jobs."
                print("⚠️  Detected existing running sync jobs. Auto-resolve is disabled.")
                print(f"❌ {error_msg}")
                print("💡 Alternatively, use --force-stop flag to stop all running jobs before starting.")
                return 1
        
        # Clean up any stale sync job registrations first
        sync_coordinator.cleanup_stale_registrations()
        
        # Start or join sync job using coordinator
        print("🚀 Starting or joining Q Business sync job for the full sync process...")
        sync_job_id = qbusiness_client.start_sync_job_if_needed()
        if not sync_job_id:
            error_msg = "Failed to start or join Q Business sync job"
            print(f"❌ {error_msg}")
            return 1
        
        # Process all configured Exchange accounts
        print(f"\n📧 Processing Exchange accounts ({sync_mode} sync)...")
        success, changes = email_processor.process_all_accounts(sync_mode, sync_job_id)
        if not success:
            error_msg = "Failed to process any Exchange accounts"
            print(f"❌ {error_msg}")
            return 1
        
        # Orphaned items are now cleaned up during folder processing
        print(f"\n✅ Orphaned items were cleaned up during folder processing")
        
        # Final processing summary
        print(f"\n✅ All processing completed successfully with sync job: {qbusiness_client.current_sync_job_id}")
        print(f"📊 Final Summary: {changes.get('processed_count', 0)} documents processed, {changes.get('failed_count', 0)} failed, {changes.get('orphaned_count', 0)} orphaned items deleted")
        
        # Stop sync job at the end
        print("\n🛑 Stopping Q Business sync job...")
        stop_success = qbusiness_client.stop_sync_job()
        if stop_success:
            print("✅ Q Business sync job stopped successfully")
        else:
            print("⚠️  Warning: Q Business sync job may not have stopped properly")
        
        # Print final summary
        print(f"Total emails attempted: {email_processor.emails_attempted_count}")
        print(f"Total emails successfully processed: {email_processor.emails_processed_count}")
        
        # Execution summary
        print("\n" + "=" * 80)
        print(f"EXECUTION SUMMARY - {sync_mode.upper()} SYNC")
        print("=" * 80)
        print(f"✅ Sync Mode: {sync_mode.upper()}")
        print(f"✅ Total emails attempted: {email_processor.emails_attempted_count}")
        print(f"✅ Total emails successfully processed: {email_processor.emails_processed_count}")
        print(f"✅ Documents processed: {changes.get('processed_count', 0)}")
        print(f"✅ Documents failed: {changes.get('failed_count', 0)}")
        print(f"✅ Orphaned items cleaned: {changes.get('orphaned_count', 0)}")
        print(f"✅ Completed at: {datetime.now(timezone.utc).isoformat()}")
        
        # Additional summaries
        if sync_mode == 'delta' and changes.get('orphaned_count', 0) > 0:
            print(f"\n🧹 ORPHANED DOCUMENT CLEANUP SUMMARY:")
            print(f"  📧 Detected {changes.get('orphaned_count', 0)} emails that no longer exist in Exchange")
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
        account_stats = email_processor.get_account_processing_stats()
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
        total_orphaned = changes.get('orphaned_count', 0)
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
        print("\n\n⚠️  Execution interrupted by user")
        try:
            if qbusiness_client.sync_job_started:
                print("🛑 Attempting to stop any active sync job...")
                stop_success = qbusiness_client.stop_sync_job()
                if stop_success:
                    print("✅ Sync job stopped successfully")
                else:
                    print("⚠️  Warning: Sync job may not have stopped properly")
        except Exception as stop_e:
            print(f"⚠️  Error stopping sync job during interrupt: {stop_e}")
        return 130
        
    except Exception as e:
        error_message = f"Execution failed: {str(e)}"
        print(f"\n❌ {error_message}")
        
        import traceback
        traceback.print_exc()
        
        # Try to stop sync job in case of error
        try:
            if qbusiness_client.sync_job_started:
                print("\n🛑 Stopping sync job due to error...")
                stop_success = qbusiness_client.stop_sync_job()
                if stop_success:
                    print("✅ Sync job stopped successfully")
                else:
                    print("⚠️  Warning: Sync job may not have stopped properly")
        except Exception as stop_e:
            print(f"⚠️  Error stopping sync job during error handling: {stop_e}")
        
        return 1

class SyncScheduler:
    """
    Scheduler that runs sync operations every 24 hours in a continuously running container.
    """
    
    def __init__(self):
        self.running = True
        self.sync_in_progress = False
        self.sync_thread = None
        self.health_server = None
        
        # Get configuration
        self.container_index = None
        self.total_containers = None
        self.sync_mode = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False
        
        # Stop health server
        stop_health_server()
        
        # Wait for current sync to complete if running
        if self.sync_in_progress and self.sync_thread:
            print("⏳ Waiting for current sync to complete...")
            self.sync_thread.join(timeout=30)  # Wait up to 30 seconds
    
    def _parse_arguments(self):
        """Parse command line arguments."""
        if len(sys.argv) > 1:
            for arg in sys.argv[1:]:
                arg_lower = arg.lower()
                if arg_lower in ['full_sync', 'full']:
                    self.sync_mode = 'full'
                elif arg_lower in ['delta_sync', 'delta']:
                    self.sync_mode = 'delta'
                elif arg_lower == '--once':
                    # Run once and exit (for testing)
                    return 'once'
                elif arg_lower not in ['--force-stop', '-h', '--help']:
                    print(f"❌ Invalid argument: {arg}")
                    print("Valid options: full_sync, delta_sync, full, delta, --once")
                    return 'error'
        return 'continuous'
    
    def _get_container_config(self):
        """Get container splitting configuration."""
        if os.environ.get('CONTAINER_INDEX') and os.environ.get('TOTAL_CONTAINERS'):
            try:
                self.container_index = int(os.environ.get('CONTAINER_INDEX'))
                self.total_containers = int(os.environ.get('TOTAL_CONTAINERS'))
                
                if self.container_index < 0 or self.total_containers <= 0:
                    print("❌ Invalid container parameters: CONTAINER_INDEX must be >= 0, TOTAL_CONTAINERS must be > 0")
                    return False
                    
                if self.container_index >= self.total_containers:
                    print(f"❌ Invalid container parameters: CONTAINER_INDEX ({self.container_index}) must be < TOTAL_CONTAINERS ({self.total_containers})")
                    return False
                    
            except ValueError:
                print("❌ Invalid container parameters: CONTAINER_INDEX and TOTAL_CONTAINERS must be integers")
                return False
        
        return True
    
    def _run_sync(self):
        """Run a single sync operation in a separate thread."""
        if self.sync_in_progress:
            print("⚠️  Sync already in progress, skipping this cycle")
            return
        
        self.sync_in_progress = True
        
        try:
            execution_start_time = datetime.now(timezone.utc)
            
            # Initialize configuration
            config = Config()
            
            # Override sync mode if provided via command line
            if self.sync_mode:
                config.sync_mode = self.sync_mode
            
            sync_mode = config.sync_mode
            
            print("\n" + "=" * 80)
            print(f"Exchange Online Archive Connector - {sync_mode.upper()} SYNC")
            print("=" * 80)
            print(f"Started at: {execution_start_time.isoformat()}")
            print(f"Sync Mode: {sync_mode.upper()}")
            if self.container_index is not None:
                print(f"Container: {self.container_index + 1}/{self.total_containers}")
            print()
            
            # Run the sync
            exit_code = run_exchange_connector(
                sync_mode=sync_mode, 
                container_index=self.container_index, 
                total_containers=self.total_containers
            )
            
            if exit_code == 0:
                print("✅ Sync completed successfully")
            else:
                print(f"⚠️  Sync completed with warnings (exit code: {exit_code})")
                
        except Exception as e:
            print(f"❌ Sync failed with error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.sync_in_progress = False
    
    def _sync_worker(self):
        """Worker thread that runs the sync operation."""
        self._run_sync()
    
    def run_continuous(self):
        """Run the scheduler continuously with 24-hour intervals."""
        print("🚀 Starting Exchange EWS Connector Scheduler")
        print("⏰ Sync interval: 24 hours")
        print("🔄 Running continuously until stopped...")
        print()
        
        # Start health server
        self.health_server = start_health_server(port=8080)
        
        # Validate configuration
        if not self._get_container_config():
            return 1
        
        # Run first sync immediately
        print("🏃 Running initial sync...")
        self.sync_thread = threading.Thread(target=self._sync_worker)
        self.sync_thread.start()
        self.sync_thread.join()  # Wait for first sync to complete
        
        # Main scheduler loop
        while self.running:
            try:
                # Wait 24 hours (86400 seconds)
                for i in range(86400):  # 24 hours = 86400 seconds
                    if not self.running:
                        break
                    time.sleep(1)
                
                if self.running:
                    print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Starting scheduled sync...")
                    self.sync_thread = threading.Thread(target=self._sync_worker)
                    self.sync_thread.start()
                    # Don't wait for completion, let it run in background
                    
            except KeyboardInterrupt:
                print("\n🛑 Received keyboard interrupt, shutting down...")
                break
        
        print("👋 Scheduler stopped")
        return 0
    
    def run_once(self):
        """Run sync once and exit (for testing)."""
        print("🏃 Running sync once...")
        
        if not self._get_container_config():
            return 1
        
        self._run_sync()
        return 0

def main():
    """
    Main method for running the Exchange connector.
    Supports both continuous scheduling and one-time execution.
    """
    scheduler = SyncScheduler()
    
    # Parse arguments
    mode = scheduler._parse_arguments()
    
    if mode == 'error':
        return 1
    elif mode == 'once':
        return scheduler.run_once()
    else:
        return scheduler.run_continuous()


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
        print("Usage: python qbusiness_ews_sync_modular.py [sync_mode] [--force-stop]")
        print()
        print("Sync Modes:")
        print("  full_sync, full             - Reprocess all emails, clearing existing records")
        print("  delta_sync, delta           - Only process new/changed emails (default)")
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
        print("  ENABLE_THREADING            - Enable parallel processing: 'true' or 'false' (default: true)")
        print("  MAX_WORKER_THREADS          - Maximum number of worker threads (default: 4)")
        print("  THREAD_BATCH_SIZE           - Number of emails per thread batch (default: 50)")
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