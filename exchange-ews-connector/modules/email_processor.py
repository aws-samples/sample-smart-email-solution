"""
Email Processor Module
Main email processor that coordinates the workflow between all components
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from .config import Config
from .ews_client import EWSClient
from .document_processor import DocumentProcessor
from .dynamodb_client import DynamoDBClient
from .qbusiness_client import QBusinessClient

logger = logging.getLogger(__name__)

class EmailProcessor:
    """Main email processor that coordinates the workflow"""
    
    def __init__(self, config: Config):
        self.config = config
        self.ews_client = EWSClient(config)
        self.document_processor = DocumentProcessor(config)
        self.dynamodb_client = DynamoDBClient(config)
        self.qbusiness_client = QBusinessClient(config)
        
        # Processing counters
        self.emails_processed_count = 0
        self.emails_attempted_count = 0
        self.execution_start_time = None
    
    def is_timeout_approaching(self) -> bool:
        """Check if we're approaching the Lambda timeout (14 minutes) - only applies when running in Lambda"""
        # Only check timeout when running in Lambda environment
        if not os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            return False
            
        if self.execution_start_time is None:
            return False
        
        elapsed_time = (datetime.now(timezone.utc) - self.execution_start_time).total_seconds()
        return elapsed_time >= self.config.lambda_timeout_seconds
    
    def _delete_documents_and_records(self, email_ids: list, sync_job_id: str = None, reason: str = "deletion") -> bool:
        """Delete documents from Q Business and corresponding records from DynamoDB"""
        if not email_ids:
            return True
            
        try:
            print(f"🗑️  Deleting {len(email_ids)} documents from Q Business and DynamoDB ({reason})...")
            
            # Delete from Q Business first
            qbusiness_success = self.qbusiness_client.batch_delete_documents(email_ids)
            if not qbusiness_success:
                print(f"⚠️  Some Q Business document deletions failed for {reason}")
            
            # Delete from DynamoDB
            dynamodb_deleted_count = 0
            for email_id in email_ids:
                if self.dynamodb_client.delete_email_record(email_id):
                    dynamodb_deleted_count += 1
            
            print(f"✅ Deleted {len(email_ids)} documents from Q Business and {dynamodb_deleted_count} records from DynamoDB ({reason})")
            return qbusiness_success and dynamodb_deleted_count == len(email_ids)
            
        except Exception as e:
            print(f"❌ Error deleting documents and records ({reason}): {e}")
            return False
    
    def _submit_document_batch(self, documents: list, documents_to_delete: list, emails_to_mark: list, sync_job_id: str = None) -> bool:
        """Submit a batch of documents to Q Business and mark emails as processed based on individual success/failure"""
        failed_document_ids = set()
        qbusiness_submission_failed = False
        
        try:
            batch_size = len(documents) + len(documents_to_delete)
            if batch_size == 0:
                # Still mark emails as processed even if no documents to submit
                if emails_to_mark:
                    print(f"📝 Marking {len(emails_to_mark)} emails as processed (no documents to submit)")
                    self._mark_emails_in_dynamodb(emails_to_mark, failed_document_ids)
                return True
            
            print(f"📤 Submitting batch: {len(documents)} documents, {len(documents_to_delete)} deletions")
            
            # Delete documents that need updating first (only from Q Business, not DynamoDB)
            if documents_to_delete:
                try:
                    print(f"🗑️  Deleting {len(documents_to_delete)} documents for updates...")
                    self.qbusiness_client.batch_delete_documents(documents_to_delete)
                except Exception as delete_e:
                    print(f"⚠️  Error deleting documents for updates: {delete_e}")
                    # Continue with submission even if deletion fails
            
            # Submit new/updated documents and get detailed results
            if documents:
                try:
                    print(f"📄 Submitting {len(documents)} documents...")
                    success, failed_documents = self.qbusiness_client.batch_put_documents(documents)
                    
                    # Extract failed document IDs
                    failed_document_ids = {failed_doc.get('id') for failed_doc in failed_documents if failed_doc.get('id')}
                    
                    successful_count = len(documents) - len(failed_documents)
                    self.emails_processed_count += successful_count
                    
                    if success:
                        print(f"✅ All {len(documents)} documents submitted successfully")
                    else:
                        print(f"⚠️  {successful_count}/{len(documents)} documents submitted successfully, {len(failed_documents)} failed")
                        
                except Exception as submit_e:
                    print(f"❌ Error submitting documents to Q Business: {submit_e}")
                    qbusiness_submission_failed = True
                    # Mark all documents as failed if Q Business submission completely fails
                    failed_document_ids = {doc.get('id') for doc in documents if doc.get('id')}
            
        except Exception as e:
            print(f"❌ Unexpected error in document batch submission: {e}")
            qbusiness_submission_failed = True
            # Mark all documents as failed if there's an unexpected error
            failed_document_ids = {doc.get('id') for doc in documents if doc.get('id')}
        
        finally:
            # ALWAYS mark emails in DynamoDB, regardless of what happened above
            if emails_to_mark:
                try:
                    print(f"📝 Marking {len(emails_to_mark)} emails in DynamoDB based on submission results...")
                    self._mark_emails_in_dynamodb(emails_to_mark, failed_document_ids, qbusiness_submission_failed)
                except Exception as mark_e:
                    print(f"❌ Critical error marking emails in DynamoDB: {mark_e}")
                    # Even if DynamoDB marking fails, try to mark each email individually
                    self._mark_emails_individually(emails_to_mark, failed_document_ids, qbusiness_submission_failed)
        
        return not qbusiness_submission_failed
    
    def _mark_emails_in_dynamodb(self, emails_to_mark: list, failed_document_ids: set, qbusiness_submission_failed: bool = False):
        """Mark emails in DynamoDB based on Q Business submission results"""
        successful_marks = 0
        failed_marks = 0
        
        for email_info in emails_to_mark:
            email_id = email_info['email_id']
            
            # Determine final status based on Q Business submission result
            if qbusiness_submission_failed:
                # Entire Q Business submission failed, mark as failed
                final_status = 'failed'
                failed_marks += 1
                print(f"  ❌ Marking email {email_id[:20]}... as FAILED (Q Business submission completely failed)")
            elif email_id in failed_document_ids:
                # Document failed in Q Business, mark as failed
                final_status = 'failed'
                failed_marks += 1
                print(f"  ❌ Marking email {email_id[:20]}... as FAILED (Q Business submission failed)")
            elif email_info['status'] == 'failed':
                # Document creation failed before Q Business submission
                final_status = 'failed'
                failed_marks += 1
                print(f"  ❌ Marking email {email_id[:20]}... as FAILED (document creation failed)")
            else:
                # Document was successfully submitted to Q Business
                final_status = 'processed'
                successful_marks += 1
            
            self.dynamodb_client.mark_email_processed(
                email_info['email_id'],
                email_info['folder_name'],
                email_info['datetime_created'],
                final_status,
                email_info['account_email']
            )
        
        print(f"📊 DynamoDB marking completed: {successful_marks} processed, {failed_marks} failed")
    
    def _mark_emails_individually(self, emails_to_mark: list, failed_document_ids: set, qbusiness_submission_failed: bool = False):
        """Fallback method to mark emails individually if batch marking fails"""
        print(f"🔄 Attempting to mark {len(emails_to_mark)} emails individually as fallback...")
        
        successful_marks = 0
        failed_marks = 0
        
        for email_info in emails_to_mark:
            try:
                email_id = email_info['email_id']
                
                # Determine final status
                if qbusiness_submission_failed or email_id in failed_document_ids or email_info['status'] == 'failed':
                    final_status = 'failed'
                    failed_marks += 1
                else:
                    final_status = 'processed'
                    successful_marks += 1
                
                self.dynamodb_client.mark_email_processed(
                    email_info['email_id'],
                    email_info['folder_name'],
                    email_info['datetime_created'],
                    final_status,
                    email_info['account_email']
                )
                
            except Exception as individual_e:
                print(f"❌ Failed to mark individual email {email_info.get('email_id', 'unknown')}: {individual_e}")
                failed_marks += 1
        
        print(f"📊 Individual DynamoDB marking completed: {successful_marks} processed, {failed_marks} failed")   
 
    def process_folder_emails(self, folder, folder_name: str, account_email: str, sync_mode: str = 'delta', sync_job_id: str = None) -> Tuple[bool, Dict[str, int]]:
        """Process emails in a specific folder with streaming document submission"""
        try:
            if self.ews_client.should_skip_folder(folder, folder_name):
                print(f"Skipping folder: {folder_name}")
                return True, {'processed_count': 0, 'failed_count': 0}
            
            print(f"Processing folder: {folder_name} (Total: {folder.total_count} emails) - {sync_mode.upper()} mode")
            
            if folder.total_count == 0:
                print(f"  No emails in folder {folder_name}")
                return True, {'processed_count': 0, 'failed_count': 0}
            
            # Get processed email IDs for this folder (only for delta sync)
            processed_ids = set()
            if sync_mode == 'delta':
                processed_ids = self.dynamodb_client.get_processed_email_ids_for_folder(folder_name)
                print(f"  Found {len(processed_ids)} already processed emails in folder {folder_name}")
            else:
                print(f"  Full sync mode - processing all emails in folder {folder_name}")
            
            # Get all emails in folder
            items = folder.all().only('id', 'datetime_created', 'subject')
            
            # Streaming processing with batched submission
            documents_batch = []
            emails_to_mark_batch = []
            documents_to_delete_batch = []
            
            processed_count = 0
            failed_count = 0
            
            for item in items:
                if self.is_timeout_approaching():
                    print("Lambda timeout approaching. Stopping email processing...")
                    break
                
                # Check processing limit for testing
                if self.config.testing_email_limit is not None and self.emails_attempted_count >= self.config.testing_email_limit:
                    print(f"Reached processing limit of {self.config.testing_email_limit} emails")
                    break
                
                self.emails_attempted_count += 1
                
                # Check if email needs processing (delta mode only)
                email_id_str = str(item.id)
                needs_processing = True
                is_update = False
                
                if sync_mode == 'delta':
                    if email_id_str in processed_ids:
                        # Check if email was modified since last processing
                        last_modified = getattr(item, 'last_modified_time', None)
                        if self.dynamodb_client.needs_update(email_id_str, last_modified):
                            print(f"  📝 Email {email_id_str[:20]}... was modified, will update")
                            is_update = True
                            needs_processing = True
                        else:
                            needs_processing = False
                
                if not needs_processing:
                    continue
                
                try:
                    # If this is an update, we need to delete the existing document first
                    if is_update:
                        print(f"  🗑️  Marking document for deletion before update: {email_id_str[:20]}...")
                        documents_to_delete_batch.append(email_id_str)
                    
                    # Create Q Business document with ACL for the account owner
                    document = self.document_processor.create_qbusiness_document(item, folder_name, folder, account_email)
                    if document:
                        documents_batch.append(document)
                        processed_count += 1
                    else:
                        failed_count += 1
                    
                    # Store email info for marking as processed later
                    status = 'processed' if document else 'failed'
                    emails_to_mark_batch.append({
                        'email_id': str(item.id),
                        'folder_name': folder_name,
                        'datetime_created': item.datetime_created,
                        'status': status,
                        'account_email': account_email
                    })
                    
                    # Submit batch when it reaches the configured size
                    if len(documents_batch) >= self.config.document_batch_size:
                        try:
                            self._submit_document_batch(documents_batch, documents_to_delete_batch, emails_to_mark_batch, sync_job_id)
                        except Exception as batch_e:
                            print(f"❌ Critical error in batch submission: {batch_e}")
                            # Even if batch submission fails completely, try to mark emails as failed
                            self._mark_emails_individually(emails_to_mark_batch, set(), True)
                        finally:
                            # Always clear batches after submission attempt
                            documents_batch = []
                            documents_to_delete_batch = []
                            emails_to_mark_batch = []
                    
                except Exception as e:
                    print(f"Error processing email {item.id}: {e}")
                    failed_count += 1
                    emails_to_mark_batch.append({
                        'email_id': str(item.id),
                        'folder_name': folder_name,
                        'datetime_created': item.datetime_created,
                        'status': 'failed',
                        'account_email': account_email
                    })
            
            # Submit any remaining documents in the final batch
            if documents_batch or documents_to_delete_batch or emails_to_mark_batch:
                try:
                    self._submit_document_batch(documents_batch, documents_to_delete_batch, emails_to_mark_batch, sync_job_id)
                except Exception as final_batch_e:
                    print(f"❌ Critical error in final batch submission: {final_batch_e}")
                    # Even if final batch submission fails completely, try to mark emails as failed
                    self._mark_emails_individually(emails_to_mark_batch, set(), True)
            
            print(f"  ✅ Folder {folder_name} completed: {processed_count} processed, {failed_count} failed")
            return True, {'processed_count': processed_count, 'failed_count': failed_count}
            
        except Exception as e:
            print(f"Error processing folder {folder_name}: {e}")
            return False, {'processed_count': 0, 'failed_count': 0}
    
    def process_account_folders(self, account, account_email: str, folder_root, root_name: str, sync_mode: str = 'delta', sync_job_id: str = None) -> Tuple[bool, Dict[str, int]]:
        """Process all folders in an account (main or archive) with streaming submission"""
        try:
            print(f"Processing {root_name} folders for {account_email} - {sync_mode.upper()} mode...")
            
            total_processed = 0
            total_failed = 0
            
            def process_folder_recursive(folder, parent_path=""):
                nonlocal total_processed, total_failed
                
                if self.is_timeout_approaching():
                    return False
                
                folder_path = f"{parent_path}/{folder.name}" if parent_path else folder.name
                
                # Strip "Top of Information Store/" prefix from folder display name
                if folder_path.startswith("Top of Information Store/"):
                    folder_path = folder_path[len("Top of Information Store/"):]
                elif folder_path == "Top of Information Store":
                    folder_path = "Root"  # Use "Root" instead of empty string for clarity
                
                # Strip "Root/" prefix from folder display name
                if folder_path.startswith("Root/"):
                    folder_path = folder_path[len("Root/"):]
                elif folder_path == "Root":
                    folder_path = ""  # Root folder becomes empty path
                
                # Skip processing if folder_path is empty (root folder case)
                if not folder_path:
                    folder_path = "Root"
                
                # Process current folder with streaming submission
                success, folder_stats = self.process_folder_emails(folder, folder_path, account_email, sync_mode, sync_job_id)
                if not success:
                    return False
                
                # Accumulate statistics
                total_processed += folder_stats.get('processed_count', 0)
                total_failed += folder_stats.get('failed_count', 0)
                
                # Process child folders
                if hasattr(folder, 'children') and folder.children:
                    for child_folder in folder.children:
                        if not process_folder_recursive(child_folder, folder_path):
                            return False
                
                return True
            
            success = process_folder_recursive(folder_root)
            return success, {'processed_count': total_processed, 'failed_count': total_failed}
            
        except Exception as e:
            print(f"Error processing {root_name} folders for {account_email}: {e}")
            return False, {'processed_count': 0, 'failed_count': 0}
    
    def process_single_account(self, smtp_address: str, sync_mode: str = 'delta', sync_job_id: str = None) -> Tuple[bool, Dict[str, int]]:
        """Process a single Exchange account with streaming document submission"""
        try:
            print(f"\n{'='*60}")
            print(f"Processing Exchange account: {smtp_address} - {sync_mode.upper()} SYNC")
            print(f"{'='*60}")
            
            # For full sync, clear existing processed records for this account
            if sync_mode == 'full':
                print(f"🔄 Full sync mode - clearing existing processed records for {smtp_address}")
                self.dynamodb_client.clear_processed_emails_for_account(smtp_address)
            
            # Create Exchange account connection
            account = self.ews_client.create_exchange_account(smtp_address)
            if not account:
                print(f"Failed to connect to account: {smtp_address}")
                return False, {'processed_count': 0, 'failed_count': 0}
            
            total_processed = 0
            total_failed = 0
            
            # Process main mailbox folders (if enabled)
            if self.config.process_main_mailbox and hasattr(account, 'msg_folder_root'):
                print(f"📁 Processing main mailbox folders for {smtp_address}")
                success, main_stats = self.process_account_folders(account, smtp_address, account.msg_folder_root, "main mailbox", sync_mode, sync_job_id)
                if not success:
                    return False, {'processed_count': 0, 'failed_count': 0}
                total_processed += main_stats.get('processed_count', 0)
                total_failed += main_stats.get('failed_count', 0)
            elif not self.config.process_main_mailbox:
                print(f"⏭️  Skipping main mailbox folders for {smtp_address} (PROCESS_MAIN_MAILBOX=false)")
            
            # Process archive folders if available
            if hasattr(account, 'archive_msg_folder_root') and account.archive_msg_folder_root:
                success, archive_stats = self.process_account_folders(account, smtp_address, account.archive_msg_folder_root, "archive", sync_mode, sync_job_id)
                if not success:
                    return False, {'processed_count': total_processed, 'failed_count': total_failed}
                total_processed += archive_stats.get('processed_count', 0)
                total_failed += archive_stats.get('failed_count', 0)
            
            print(f"✅ Completed processing account: {smtp_address} - {total_processed} processed, {total_failed} failed")
            return True, {'processed_count': total_processed, 'failed_count': total_failed}
            
        except Exception as e:
            print(f"❌ Error processing account {smtp_address}: {e}")
            return False, {'processed_count': 0, 'failed_count': 0} 
   
    def prepare_full_sync(self, sync_job_id: str = None) -> bool:
        """
        Prepare for full sync by clearing both Q Business documents and DynamoDB tracking table.
        This ensures we reprocess all emails and clean up any orphaned documents.
        """
        try:
            print("🧹 Preparing for FULL SYNC - clearing Q Business documents and DynamoDB tracking table...")
            
            # Get all processed emails before clearing
            all_processed_set = self.dynamodb_client.get_all_processed_email_ids()
            all_processed = list(all_processed_set)  # Convert set to list for indexing
            print(f"  📊 Found {len(all_processed)} existing processed emails in DynamoDB")
            
            if all_processed:
                print("  🗑️  Clearing both Q Business documents and DynamoDB tracking table for fresh start...")
                
                # Use provided sync job ID - it should already be started by the main process
                if not sync_job_id:
                    print("  ❌ No sync job ID provided for full sync preparation")
                    return False
                else:
                    print(f"  📋 Using existing sync job for cleanup: {sync_job_id}")
                    should_stop_sync_job = False
                
                try:
                    # Delete documents from Q Business and records from DynamoDB in batches
                    batch_size = 10  # Smaller batch size for Q Business operations
                    total_deleted = 0
                    
                    for i in range(0, len(all_processed), batch_size):
                        batch = all_processed[i:i + batch_size]
                        
                        print(f"  🗑️  Deleting batch {i//batch_size + 1}/{(len(all_processed) + batch_size - 1)//batch_size}...")
                        
                        # Use helper method to ensure both Q Business and DynamoDB deletions
                        if self._delete_documents_and_records(batch, sync_job_id, "full sync preparation"):
                            total_deleted += len(batch)
                        
                        if self.is_timeout_approaching():
                            print("  ⏰ Timeout approaching, stopping full sync preparation...")
                            break
                    
                    print(f"  ✅ Cleared {total_deleted}/{len(all_processed)} documents from both Q Business and DynamoDB")
                    
                finally:
                    # Don't stop the sync job - it will be stopped by the main process
                    pass
                    
            else:
                print("  ✅ DynamoDB table is already empty, no Q Business cleanup needed")
            
            print("🎯 Full sync preparation completed - ready to reprocess all emails")
            return True
            
        except Exception as e:
            print(f"❌ Error preparing for full sync: {e}")
            return False
    
    def process_all_accounts(self, sync_mode: str = 'delta', sync_job_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process all configured Exchange accounts with streaming document submission"""
        self.execution_start_time = datetime.now(timezone.utc)
        
        if not self.config.primary_smtp_addresses:
            print("❌ No email addresses configured")
            return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_ids': []}
        
        print(f"🔄 Starting {sync_mode.upper()} SYNC for {len(self.config.primary_smtp_addresses)} account(s)")
        print(f"📦 Using batch size of {self.config.document_batch_size} documents per submission")
        
        # For full sync, prepare by clearing DynamoDB tracking
        if sync_mode == 'full':
            if not self.prepare_full_sync(sync_job_id):
                print("❌ Failed to prepare for full sync")
                return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_ids': []}
        
        # Use provided sync job ID or prepare to start one when needed
        if sync_job_id:
            print(f"📋 Using existing Q Business sync job: {sync_job_id}")
            self.qbusiness_client.current_sync_job_id = sync_job_id
            self.qbusiness_client.sync_job_started = True
        else:
            print("📋 Q Business sync job will be started once at the beginning and reused throughout the process...")
            
            # Check for existing running sync jobs early to avoid conflicts later
            if self.qbusiness_client.has_running_sync_jobs():
                if self.config.auto_resolve_sync_conflicts:
                    print("⚠️  Detected existing running sync jobs. Auto-resolve is enabled - will stop existing jobs when sync job is needed.")
                else:
                    print("⚠️  Detected existing running sync jobs. Auto-resolve is disabled.")
                    print("❌ Cannot start new sync job while another is running. Set AUTO_RESOLVE_SYNC_CONFLICTS=true to automatically stop existing jobs.")
                    return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_ids': []}
        
        try:
            success_count = 0
            total_processed = 0
            total_failed = 0
            
            for smtp_address in self.config.primary_smtp_addresses:
                if self.is_timeout_approaching():
                    print("Lambda timeout approaching. Stopping account processing...")
                    break
                
                success, account_stats = self.process_single_account(smtp_address, sync_mode, sync_job_id)
                if success:
                    success_count += 1
                    total_processed += account_stats.get('processed_count', 0)
                    total_failed += account_stats.get('failed_count', 0)
            
            print(f"\n📊 Processing Summary ({sync_mode.upper()} SYNC):")
            print(f"  Accounts processed: {success_count}/{len(self.config.primary_smtp_addresses)}")
            print(f"  Emails attempted: {self.emails_attempted_count}")
            print(f"  Documents processed: {total_processed}")
            print(f"  Documents failed: {total_failed}")
            print(f"  Total emails processed: {self.emails_processed_count}")
            
            return success_count > 0, {
                'processed_count': total_processed, 
                'failed_count': total_failed,
                'orphaned_ids': []
            }
        
        except Exception as e:
            print(f"❌ Error processing accounts: {e}")
            return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_ids': []}
    
    def find_orphaned_items_with_sync(self, sync_mode: str = 'delta', sync_job_id: str = None) -> List[str]:
        """Find orphaned items that exist in DynamoDB but not in Exchange"""
        try:
            print(f"🔍 Finding orphaned items ({sync_mode} mode)...")
            
            if sync_mode == 'full':
                print("  Full sync mode - no orphaned item detection needed (all items reprocessed)")
                return []
            
            # Get all processed email IDs from DynamoDB
            processed_ids = self.dynamodb_client.get_all_processed_email_ids()
            print(f"  Found {len(processed_ids)} processed emails in DynamoDB")
            
            if not processed_ids:
                print("  No processed emails found in DynamoDB")
                return []
            
            # Get current email IDs from all Exchange accounts
            current_ids = set()
            for smtp_address in self.config.primary_smtp_addresses:
                if self.is_timeout_approaching():
                    print("  Timeout approaching, stopping orphaned item detection...")
                    break
                
                print(f"  Collecting current email IDs from {smtp_address}...")
                account = self.ews_client.create_exchange_account(smtp_address)
                if account:
                    account_current_ids = self.ews_client.get_all_current_email_ids(account, self.config.process_main_mailbox)
                    current_ids.update(account_current_ids)
                    print(f"    Found {len(account_current_ids)} current emails in {smtp_address}")
            
            print(f"  Total current emails in Exchange: {len(current_ids)}")
            
            # Find orphaned items (in DynamoDB but not in Exchange)
            orphaned_ids = processed_ids - current_ids
            print(f"  Found {len(orphaned_ids)} orphaned items")
            
            if orphaned_ids:
                print(f"  🗑️  Cleaning up {len(orphaned_ids)} orphaned items...")
                orphaned_list = list(orphaned_ids)
                
                # Delete orphaned items in batches
                batch_size = 10
                total_deleted = 0
                
                for i in range(0, len(orphaned_list), batch_size):
                    if self.is_timeout_approaching():
                        print("    Timeout approaching, stopping orphaned cleanup...")
                        break
                    
                    batch = orphaned_list[i:i + batch_size]
                    print(f"    Deleting orphaned batch {i//batch_size + 1}/{(len(orphaned_list) + batch_size - 1)//batch_size}...")
                    
                    if self._delete_documents_and_records(batch, sync_job_id, "orphaned cleanup"):
                        total_deleted += len(batch)
                
                print(f"  ✅ Cleaned up {total_deleted}/{len(orphaned_ids)} orphaned items")
                return orphaned_list[:total_deleted]  # Return only successfully deleted items
            else:
                print("  ✅ No orphaned items found")
                return []
                
        except Exception as e:
            print(f"❌ Error finding orphaned items: {e}")
            return []