"""
Email Processor Module
Main email processor that coordinates the workflow between all components
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
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
        
        # Account-specific statistics tracking
        self.account_stats = {}
        
        # Thread-safe locks for counters
        self._counter_lock = Lock()
        self._batch_lock = Lock()
    
    def get_account_processing_stats(self) -> Dict[str, Dict[str, int]]:
        """Get processing statistics by account"""
        return self.account_stats.copy()
    
    def _increment_attempted_count(self, count: int = 1):
        """Thread-safe increment of attempted emails count"""
        with self._counter_lock:
            self.emails_attempted_count += count
    
    def _increment_processed_count(self, count: int = 1):
        """Thread-safe increment of processed emails count"""
        with self._counter_lock:
            self.emails_processed_count += count
    
    def _update_account_stats(self, account_email: str, status: str):
        """Update statistics for an account"""
        if account_email not in self.account_stats:
            self.account_stats[account_email] = {'processed': 0, 'failed': 0, 'total': 0}
        
        self.account_stats[account_email][status] = self.account_stats[account_email].get(status, 0) + 1
        self.account_stats[account_email]['total'] += 1
    

    
    def _delete_documents_and_records(self, email_ids: list, sync_job_id: str = None, reason: str = "deletion", account_email: str = None, folder_name: str = None) -> bool:
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
                if account_email and folder_name:
                    # Use the proper delete method with composite key
                    if self.dynamodb_client.delete_email_record(email_id, account_email, folder_name):
                        dynamodb_deleted_count += 1
                else:
                    # Fallback: try to delete by scanning for the email_id (less efficient)
                    if self._delete_email_record_by_scan(email_id):
                        dynamodb_deleted_count += 1
            
            print(f"✅ Deleted {len(email_ids)} documents from Q Business and {dynamodb_deleted_count} records from DynamoDB ({reason})")
            return qbusiness_success and dynamodb_deleted_count == len(email_ids)
            
        except Exception as e:
            print(f"❌ Error deleting documents and records ({reason}): {e}")
            return False
    
    def _delete_email_record_by_scan(self, email_id: str) -> bool:
        """Fallback method to delete email record by scanning for it (less efficient)"""
        try:
            # We need to scan the table to find the keys
            response = self.dynamodb_client.table.scan(
                FilterExpression='contains(folder_email_key, :email_id)',
                ExpressionAttributeValues={':email_id': f"#{email_id}"}
            )
            
            for item in response['Items']:
                folder_email_key = item.get('folder_email_key', '')
                account_email = item.get('account_email', '')
                if folder_email_key.endswith(f"#{email_id}") and account_email:
                    # Found the record, delete it
                    self.dynamodb_client.table.delete_item(Key={
                        'account_email': account_email,
                        'folder_email_key': folder_email_key
                    })
                    return True
                    
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.dynamodb_client.table.scan(
                    FilterExpression='contains(folder_email_key, :email_id)',
                    ExpressionAttributeValues={':email_id': f"#{email_id}"},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                for item in response['Items']:
                    folder_email_key = item.get('folder_email_key', '')
                    account_email = item.get('account_email', '')
                    if folder_email_key.endswith(f"#{email_id}") and account_email:
                        # Found the record, delete it
                        self.dynamodb_client.table.delete_item(Key={
                            'account_email': account_email,
                            'folder_email_key': folder_email_key
                        })
                        return True
            
            return False
        except Exception as e:
            print(f"❌ Error in fallback delete for email {email_id}: {e}")
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
            
            # Update account statistics
            self._update_account_stats(email_info['account_email'], final_status)
            
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
                
                # Update account statistics
                self._update_account_stats(email_info['account_email'], final_status)
                
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
            processed_ids = None
            if sync_mode == 'delta':
                processed_ids = self.dynamodb_client.get_processed_email_ids_for_folder(folder_name, account_email)
                print(f"  Found {len(processed_ids)} already processed emails in folder {folder_name}")
            else:
                processed_ids = set()
                print(f"  Full sync mode - processing all emails in folder {folder_name}")
            
            # Get all emails in folder
            items = folder.all().only('id')
            
            # Find and clean up orphaned items for this folder
            orphaned_count = self._find_and_cleanup_folder_orphans(folder_name, account_email, sync_mode, sync_job_id, processed_ids, items)
            
            # Choose processing method based on configuration
            if self.config.enable_threading:
                processed_count, failed_count = self._process_emails_threaded(items, folder_name, folder, account_email, sync_mode, sync_job_id, processed_ids)
            else:
                processed_count, failed_count = self._process_emails_sequential(items, folder_name, folder, account_email, sync_mode, sync_job_id, processed_ids)
            
            print(f"  ✅ Folder {folder_name} completed: {processed_count} processed, {failed_count} failed, {orphaned_count} orphaned cleaned")
            return True, {'processed_count': processed_count, 'failed_count': failed_count, 'orphaned_count': orphaned_count}
            
        except Exception as e:
            print(f"Error processing folder {folder_name}: {e}")
            return False, {'processed_count': 0, 'failed_count': 0}
    
    def _process_emails_sequential(self, items, folder_name: str, folder, account_email: str, sync_mode: str, sync_job_id: str, processed_ids: set) -> Tuple[int, int]:
        """Process emails sequentially (original method)"""
        documents_batch = []
        emails_to_mark_batch = []
        documents_to_delete_batch = []
        
        processed_count = 0
        failed_count = 0
        
        for item in items:
            # Check processing limit for testing
            if self.config.testing_email_limit is not None and self.emails_attempted_count >= self.config.testing_email_limit:
                print(f"Reached processing limit of {self.config.testing_email_limit} emails")
                break
            
            self._increment_attempted_count()
            
            # Check if email needs processing (delta mode only)
            email_id_str = str(item.id)
            needs_processing = True
            
            if sync_mode == 'delta':
                if email_id_str in processed_ids:
                    # Email already processed, skip it
                    needs_processing = False
            
            if not needs_processing:
                continue
            
            try:
                # Create Q Business document with ACL for the account owner
                document = self.document_processor.create_qbusiness_document(item, folder_name, folder, account_email)
                if document:
                    documents_batch.append(document)
                    processed_count += 1
                else:
                    failed_count += 1
                
                # Store email info for marking as processed later
                status = 'processed' if document else 'failed'
                current_time = datetime.now(timezone.utc)
                emails_to_mark_batch.append({
                    'email_id': str(item.id),
                    'folder_name': folder_name,
                    'datetime_created': current_time,
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
                current_time = datetime.now(timezone.utc)
                emails_to_mark_batch.append({
                    'email_id': str(item.id),
                    'folder_name': folder_name,
                    'datetime_created': current_time,
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
        
        return processed_count, failed_count
    
    def _process_emails_threaded(self, items, folder_name: str, folder, account_email: str, sync_mode: str, sync_job_id: str, processed_ids: set) -> Tuple[int, int]:
        """Process emails in parallel using ThreadPoolExecutor"""
        print(f"  🧵 Using threaded processing with {self.config.max_worker_threads} workers, batch size {self.config.thread_batch_size}")
        
        # Convert items to list and filter for processing
        items_to_process = []
        for item in items:
            # Check processing limit for testing
            if self.config.testing_email_limit is not None and self.emails_attempted_count >= self.config.testing_email_limit:
                print(f"Reached processing limit of {self.config.testing_email_limit} emails")
                break
            
            self._increment_attempted_count()
            
            # Check if email needs processing (delta mode only)
            email_id_str = str(item.id)
            needs_processing = True
            
            if sync_mode == 'delta':
                if email_id_str in processed_ids:
                    # Email already processed, skip it
                    needs_processing = False
            
            if needs_processing:
                items_to_process.append(item)
        
        if not items_to_process:
            return 0, 0
        
        print(f"  📧 Processing {len(items_to_process)} emails in parallel...")
        
        # Split items into batches for threading
        batches = []
        for i in range(0, len(items_to_process), self.config.thread_batch_size):
            batch = items_to_process[i:i + self.config.thread_batch_size]
            batches.append(batch)
        
        print(f"  📦 Created {len(batches)} batches for parallel processing")
        
        total_processed = 0
        total_failed = 0
        
        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=self.config.max_worker_threads) as executor:
            # Submit all batch processing tasks
            future_to_batch = {
                executor.submit(self._process_email_batch, batch, folder_name, folder, account_email, sync_job_id): batch
                for batch in batches
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    batch_processed, batch_failed = future.result()
                    total_processed += batch_processed
                    total_failed += batch_failed
                    print(f"    ✅ Batch completed: {batch_processed} processed, {batch_failed} failed")
                except Exception as e:
                    print(f"    ❌ Batch failed: {e}")
                    total_failed += len(batch)
        
        return total_processed, total_failed
    
    def _process_email_batch(self, email_batch: List, folder_name: str, folder, account_email: str, sync_job_id: str) -> Tuple[int, int]:
        """Process a batch of emails (used by threading)"""
        documents_batch = []
        emails_to_mark_batch = []
        documents_to_delete_batch = []
        
        processed_count = 0
        failed_count = 0
        
        for item in email_batch:
            try:
                # Create Q Business document with ACL for the account owner
                document = self.document_processor.create_qbusiness_document(item, folder_name, folder, account_email)
                if document:
                    documents_batch.append(document)
                    processed_count += 1
                    self._increment_processed_count()
                else:
                    failed_count += 1
                
                # Store email info for marking as processed later
                status = 'processed' if document else 'failed'
                current_time = datetime.now(timezone.utc)
                emails_to_mark_batch.append({
                    'email_id': str(item.id),
                    'folder_name': folder_name,
                    'datetime_created': current_time,
                    'status': status,
                    'account_email': account_email
                })
                
            except Exception as e:
                print(f"Error processing email {item.id}: {e}")
                failed_count += 1
                current_time = datetime.now(timezone.utc)
                emails_to_mark_batch.append({
                    'email_id': str(item.id),
                    'folder_name': folder_name,
                    'datetime_created': current_time,
                    'status': 'failed',
                    'account_email': account_email
                })
        
        # Submit the batch (thread-safe)
        if documents_batch or documents_to_delete_batch or emails_to_mark_batch:
            try:
                with self._batch_lock:  # Ensure thread-safe batch submission
                    self._submit_document_batch(documents_batch, documents_to_delete_batch, emails_to_mark_batch, sync_job_id)
            except Exception as batch_e:
                print(f"❌ Critical error in threaded batch submission: {batch_e}")
                # Even if batch submission fails completely, try to mark emails as failed
                with self._batch_lock:
                    self._mark_emails_individually(emails_to_mark_batch, set(), True)
        
        return processed_count, failed_count
    
    def process_account_folders(self, account, account_email: str, folder_root, root_name: str, sync_mode: str = 'delta', sync_job_id: str = None) -> Tuple[bool, Dict[str, int]]:
        """Process all folders in an account (main or archive) with streaming submission"""
        try:
            print(f"Processing {root_name} folders for {account_email} - {sync_mode.upper()} mode...")
            
            total_processed = 0
            total_failed = 0
            total_orphaned = 0
            
            def process_folder_recursive(folder, parent_path=""):
                nonlocal total_processed, total_failed, total_orphaned
                

                
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
                total_orphaned += folder_stats.get('orphaned_count', 0)
                
                # Process child folders
                if hasattr(folder, 'children') and folder.children:
                    for child_folder in folder.children:
                        if not process_folder_recursive(child_folder, folder_path):
                            return False
                
                return True
            
            success = process_folder_recursive(folder_root)
            return success, {'processed_count': total_processed, 'failed_count': total_failed, 'orphaned_count': total_orphaned}
            
        except Exception as e:
            print(f"Error processing {root_name} folders for {account_email}: {e}")
            return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0}
    
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
            total_orphaned = 0
            
            # Process main mailbox folders (if enabled)
            if self.config.process_main_mailbox and hasattr(account, 'msg_folder_root'):
                print(f"📁 Processing main mailbox folders for {smtp_address}")
                success, main_stats = self.process_account_folders(account, smtp_address, account.msg_folder_root, "main mailbox", sync_mode, sync_job_id)
                if not success:
                    return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0}
                total_processed += main_stats.get('processed_count', 0)
                total_failed += main_stats.get('failed_count', 0)
                total_orphaned += main_stats.get('orphaned_count', 0)
            elif not self.config.process_main_mailbox:
                print(f"⏭️  Skipping main mailbox folders for {smtp_address} (PROCESS_MAIN_MAILBOX=false)")
            
            # Process archive folders if available
            if hasattr(account, 'archive_msg_folder_root') and account.archive_msg_folder_root:
                success, archive_stats = self.process_account_folders(account, smtp_address, account.archive_msg_folder_root, "archive", sync_mode, sync_job_id)
                if not success:
                    return False, {'processed_count': total_processed, 'failed_count': total_failed, 'orphaned_count': total_orphaned}
                total_processed += archive_stats.get('processed_count', 0)
                total_failed += archive_stats.get('failed_count', 0)
                total_orphaned += archive_stats.get('orphaned_count', 0)
            
            # Clean up orphaned folders for delta sync
            if sync_mode == 'delta':
                orphaned_folder_count = self._cleanup_orphaned_folders(account, smtp_address, sync_job_id)
                total_orphaned += orphaned_folder_count
            
            print(f"✅ Completed processing account: {smtp_address} - {total_processed} processed, {total_failed} failed, {total_orphaned} orphaned cleaned")
            return True, {'processed_count': total_processed, 'failed_count': total_failed, 'orphaned_count': total_orphaned}
            
        except Exception as e:
            print(f"❌ Error processing account {smtp_address}: {e}")
            return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0} 
   
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
            return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0}
        
        print(f"🔄 Starting {sync_mode.upper()} SYNC for {len(self.config.primary_smtp_addresses)} account(s)")
        print(f"📦 Using batch size of {self.config.document_batch_size} documents per submission")
        
        # For full sync, prepare by clearing DynamoDB tracking
        if sync_mode == 'full':
            if not self.prepare_full_sync(sync_job_id):
                print("❌ Failed to prepare for full sync")
                return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0}
        
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
                    return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0}
        
        try:
            success_count = 0
            total_processed = 0
            total_failed = 0
            total_orphaned = 0
            
            for smtp_address in self.config.primary_smtp_addresses:
                success, account_stats = self.process_single_account(smtp_address, sync_mode, sync_job_id)
                if success:
                    success_count += 1
                    total_processed += account_stats.get('processed_count', 0)
                    total_failed += account_stats.get('failed_count', 0)
                    total_orphaned += account_stats.get('orphaned_count', 0)
            
            print(f"\n📊 Processing Summary ({sync_mode.upper()} SYNC):")
            print(f"  Accounts processed: {success_count}/{len(self.config.primary_smtp_addresses)}")
            print(f"  Emails attempted: {self.emails_attempted_count}")
            print(f"  Documents processed: {total_processed}")
            print(f"  Documents failed: {total_failed}")
            print(f"  Orphaned items cleaned: {total_orphaned}")
            print(f"  Total emails processed: {self.emails_processed_count}")
            
            return success_count > 0, {
                'processed_count': total_processed, 
                'failed_count': total_failed,
                'orphaned_count': total_orphaned
            }
        
        except Exception as e:
            print(f"❌ Error processing accounts: {e}")
            return False, {'processed_count': 0, 'failed_count': 0, 'orphaned_count': 0}

    def _find_and_cleanup_folder_orphans(self, folder_name: str, account_email: str, sync_mode: str, sync_job_id: str = None, processed_ids: set = None, items = None) -> int:
        """Find and cleanup orphaned items for a specific folder during processing"""
        try:
            # Skip orphaned detection for full sync mode
            if sync_mode == 'full':
                return 0
            
            # Validate required inputs
            if processed_ids is None or items is None:
                print(f"  ⚠️  Missing required data for orphaned detection in folder {folder_name}")
                print(f"  ⏭️  Skipping orphaned cleanup for folder {folder_name} for safety")
                return 0
            
            # Get current email IDs from the provided items - with error handling
            current_ids = set()
            try:
                for item in items:
                    current_ids.add(str(item.id))
            except Exception as e:
                print(f"  ⚠️  Error processing Exchange items for folder {folder_name}: {e}")
                print(f"  ⏭️  Skipping orphaned cleanup for folder {folder_name} due to Exchange error")
                return 0
            
            # Find orphaned items (in DynamoDB but not in current folder)
            orphaned_ids = processed_ids - current_ids
            
            if orphaned_ids:
                print(f"  🗑️  Found {len(orphaned_ids)} orphaned items in folder {folder_name}")
                
                # Delete orphaned items in batches
                batch_size = 10
                total_deleted = 0
                orphaned_list = list(orphaned_ids)
                
                for i in range(0, len(orphaned_list), batch_size):
                    batch = orphaned_list[i:i + batch_size]
                    print(f"    Deleting orphaned batch {i//batch_size + 1}/{(len(orphaned_list) + batch_size - 1)//batch_size} from {folder_name}...")
                    
                    if self._delete_documents_and_records(batch, sync_job_id, f"orphaned cleanup in {folder_name}", account_email, folder_name):
                        total_deleted += len(batch)
                
                print(f"  ✅ Cleaned up {total_deleted}/{len(orphaned_ids)} orphaned items from folder {folder_name}")
                return total_deleted
            else:
                print(f"  ✅ No orphaned items found in folder {folder_name}")
                return 0
                
        except Exception as e:
            print(f"❌ Unexpected error during orphaned cleanup in folder {folder_name}: {e}")
            print(f"  ⏭️  Skipping orphaned cleanup for folder {folder_name} for safety")
            return 0
    
    def _cleanup_orphaned_folders(self, account, account_email: str, sync_job_id: str = None) -> int:
        """Clean up orphaned folders that exist in DynamoDB but not in Exchange"""
        try:
            print(f"\n🗂️  Checking for orphaned folders in account {account_email}...")
            
            # Get all current folder names from Exchange
            current_folders = set()
            exchange_error = False
            
            try:
                # Collect folders from main mailbox if enabled
                if self.config.process_main_mailbox and hasattr(account, 'msg_folder_root'):
                    print("  📁 Collecting current folder names from main mailbox...")
                    self._collect_folder_names(account.msg_folder_root, current_folders)
                
                # Collect folders from archive if available
                if hasattr(account, 'archive_msg_folder_root') and account.archive_msg_folder_root:
                    print("  📁 Collecting current folder names from archive...")
                    self._collect_folder_names(account.archive_msg_folder_root, current_folders)
                    
                print(f"  📊 Found {len(current_folders)} current folders in Exchange")
                
            except Exception as e:
                print(f"  ⚠️  Error collecting current folder names from Exchange: {e}")
                print(f"  ⏭️  Skipping orphaned folder cleanup for safety")
                exchange_error = True
            
            if exchange_error:
                return 0
            
            # Get all processed folder names from DynamoDB
            processed_folders = set()
            dynamodb_error = False
            
            try:
                print("  🗄️  Collecting processed folder names from DynamoDB...")
                processed_emails = self.dynamodb_client.get_processed_emails_by_account(account_email)
                
                for email_record in processed_emails:
                    folder_email_key = email_record.get('folder_email_key', '')
                    if folder_email_key:
                        folder_name = self.dynamodb_client._extract_folder_from_folder_email_key(folder_email_key)
                        if folder_name:
                            processed_folders.add(folder_name)
                
                print(f"  📊 Found {len(processed_folders)} processed folders in DynamoDB")
                
            except Exception as e:
                print(f"  ⚠️  Error collecting processed folder names from DynamoDB: {e}")
                print(f"  ⏭️  Skipping orphaned folder cleanup for safety")
                dynamodb_error = True
            
            if dynamodb_error:
                return 0
            
            # Find orphaned folders (in DynamoDB but not in Exchange)
            orphaned_folders = processed_folders - current_folders
            
            if not orphaned_folders:
                print("  ✅ No orphaned folders found")
                return 0
            
            print(f"  🗑️  Found {len(orphaned_folders)} orphaned folders: {', '.join(sorted(orphaned_folders))}")
            
            # Clean up orphaned folders one by one
            total_orphaned_items = 0
            
            for folder_name in sorted(orphaned_folders):
                print(f"    🗂️  Cleaning up orphaned folder: {folder_name}")
                
                try:
                    # Get all processed email IDs for this orphaned folder
                    orphaned_folder_emails = self.dynamodb_client.get_processed_email_ids_for_folder(folder_name, account_email)
                    
                    if orphaned_folder_emails:
                        print(f"      📧 Found {len(orphaned_folder_emails)} orphaned emails in folder {folder_name}")
                        
                        # Delete orphaned emails in batches
                        batch_size = 10
                        orphaned_list = list(orphaned_folder_emails)
                        
                        for i in range(0, len(orphaned_list), batch_size):
                            batch = orphaned_list[i:i + batch_size]
                            print(f"      🗑️  Deleting batch {i//batch_size + 1}/{(len(orphaned_list) + batch_size - 1)//batch_size} from orphaned folder {folder_name}...")
                            
                            if self._delete_documents_and_records(batch, sync_job_id, f"orphaned folder cleanup: {folder_name}", account_email, folder_name):
                                total_orphaned_items += len(batch)
                        
                        print(f"      ✅ Cleaned up {len(orphaned_folder_emails)} orphaned emails from folder {folder_name}")
                    else:
                        print(f"      ℹ️  No orphaned emails found in folder {folder_name}")
                        
                except Exception as e:
                    print(f"      ❌ Error cleaning up orphaned folder {folder_name}: {e}")
                    continue
            
            print(f"  ✅ Orphaned folder cleanup completed: {total_orphaned_items} total items cleaned from {len(orphaned_folders)} folders")
            return total_orphaned_items
            
        except Exception as e:
            print(f"❌ Unexpected error during orphaned folder cleanup for account {account_email}: {e}")
            print(f"  ⏭️  Skipping orphaned folder cleanup for safety")
            return 0
    
    def _collect_folder_names(self, folder_root, folder_names: set, parent_path: str = "") -> None:
        """Recursively collect folder names from Exchange folder structure"""
        try:
            def collect_recursive(folder, parent_path=""):
                folder_path = f"{parent_path}/{folder.name}" if parent_path else folder.name
                
                # Strip "Top of Information Store/" prefix from folder display name
                if folder_path.startswith("Top of Information Store/"):
                    folder_path = folder_path[len("Top of Information Store/"):]
                elif folder_path == "Top of Information Store":
                    folder_path = "Root"
                
                # Strip "Root/" prefix from folder display name
                if folder_path.startswith("Root/"):
                    folder_path = folder_path[len("Root/"):]
                elif folder_path == "Root":
                    folder_path = ""
                
                # Skip processing if folder_path is empty (root folder case)
                if not folder_path:
                    folder_path = "Root"
                
                # Skip system folders that we don't process
                if not self.ews_client.should_skip_folder(folder, folder_path):
                    folder_names.add(folder_path)
                
                # Process child folders
                if hasattr(folder, 'children') and folder.children:
                    for child_folder in folder.children:
                        collect_recursive(child_folder, folder_path)
            
            collect_recursive(folder_root)
            
        except Exception as e:
            print(f"    ⚠️  Error collecting folder names: {e}")
            raise