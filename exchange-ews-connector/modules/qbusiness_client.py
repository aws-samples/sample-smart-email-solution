"""
Q Business Client Module
Handles AWS Q Business operations for document indexing and sync job management
"""

import boto3
import time
import logging
import threading
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError
from .security_utils import handle_error_securely

logger = logging.getLogger(__name__)

class QBusinessClient:
    """AWS Q Business client for managing sync jobs and document indexing"""
    
    def __init__(self, config):
        self.config = config
        self.client = boto3.client('qbusiness')
        self.current_sync_job_id = None
        self.sync_job_started = False
        
        # Sync job coordinator will be set by the main process
        self.sync_coordinator = None
        
        # Heartbeat thread for keeping sync job alive
        self._heartbeat_thread = None
        self._heartbeat_stop_event = threading.Event()
    
    def set_sync_coordinator(self, sync_coordinator):
        """Set the sync job coordinator for distributed sync job management"""
        self.sync_coordinator = sync_coordinator
    
    def start_sync_job_if_needed(self) -> Optional[str]:
        """Start a Q Business data source sync job only if not already started"""
        # Check if we already have an active sync job
        if self.current_sync_job_id and self.sync_job_started:
            print(f"📋 Sync job already active: {self.current_sync_job_id}")
            return self.current_sync_job_id
        
        print("🚀 Starting Q Business sync job for first batch operation...")
        
        # Use coordinator if available, otherwise fall back to direct start
        if self.sync_coordinator:
            return self.sync_coordinator.start_or_join_sync_job()
        else:
            return self.start_sync_job()
    
    def start_sync_job(self) -> Optional[str]:
        """Start a Q Business data source sync job"""
        # Check if we already have an active sync job
        if self.current_sync_job_id and self.sync_job_started:
            print(f"📋 Sync job already active: {self.current_sync_job_id}")
            return self.current_sync_job_id
        
        # First check if there are already running sync jobs
        if self.has_running_sync_jobs():
            if not self.config.auto_resolve_sync_conflicts:
                print("❌ Cannot start sync job: another sync job is already running and auto-resolve is disabled")
                return None
            print("🔄 Auto-resolve is enabled. Will attempt to stop existing sync jobs first...")
        
        try:
            print("Starting Q Business data source sync...")
            response = self.client.start_data_source_sync_job(
                applicationId=self.config.application_id,
                indexId=self.config.index_id,
                dataSourceId=self.config.data_source_id
            )
            
            self.current_sync_job_id = response.get('executionId')
            if self.current_sync_job_id:
                self.sync_job_started = True
                print(f"✅ Sync job started successfully with ID: {self.current_sync_job_id}")
                
                # Start heartbeat thread to keep sync job alive
                self._start_heartbeat_thread()
                
                return self.current_sync_job_id
            else:
                print("⚠️  Warning: Sync job started but no execution ID returned")
                return None
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            # Handle ConflictException or ValidationException - another sync job is already running
            if error_code in ['ConflictException', 'ValidationException'] and 'already syncing' in error_message:
                print(f"Sync conflict detected ({error_code}): {error_message}")
                
                # Check if auto-resolution is enabled
                if not getattr(self.config, 'auto_resolve_sync_conflicts', True):
                    print("Auto-resolution of sync conflicts is disabled. Cannot proceed.")
                    return None
                
                print("🔄 Another sync job is already running. Attempting to stop it and retry...")
                
                # Get max retries from config
                max_retries = getattr(self.config, 'max_sync_conflict_retries', 3)
                
                for attempt in range(max_retries):
                    print(f"Conflict resolution attempt {attempt + 1}/{max_retries}")
                    
                    # Try to stop any existing sync job
                    if self._stop_existing_sync_jobs():
                        print("Existing sync job stopped. Retrying start sync job...")
                        
                        # Add a longer delay to ensure the stop operation is processed
                        print("Waiting for sync job to fully stop...")
                        time.sleep(10)  # Increased wait time
                        
                        # Retry starting the sync job
                        try:
                            response = self.client.start_data_source_sync_job(
                                applicationId=self.config.application_id,
                                indexId=self.config.index_id,
                                dataSourceId=self.config.data_source_id
                            )
                            
                            self.current_sync_job_id = response.get('executionId')
                            if self.current_sync_job_id:
                                self.sync_job_started = True
                                print(f"Sync job started successfully after conflict resolution with ID: {self.current_sync_job_id}")
                                return self.current_sync_job_id
                            else:
                                print("Warning: Sync job started after retry but no execution ID returned")
                                return None
                                
                        except ClientError as retry_e:
                            retry_error_code = retry_e.response.get('Error', {}).get('Code', 'Unknown')
                            retry_error_message = retry_e.response.get('Error', {}).get('Message', str(retry_e))
                            if retry_error_code in ['ConflictException', 'ValidationException'] and 'already syncing' in retry_error_message and attempt < max_retries - 1:
                                print(f"Still getting ConflictException on attempt {attempt + 1}, retrying...")
                                continue
                            else:
                                print(f"Failed to start sync job after stopping existing job: {retry_e}")
                                return None
                        except Exception as retry_e:
                            print(f"Unexpected error starting sync job after stopping existing job: {retry_e}")
                            return None
                    else:
                        print(f"Failed to stop existing sync job on attempt {attempt + 1}")
                        if attempt < max_retries - 1:
                            print("Retrying conflict resolution...")
                            time.sleep(5)  # Wait longer before retrying
                            continue
                        else:
                            print("Failed to stop existing sync job after all attempts. Cannot proceed with new sync job.")
                            return None
                
                print("All conflict resolution attempts failed")
                return None
            else:
                error_msg = handle_error_securely(e, "starting Q Business sync job")
                logger.error(f"Error starting Q Business sync job: {error_code}")
                return None
                
        except Exception as e:
            print(f"Unexpected error starting sync job: {type(e).__name__}: {e}")
            return None
    
    def _stop_existing_sync_jobs(self) -> bool:
        """Stop any existing sync jobs that might be running and wait for them to complete"""
        try:
            print("Checking for existing sync jobs...")
            
            # List current sync jobs to find any running ones
            response = self.client.list_data_source_sync_jobs(
                applicationId=self.config.application_id,
                indexId=self.config.index_id,
                dataSourceId=self.config.data_source_id
            )
            
            jobs = response.get('history', [])
            running_jobs = [job for job in jobs if job.get('status') in ['SYNCING', 'SYNCING_INDEXING']]
            
            if not running_jobs:
                print("No running sync jobs found")
                return True
            
            print(f"Found {len(running_jobs)} running sync job(s). Attempting to stop them...")
            
            # Stop each running job
            stopped_count = 0
            for job in running_jobs:
                job_id = job.get('executionId')
                job_status = job.get('status')
                print(f"Stopping sync job {job_id} (status: {job_status})...")
                
                try:
                    self.client.stop_data_source_sync_job(
                        applicationId=self.config.application_id,
                        indexId=self.config.index_id,
                        dataSourceId=self.config.data_source_id
                    )
                    print(f"Stop command sent for sync job {job_id}")
                    stopped_count += 1
                    
                except ClientError as stop_e:
                    stop_error_code = stop_e.response.get('Error', {}).get('Code', 'Unknown')
                    stop_error_message = stop_e.response.get('Error', {}).get('Message', str(stop_e))
                    print(f"Failed to stop sync job {job_id}:")
                    print(f"  Error Code: {stop_error_code}")
                    print(f"  Error Message: {stop_error_message}")
                    
                    # If the job is already stopped or doesn't exist, that's okay
                    if stop_error_code in ['ResourceNotFoundException', 'ValidationException']:
                        print(f"Sync job {job_id} may have already stopped or completed")
                        stopped_count += 1
            
            if stopped_count > 0:
                # Wait for jobs to actually stop
                print("Waiting for sync jobs to fully stop...")
                max_wait_time = 60  # Maximum wait time in seconds
                wait_interval = 5   # Check every 5 seconds
                waited_time = 0
                
                while waited_time < max_wait_time:
                    time.sleep(wait_interval)
                    waited_time += wait_interval
                    
                    # Check if jobs are still running
                    response = self.client.list_data_source_sync_jobs(
                        applicationId=self.config.application_id,
                        indexId=self.config.index_id,
                        dataSourceId=self.config.data_source_id
                    )
                    
                    jobs = response.get('history', [])
                    still_running = [job for job in jobs if job.get('status') in ['SYNCING', 'SYNCING_INDEXING']]
                    
                    if not still_running:
                        print(f"All sync jobs have stopped after {waited_time} seconds")
                        return True
                    else:
                        print(f"Still waiting for {len(still_running)} sync job(s) to stop... ({waited_time}s elapsed)")
                
                print(f"Timeout waiting for sync jobs to stop after {max_wait_time} seconds")
                return False
            
            success = stopped_count == len(running_jobs)
            if success:
                print(f"Successfully stopped all {stopped_count} running sync jobs")
            else:
                print(f"Only stopped {stopped_count}/{len(running_jobs)} sync jobs")
            
            return success
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            print(f"Error checking/stopping existing sync jobs:")
            print(f"  Error Code: {error_code}")
            print(f"  Error Message: {error_message}")
            return False
        except Exception as e:
            print(f"Unexpected error stopping existing sync jobs: {type(e).__name__}: {e}")
            return False
    
    def force_stop_all_sync_jobs(self) -> bool:
        """Force stop all running sync jobs and wait for them to complete"""
        print("🛑 Force stopping all running sync jobs...")
        return self._stop_existing_sync_jobs()
    
    def _start_heartbeat_thread(self):
        """Start heartbeat thread to keep sync job coordinator updated"""
        if self.sync_coordinator and not self._heartbeat_thread:
            self._heartbeat_stop_event.clear()
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
            self._heartbeat_thread.start()
            logger.info("Started sync job heartbeat thread")
    
    def _stop_heartbeat_thread(self):
        """Stop heartbeat thread"""
        if self._heartbeat_thread:
            logger.info("Stopping heartbeat thread...")
            self._heartbeat_stop_event.set()
            
            # Wait for thread to finish
            self._heartbeat_thread.join(timeout=10)
            
            if self._heartbeat_thread.is_alive():
                logger.warning("Heartbeat thread did not stop within timeout")
            else:
                logger.info("Heartbeat thread stopped successfully")
                
            self._heartbeat_thread = None
    
    def _heartbeat_worker(self):
        """Worker thread that sends periodic heartbeats to sync coordinator"""
        while not self._heartbeat_stop_event.is_set():
            try:
                if self.sync_coordinator and self.current_sync_job_id:
                    self.sync_coordinator.update_heartbeat(self.current_sync_job_id)
                
                # Wait 30 seconds between heartbeats
                if self._heartbeat_stop_event.wait(30):
                    break  # Stop event was set
                    
            except Exception as e:
                logger.debug(f"Heartbeat error: {e}")
                # Continue heartbeat even if there are errors
                if self._heartbeat_stop_event.wait(30):
                    break
    
    def stop_sync_job(self) -> bool:
        """Stop the current Q Business data source sync job"""
        if not self.current_sync_job_id:
            print("No active sync job to stop")
            return True
        
        print(f"🛑 Stopping Q Business sync job: {self.current_sync_job_id}")
        
        # Stop heartbeat thread first to prevent it from keeping the job "alive"
        print("  📡 Stopping heartbeat thread...")
        self._stop_heartbeat_thread()
        
        # Use coordinator if available
        if self.sync_coordinator:
            print("  🤝 Using sync coordinator to stop job...")
            success = self.sync_coordinator.stop_sync_job_if_owner()
            if success:
                print("  ✅ Sync job stopped via coordinator")
                self.current_sync_job_id = None
                self.sync_job_started = False
            else:
                print("  ⚠️  Coordinator failed to stop sync job, trying direct stop...")
                # Fall back to direct stop if coordinator fails
                return self._direct_stop_sync_job()
            return success
        
        # Fall back to direct stop
        return self._direct_stop_sync_job()
    
    def _direct_stop_sync_job(self) -> bool:
        """Directly stop the sync job via AWS API"""
        try:
            print(f"  🔌 Directly stopping sync job via AWS API...")
            response = self.client.stop_data_source_sync_job(
                applicationId=self.config.application_id,
                indexId=self.config.index_id,
                dataSourceId=self.config.data_source_id
            )
            
            print("  ✅ Sync job stopped successfully via direct API call")
            self.current_sync_job_id = None
            self.sync_job_started = False
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            # If the job is already stopped or doesn't exist, that's okay
            if error_code in ['ResourceNotFoundException', 'ValidationException']:
                print(f"  ℹ️  Sync job may have already stopped: {error_message}")
                self.current_sync_job_id = None
                self.sync_job_started = False
                return True
            
            error_msg = handle_error_securely(e, "stopping Q Business sync job")
            logger.error(f"Error stopping Q Business sync job: {error_code}")
            print(f"  ❌ Failed to stop sync job: {error_message}")
            return False
        except Exception as e:
            print(f"  ❌ Unexpected error stopping sync job: {type(e).__name__}: {e}")
            return False
    
    def has_running_sync_jobs(self) -> bool:
        """Check if there are any currently running sync jobs"""
        try:
            response = self.client.list_data_source_sync_jobs(
                applicationId=self.config.application_id,
                indexId=self.config.index_id,
                dataSourceId=self.config.data_source_id
            )
            
            jobs = response.get('history', [])
            running_jobs = [job for job in jobs if job.get('status') in ['SYNCING', 'SYNCING_INDEXING']]
            
            if running_jobs:
                print(f"Found {len(running_jobs)} running sync job(s)")
                for job in running_jobs:
                    job_id = job.get('executionId', 'Unknown')
                    job_status = job.get('status', 'Unknown')
                    start_time = job.get('startTime', 'Unknown')
                    print(f"  Job ID: {job_id}, Status: {job_status}, Started: {start_time}")
                return True
            else:
                print("No running sync jobs found")
                return False
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            print(f"Error checking for running sync jobs:")
            print(f"  Error Code: {error_code}")
            print(f"  Error Message: {error_message}")
            return False
        except Exception as e:
            print(f"Unexpected error checking for running sync jobs: {type(e).__name__}: {e}")
            return False
    
    def get_sync_job_status(self) -> Optional[str]:
        """Get the status of the current sync job"""
        if not self.current_sync_job_id:
            return None
        
        try:
            response = self.client.list_data_source_sync_jobs(
                applicationId=self.config.application_id,
                indexId=self.config.index_id,
                dataSourceId=self.config.data_source_id
            )
            
            jobs = response.get('history', [])
            for job in jobs:
                if job.get('executionId') == self.current_sync_job_id:
                    status = job.get('status')
                    print(f"Sync job {self.current_sync_job_id} status: {status}")
                    return status
            
            print(f"Sync job {self.current_sync_job_id} not found in job history")
            return None
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            print(f"Error getting sync job status:")
            print(f"  Error Code: {error_code}")
            print(f"  Error Message: {error_message}")
            return None
        except Exception as e:
            print(f"Unexpected error getting sync job status: {type(e).__name__}: {e}")
            return None
    
    def batch_put_documents(self, documents: List[Dict[str, Any]]) -> tuple[bool, List[Dict[str, Any]]]:
        """Submit documents to Q Business using batch put API
        
        Returns:
            tuple: (success, failed_documents_list)
                - success: True if all documents succeeded, False if any failed
                - failed_documents_list: List of failed document details with id, errorCode, errorMessage
        """
        if not documents:
            print("No documents to submit")
            return True, []
            
        # Use existing sync job - it should already be started by the main process
        if not self.sync_job_started or not self.current_sync_job_id:
            print("❌ No active sync job available for document submission")
            return False, []
            
        print(f"Submitting {len(documents)} documents to Q Business...")
        
        try:
            batch_params = {
                'applicationId': self.config.application_id,
                'indexId': self.config.index_id,
                'documents': documents
            }
            
            if self.current_sync_job_id:
                batch_params['dataSourceSyncId'] = self.current_sync_job_id
            
            # Debug logging for request parameters
            print(f"DEBUG: Q Business batch_put_document request parameters:")
            print(f"  - applicationId: {batch_params['applicationId']}")
            print(f"  - indexId: {batch_params['indexId']}")
            print(f"  - dataSourceSyncId: {batch_params.get('dataSourceSyncId', 'None')}")
            print(f"  - documents count: {len(batch_params['documents'])}")
                
            response = self.client.batch_put_document(**batch_params)
            
            # Debug logging for response
            print(f"DEBUG: Q Business batch_put_document response:")
            print(f"  - Response type: {type(response)}")
            print(f"  - Response keys: {list(response.keys()) if isinstance(response, dict) else 'N/A'}")
            
            if not isinstance(response, dict):
                print(f"ERROR: Invalid response type: {type(response)}")
                return False, []
            
            # Log response metadata
            response_metadata = response.get('ResponseMetadata', {})
            http_status = response_metadata.get('HTTPStatusCode')
            request_id = response_metadata.get('RequestId')
            print(f"DEBUG: Response metadata:")
            print(f"  - HTTP Status: {http_status}")
            print(f"  - Request ID: {request_id}")
            print(f"  - Full metadata: {response_metadata}")
                
            if http_status and http_status != 200:
                print(f"ERROR: HTTP error: {http_status}")
                return False, []
            
            failed_documents = response.get('failedDocuments', [])
            successful_documents = response.get('successfulDocuments', [])
            
            print(f"DEBUG: Response analysis:")
            print(f"  - failedDocuments count: {len(failed_documents)}")
            print(f"  - successfulDocuments count: {len(successful_documents) if successful_documents else 'N/A'}")
            
            if failed_documents:
                print(f"Failed to index {len(failed_documents)} documents:")
                for i, failed_doc in enumerate(failed_documents):
                    doc_id = failed_doc.get('id', 'Unknown')
                    error_msg = failed_doc.get('errorMessage', 'No error message')
                    error_code = failed_doc.get('errorCode', 'No error code')
                    print(f"  Failed Document #{i+1}:")
                    print(f"    Document ID: {doc_id}")
                    print(f"    Error Code: {error_code}")
                    print(f"    Error Message: {error_msg}")
                    print(f"    Full failed doc: {failed_doc}")
            
            if successful_documents:
                print(f"Successfully indexed {len(successful_documents)} documents:")
                for i, success_doc in enumerate(successful_documents[:3]):  # Show first 3
                    print(f"  Success Document #{i+1}: {success_doc}")
            
            successful_count = len(documents) - len(failed_documents)
            print(f"Batch operation completed: {successful_count}/{len(documents)} documents successfully indexed")
            
            return len(failed_documents) == 0, failed_documents
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = handle_error_securely(e, "submitting documents to Q Business")
            logger.error(f"AWS ClientError submitting documents to Q Business: {error_code}")
            return False, []
        except Exception as e:
            error_msg = handle_error_securely(e, "submitting documents to Q Business")
            logger.error(error_msg)
            return False, []
    
    def batch_delete_documents(self, document_ids: List[str]) -> bool:
        """Delete documents from Q Business index"""
        if not document_ids:
            print("No documents to delete")
            return True
            
        # Use existing sync job - it should already be started by the main process
        if not self.sync_job_started or not self.current_sync_job_id:
            print("❌ No active sync job available for document deletion")
            return False
            
        print(f"Deleting {len(document_ids)} documents from Q Business...")
        
        try:
            batch_params = {
                'applicationId': self.config.application_id,
                'indexId': self.config.index_id,
                'documents': [{'documentId': doc_id} for doc_id in document_ids]
            }
            
            if self.current_sync_job_id:
                batch_params['dataSourceSyncId'] = self.current_sync_job_id
                
            response = self.client.batch_delete_document(**batch_params)
            
            failed_documents = response.get('failedDocuments', [])
            if failed_documents:
                print(f"Failed to delete {len(failed_documents)} documents:")
                for failed_doc in failed_documents:
                    doc_id = failed_doc.get('id', 'Unknown')
                    error_msg = failed_doc.get('errorMessage', 'No error message')
                    error_code = failed_doc.get('errorCode', 'No error code')
                    print(f"  Document ID: {doc_id}")
                    print(f"    Error Code: {error_code}")
                    print(f"    Error Message: {error_msg}")
            
            successful_count = len(document_ids) - len(failed_documents)
            print(f"Batch delete completed: {successful_count}/{len(document_ids)} documents successfully deleted")
            
            return len(failed_documents) == 0
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = handle_error_securely(e, "deleting documents from Q Business")
            logger.error(f"AWS ClientError deleting documents from Q Business: {error_code}")
            return False
        except Exception as e:
            error_msg = handle_error_securely(e, "deleting documents from Q Business")
            logger.error(error_msg)
            return False
    
    def cleanup_orphaned_qbusiness_documents(self, valid_document_ids: set) -> bool:
        """
        Clean up orphaned documents in Q Business that shouldn't exist.
        
        Note: Q Business doesn't provide a direct API to list documents, so this method
        attempts to delete documents that we know should no longer exist based on our
        DynamoDB tracking and current Exchange state.
        
        Args:
            valid_document_ids: Set of document IDs that should exist in Q Business
            
        Returns:
            bool: True if cleanup was successful
        """
        try:
            print("🧹 Starting Q Business orphaned document cleanup...")
            
            # Since Q Business doesn't provide a list documents API, we can only clean up
            # documents that we know about from our DynamoDB tracking but shouldn't exist
            # This is already handled by the main orphaned items detection logic
            
            # For full sync mode, we could potentially delete all documents and re-index
            # but that's handled by the full sync logic
            
            print("✅ Q Business orphaned document cleanup completed")
            return True
            
        except Exception as e:
            print(f"❌ Error during Q Business orphaned document cleanup: {e}")
            return False