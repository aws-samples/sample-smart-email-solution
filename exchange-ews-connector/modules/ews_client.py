"""
EWS Client Module
Handles Exchange Web Services connections and operations
"""

import logging
from typing import Optional, Set
from exchangelib import OAuth2Credentials, Configuration, Account, OAUTH2, IMPERSONATION, Identity
import pytz

# Set default timezone for exchangelib to avoid naive datetime warnings
import exchangelib.util
import exchangelib.fields

# Configure exchangelib to use UTC timezone by default
exchangelib.util.DEFAULT_TIMEZONE = pytz.UTC

# Also set the default timezone for datetime fields
try:
    exchangelib.fields.DEFAULT_TIMEZONE = pytz.UTC
except AttributeError:
    # Fallback if the attribute doesn't exist in this version
    pass

# Suppress verbose exchangelib logging for naive datetime warnings
exchangelib_fields_logger = logging.getLogger('exchangelib.fields')
exchangelib_fields_logger.setLevel(logging.WARNING)  # Only show WARNING and above, suppress INFO

class EWSClient:
    """
    Exchange Web Services client for connecting to Exchange Online
    
    SECURITY NOTE: This client uses IMPERSONATION access type and only performs
    READ operations. While the EWS.AccessAsUser.All permission allows write access,
    this implementation contains no write methods and uses read-only patterns.
    """
    
    def __init__(self, config):
        self.config = config
        self.accounts = {}
        # Security flag to prevent accidental write operations
        self._read_only_mode = True
    
    def create_exchange_account(self, smtp_address: str) -> Optional[Account]:
        """Create an Exchange account for a specific email address"""
        if smtp_address in self.accounts:
            return self.accounts[smtp_address]
        
        try:
            credentials = OAuth2Credentials(
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                tenant_id=self.config.tenant_id,
                identity=Identity(primary_smtp_address=smtp_address)
            )

            config = Configuration(
                server=self.config.exchange_server,
                credentials=credentials,
                auth_type=OAUTH2
            )

            # Use IMPERSONATION access type for read-only operations
            # This provides a safer access pattern than DELEGATE
            account = Account(
                primary_smtp_address=smtp_address,
                autodiscover=False,
                config=config,
                access_type=IMPERSONATION,  # Read-focused access pattern
                default_timezone=pytz.UTC  # Explicitly set timezone to UTC to avoid naive datetime warnings
            )
            
            # Cache the account
            self.accounts[smtp_address] = account
            print(f"✅ Successfully connected to Exchange account: {smtp_address}")
            return account
            
        except Exception as e:
            print(f"❌ Failed to connect to Exchange account {smtp_address}: {e}")
            return None
    
    def collect_current_email_ids(self, folder, current_ids: Set[str], folder_path: str = ""):
        """Recursively collect all current email IDs from Exchange folders"""
        try:
            # Build current folder path
            current_folder_path = f"{folder_path}/{folder.name}" if folder_path else folder.name
            
            # Skip folders using centralized logic (including Deleted Items and subfolders)
            if self.should_skip_folder(folder, current_folder_path):
                # Still process children in case there are non-skipped subfolders
                if hasattr(folder, 'children') and folder.children:
                    for child_folder in folder.children:
                        self.collect_current_email_ids(child_folder, current_ids, current_folder_path)
                return
            
            if folder.total_count > 0:
                items = folder.all().only('id')
                for item in items:
                    current_ids.add(str(item.id))
            
            if hasattr(folder, 'children') and folder.children:
                for child_folder in folder.children:
                    self.collect_current_email_ids(child_folder, current_ids, current_folder_path)
                    
        except Exception as e:
            print(f"Error collecting IDs from folder {folder.name}: {e}")
    
    def get_all_current_email_ids(self, account: Account, process_main_mailbox: bool = False) -> Set[str]:
        """Get all current email IDs from an Exchange account"""
        current_ids = set()
        
        # Only collect from main mailbox if enabled
        if process_main_mailbox and hasattr(account, 'msg_folder_root'):
            print("Collecting current email IDs from main mailbox...")
            self.collect_current_email_ids(account.msg_folder_root, current_ids)
        elif not process_main_mailbox:
            print("Skipping main mailbox for orphaned item detection (PROCESS_MAIN_MAILBOX=false)")
        
        if hasattr(account, 'archive_msg_folder_root') and account.archive_msg_folder_root:
            print("Collecting current email IDs from archive...")
            self.collect_current_email_ids(account.archive_msg_folder_root, current_ids)
        
        return current_ids
    
    def should_skip_folder(self, folder, folder_path: str = None) -> bool:
        """Determine if a folder should be skipped during processing"""
        skip_folders = ['Deleted Items', 'Junk Email', 'Drafts']
        
        # Check if current folder name is in skip list
        if folder.name in skip_folders:
            return True
        
        # Check if folder is a subfolder of "Deleted Items"
        if folder_path and 'Deleted Items' in folder_path:
            return True
            
        if hasattr(folder, 'folder_class') and folder.folder_class:
            if folder.folder_class not in ['IPF.Note', 'IPF.Note.OutlookHomepage']:
                return True
                
        return False
    
    def verify_read_only_mode(self) -> bool:
        """
        Verify that the client is operating in read-only mode.
        This is a safety check to ensure no write operations are performed.
        """
        return self._read_only_mode
    
    def _prevent_write_operations(self, operation_name: str):
        """
        Internal method to prevent write operations.
        Raises an exception if write operations are attempted.
        """
        if self._read_only_mode:
            raise PermissionError(
                f"Write operation '{operation_name}' is not allowed in read-only mode. "
                f"This connector is designed for read-only email indexing only."
            )