"""
Document Processor Module
Handles email processing and conversion to Q Business documents
"""

import re
import io
import time
import base64
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from .security_utils import sanitize_html_content, handle_error_securely, validate_email_content, _sanitize_text_content

# Document processing libraries (optional)
try:
    import PyPDF2
    from docx import Document as DocxDocument
    import pandas as pd
    import openpyxl
except ImportError as e:
    print(f"Warning: Some document parsing libraries not available: {e}")

logger = logging.getLogger(__name__)

class DocumentProcessor:
    """Processes Exchange emails and converts them to Q Business documents"""
    
    def __init__(self, config):
        self.config = config
        # Attachment processing settings
        self.max_attachment_size = getattr(config, 'max_attachment_size_mb', 50) * 1000000
        self.process_attachments = getattr(config, 'process_attachments', True)
        self.supported_attachment_types = getattr(config, 'supported_attachment_types', 
                                            ['.pdf', '.docx', '.doc', '.xlsx', '.xls'])

    def extract_text_from_pdf(self, content_bytes: bytes) -> str:
        """Extract text from PDF attachment"""
        try:
            import PyPDF2
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(content_bytes))
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            return text.strip()
        except ImportError:
            print("PyPDF2 not installed, skipping PDF text extraction")
            return ""
        except Exception as e:
            print(f"Error extracting PDF text: {e}")
            return "" 

    def extract_text_from_docx(self, content_bytes: bytes) -> str:
        """Extract text from Word document attachment"""
        try:
            from docx import Document as DocxDocument
            doc = DocxDocument(io.BytesIO(content_bytes))
            text = "\n".join([paragraph.text for paragraph in doc.paragraphs])
            return text.strip()
        except ImportError:
            print("python-docx not installed, skipping DOCX text extraction")
            return ""
        except Exception as e:
            print(f"Error extracting DOCX text: {e}")
            return ""

    def extract_text_from_excel(self, content_bytes: bytes) -> str:
        """Extract text from Excel attachment"""
        try:
            import pandas as pd
            df = pd.read_excel(io.BytesIO(content_bytes), sheet_name=None)
            text = ""
            for sheet_name, sheet_df in df.items():
                text += f"Sheet: {sheet_name}\n"
                text += sheet_df.to_string(index=False) + "\n\n"
            return text.strip()
        except ImportError:
            print("pandas/openpyxl not installed, skipping Excel text extraction")
            return ""
        except Exception as e:
            print(f"Error extracting Excel text: {e}")
            return ""

    def process_attachment(self, attachment, email_id: str) -> Optional[str]:
        """Enhanced attachment processing with better content extraction"""
        try:
            if not hasattr(attachment, 'name') or not attachment.name:
                return None
            
            filename = attachment.name.lower()
            
            # Get content
            content_bytes = self._get_attachment_content(attachment)
            if not content_bytes:
                return None
            
            # Add size check
            if len(content_bytes) > self.max_attachment_size:
                print(f"Attachment too large ({len(content_bytes)} bytes), skipping")
                return None
            
            # Extract text with context
            text_content = self._extract_attachment_text(content_bytes, filename)
            if not text_content:
                return None
            
            # Create structured attachment content
            structured_attachment = f"""
=== ATTACHMENT: {attachment.name} ===
File Type: {self._get_file_type_description(filename)}
Size: {len(content_bytes)} bytes

Content:
{text_content}

=== END OF ATTACHMENT ===
"""
            
            return structured_attachment
            
        except Exception as e:
            print(f"Error processing attachment: {e}")
            return None
    
    def _get_attachment_content(self, attachment) -> Optional[bytes]:
        """Get attachment content using multiple methods"""
        content_bytes = None
        filename = getattr(attachment, 'name', 'unknown')
        
        # Method 1: Direct content access
        if hasattr(attachment, 'content') and attachment.content:
            content_bytes = attachment.content
            print(f"Got attachment content via direct access: {len(content_bytes)} bytes")
        
        # Method 2: Load attachment if needed
        elif hasattr(attachment, 'load'):
            try:
                attachment.load()
                if hasattr(attachment, 'content') and attachment.content:
                    content_bytes = attachment.content
                    print(f"Got attachment content after load(): {len(content_bytes)} bytes")
            except Exception as e:
                print(f"Failed to load attachment: {e}")
        
        # Method 3: Try attachment_content property
        elif hasattr(attachment, 'attachment_content'):
            content_bytes = attachment.attachment_content
            print(f"Got attachment content via attachment_content: {len(content_bytes)} bytes")
        
        if not content_bytes:
            print(f"No content available for attachment")
        
        return content_bytes
    
    def _extract_attachment_text(self, content_bytes: bytes, filename: str) -> str:
        """Extract text from attachment based on file type"""
        # Skip non-document attachments
        supported_extensions = ['.pdf', '.docx', '.doc', '.xlsx', '.xls']
        if not any(filename.endswith(ext) for ext in supported_extensions):
            print(f"Skipping unsupported attachment type")
            return ""
        
        print(f"Processing attachment ({len(content_bytes)} bytes)")
        
        # Extract text based on file type
        text_content = ""
        if filename.endswith('.pdf'):
            text_content = self.extract_text_from_pdf(content_bytes)
        elif filename.endswith(('.docx', '.doc')):
            text_content = self.extract_text_from_docx(content_bytes)
        elif filename.endswith(('.xlsx', '.xls')):
            text_content = self.extract_text_from_excel(content_bytes)
        
        if not text_content:
            print(f"No text extracted from attachment")
        
        return text_content
    
    def _get_file_type_description(self, filename: str) -> str:
        """Get human-readable file type description"""
        if filename.endswith('.pdf'):
            return 'PDF Document'
        elif filename.endswith(('.docx', '.doc')):
            return 'Word Document'
        elif filename.endswith(('.xlsx', '.xls')):
            return 'Excel Spreadsheet'
        elif filename.endswith(('.pptx', '.ppt')):
            return 'PowerPoint Presentation'
        else:
            return 'Document'

    def html_to_text(self, html_content: str) -> str:
        """Convert HTML content to plain text using optimized approach"""
        if not html_content:
            return ""
        
        start_time = time.time()
        content_size = len(html_content)
        
        # Use configurable threshold for optimization decision
        threshold = getattr(self.config, 'html_processing_threshold', 100000)
        
        if content_size > threshold:
            print(f"Large HTML content detected ({content_size} chars), using optimized conversion...")
            result = self._fast_html_to_text(html_content)
        else:
            result = self._standard_html_to_text(html_content)
        
        processing_time = time.time() - start_time
        print(f"HTML conversion completed in {processing_time:.2f}s (input: {content_size} chars, output: {len(result)} chars)")
        
        return result
    
    def _fast_html_to_text(self, html_content: str) -> str:
        """Fast HTML to text conversion for large content"""
        # Use configurable chunk size threshold
        chunk_threshold = getattr(self.config, 'html_chunk_size', 500000) * 2  # 2x chunk size as threshold
        
        # For extremely large content, use chunked processing
        if len(html_content) > chunk_threshold:
            return self._chunked_html_to_text(html_content)
        
        # Pre-compile regex patterns for better performance
        if not hasattr(self, '_compiled_patterns'):
            self._compiled_patterns = {
                'script_style': re.compile(r'<(script|style)[^>]*>.*?</\1>', re.DOTALL | re.IGNORECASE),
                'block_elements': re.compile(r'<(br|p|div|h[1-6]|tr)[^>]*>', re.IGNORECASE),
                'list_items': re.compile(r'<li[^>]*>', re.IGNORECASE),
                'table_cells': re.compile(r'<td[^>]*>', re.IGNORECASE),
                'all_tags': re.compile(r'<[^>]+>'),
                'entities': re.compile(r'&(nbsp|amp|lt|gt|quot|#39);'),
                'whitespace': re.compile(r'[ \t]+'),
                'newlines': re.compile(r'\n\s*\n\s*\n+')
            }
        
        # Remove script and style elements first
        html_content = self._compiled_patterns['script_style'].sub('', html_content)
        
        # Convert block elements to newlines in one pass
        html_content = self._compiled_patterns['block_elements'].sub('\n', html_content)
        html_content = self._compiled_patterns['list_items'].sub('\n• ', html_content)
        html_content = self._compiled_patterns['table_cells'].sub('\t', html_content)
        
        # Remove all remaining HTML tags
        html_content = self._compiled_patterns['all_tags'].sub('', html_content)
        
        # Decode HTML entities using a single regex pass for better performance
        if '&' in html_content:  # Only process if entities are present
            entity_map = {'nbsp': ' ', 'amp': '&', 'lt': '<', 'gt': '>', 'quot': '"', '#39': "'"}
            def replace_entity(match):
                entity = match.group(1)
                return entity_map.get(entity, match.group(0))
            html_content = re.sub(r'&(nbsp|amp|lt|gt|quot|#39);', replace_entity, html_content)
        
        # Clean up whitespace in two passes
        html_content = self._compiled_patterns['whitespace'].sub(' ', html_content)
        html_content = self._compiled_patterns['newlines'].sub('\n\n', html_content)
        
        return html_content.strip()
    
    def _chunked_html_to_text(self, html_content: str) -> str:
        """Process extremely large HTML content in chunks to avoid memory issues"""
        print(f"Processing extremely large HTML content ({len(html_content)} chars) in chunks...")
        
        # First, remove script and style elements from the entire content
        # This is important to do first to avoid processing their content
        html_content = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Use configurable chunk size
        chunk_size = getattr(self.config, 'html_chunk_size', 500000)
        chunks = []
        
        for i in range(0, len(html_content), chunk_size):
            chunk = html_content[i:i + chunk_size]
            
            # Process chunk with simplified regex operations
            # Convert block elements to newlines
            chunk = re.sub(r'<(br|p|div|h[1-6]|tr)[^>]*>', '\n', chunk, flags=re.IGNORECASE)
            chunk = re.sub(r'<li[^>]*>', '\n• ', chunk, flags=re.IGNORECASE)
            chunk = re.sub(r'<td[^>]*>', '\t', chunk, flags=re.IGNORECASE)
            
            # Remove all HTML tags
            chunk = re.sub(r'<[^>]+>', '', chunk)
            
            # Basic entity decoding
            chunk = chunk.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace('&#39;', "'")
            
            chunks.append(chunk)
        
        # Join chunks and clean up whitespace
        result = ''.join(chunks)
        result = re.sub(r'[ \t]+', ' ', result)
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        
        print(f"Chunked processing completed, result size: {len(result)} chars")
        return result.strip()
    
    def _simple_html_strip(self, html_content: str) -> str:
        """Ultra-fast HTML stripping for emergency fallback"""
        # Remove script and style content
        html_content = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Convert line breaks
        html_content = html_content.replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n')
        html_content = html_content.replace('</p>', '\n').replace('</div>', '\n')
        
        # Strip all remaining tags
        html_content = re.sub(r'<[^>]+>', '', html_content)
        
        # Basic entity decoding
        html_content = html_content.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')
        
        # Clean whitespace
        html_content = re.sub(r'\s+', ' ', html_content)
        html_content = re.sub(r'\n\s*\n', '\n\n', html_content)
        
        return html_content.strip()
    
    def _standard_html_to_text(self, html_content: str) -> str:
        """Standard HTML to text conversion for smaller content"""
        # Remove script and style elements
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Convert common HTML elements to text equivalents
        html_content = re.sub(r'<br[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<p[^>]*>', '\n\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</p>', '', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<div[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</div>', '', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<h[1-6][^>]*>', '\n\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</h[1-6]>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<li[^>]*>', '\n• ', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</li>', '', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<tr[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<td[^>]*>', '\t', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</td>', '', html_content, flags=re.IGNORECASE)
        
        # Remove all remaining HTML tags
        html_content = re.sub(r'<[^>]+>', '', html_content)
        
        # Decode HTML entities
        html_content = html_content.replace('&nbsp;', ' ')
        html_content = html_content.replace('&amp;', '&')
        html_content = html_content.replace('&lt;', '<')
        html_content = html_content.replace('&gt;', '>')
        html_content = html_content.replace('&quot;', '"')
        html_content = html_content.replace('&#39;', "'")
        
        # Clean up whitespace
        html_content = re.sub(r'\n\s*\n\s*\n', '\n\n', html_content)  # Multiple newlines to double
        html_content = re.sub(r'[ \t]+', ' ', html_content)  # Multiple spaces to single
        html_content = html_content.strip()
        
        return html_content
    
    def clean_string(self, value) -> str:
        """Clean string value for Q Business compatibility"""
        if value is None:
            return ""
        return str(value).replace('\x00', '').strip()
    
    def format_datetime(self, dt_value) -> str:
        """Format datetime for Q Business, ensuring timezone awareness"""
        if dt_value is None:
            return ""
        try:
            if hasattr(dt_value, 'isoformat'):
                # Handle exchangelib datetime objects specially
                if (hasattr(dt_value, '__class__') and 
                    ('exchangelib' in str(dt_value.__class__) or 
                     hasattr(dt_value, 'tzinfo') and dt_value.tzinfo and 'EWSTimeZone' in str(type(dt_value.tzinfo)))):
                    # This is an exchangelib datetime object, use it directly
                    return dt_value.isoformat()
                
                # Handle standard Python datetime objects
                if hasattr(dt_value, 'tzinfo'):
                    if dt_value.tzinfo is None:
                        # Convert naive datetime to UTC with a warning
                        print(f"Warning: Converting naive datetime {dt_value} to UTC")
                        dt_value = dt_value.replace(tzinfo=timezone.utc)
                    else:
                        # For timezone-aware datetime, convert to UTC if it's not an EWS datetime
                        try:
                            dt_value = dt_value.astimezone(timezone.utc)
                        except (TypeError, ValueError):
                            # If conversion fails (e.g., EWSTimeZone), use as-is
                            pass
                
                return dt_value.isoformat()
            return str(dt_value)
        except Exception as e:
            print(f"Warning: Error formatting datetime {dt_value}: {e}")
            return ""
    
    def get_email_addresses_list(self, recipients) -> List[str]:
        """Extract email addresses from recipients list"""
        if not recipients:
            return []
        return [getattr(r, 'email_address', '') for r in recipients if r and hasattr(r, 'email_address')]
    
    def get_display_names_list(self, recipients) -> List[str]:
        """Extract display names from recipients list"""
        if not recipients:
            return []
        return [getattr(r, 'name', '') for r in recipients if r and hasattr(r, 'name')]
    
    def _get_email_body_content(self, email) -> str:
        """Extract and process email body content with security validation"""
        body_content = ''
        
        if hasattr(email, 'body') and email.body:
            raw_body_content = str(email.body)
            raw_content_size = len(raw_body_content)
            
            logger.debug(f"Processing email body: {raw_content_size} characters")
            
            # Validate content size
            try:
                validate_email_content(raw_body_content, getattr(self.config, 'max_content_size_mb', 50))
            except ValueError as e:
                logger.warning(f"Email content size validation failed: {e}")
                # Truncate to safe size
                max_chars = getattr(self.config, 'max_content_size_mb', 50) * 1000000
                raw_body_content = raw_body_content[:max_chars] + "\n[Content truncated due to size limit]"
            
            # Detect and convert HTML
            body_lower = raw_body_content.lower().strip()
            html_tags = ['<html', '<body', '<div', '<p>', '<br', '<span', '<table', '<tr', '<td']
            body_is_html = any(tag in body_lower for tag in html_tags)
            
            if body_is_html:
                try:
                    # Sanitize HTML content before processing
                    sanitized_html = sanitize_html_content(raw_body_content)
                    body_content = self.html_to_text(sanitized_html)
                except Exception as e:
                    error_msg = handle_error_securely(e, "HTML conversion")
                    logger.warning(error_msg)
                    body_content = self._simple_html_strip(raw_body_content)
            else:
                body_content = raw_body_content
            
            # Final content sanitization
            body_content = _sanitize_text_content(body_content)
        
        return body_content
    
    def _process_email_attachments(self, email) -> str:
        """Process all email attachments"""
        attachment_content = ""
        has_attachments = getattr(email, 'has_attachments', False)
        
        if has_attachments and hasattr(email, 'attachments'):
            print(f"Processing {len(email.attachments)} attachments...")
            for attachment in email.attachments:
                attachment_text = self.process_attachment(attachment, str(email.id))
                if attachment_text:
                    attachment_content += attachment_text
        
        return attachment_content
    
    def _create_structured_email_content(self, email, body_content: str, attachment_content: str, folder_name: str, account_email: str) -> str:
        """Create well-structured content for better Q Business understanding"""
        
        # Extract key information
        subject = getattr(email, 'subject', '') or ''
        sender_name = getattr(email.sender, 'name', '') if hasattr(email, 'sender') and email.sender else ''
        sender_email = getattr(email.sender, 'email_address', '') if hasattr(email, 'sender') and email.sender else ''
        sent_date = self.format_datetime(getattr(email, 'datetime_sent', None))
        
        # Create structured content
        structured_parts = []
        
        # Email header section
        structured_parts.append("=== EMAIL DETAILS ===")
        structured_parts.append(f"Subject: {subject}")
        structured_parts.append(f"From: {sender_name} <{sender_email}>")
        structured_parts.append(f"Date: {sent_date}")
        
        # Recipients section
        if hasattr(email, 'to_recipients') and email.to_recipients:
            to_list = [f"{getattr(r, 'name', '')} <{getattr(r, 'email_address', '')}>" for r in email.to_recipients if r]
            structured_parts.append(f"To: {'; '.join(to_list)}")
        
        if hasattr(email, 'cc_recipients') and email.cc_recipients:
            cc_list = [f"{getattr(r, 'name', '')} <{getattr(r, 'email_address', '')}>" for r in email.cc_recipients if r]
            structured_parts.append(f"CC: {'; '.join(cc_list)}")
        
        structured_parts.append("")  # Empty line
        
        # Main content section
        structured_parts.append("=== EMAIL CONTENT ===")
        structured_parts.append(body_content)
        
        # Attachments section
        if attachment_content:
            structured_parts.append("\n=== ATTACHMENTS ===")
            structured_parts.append(attachment_content)
        
        return "\n".join(structured_parts)
    
    def _create_enhanced_title(self, email, folder_name: str) -> str:
        """Create descriptive title for better search relevance"""
        subject = getattr(email, 'subject', '') or ''
        sender_name = getattr(email.sender, 'name', '') if hasattr(email, 'sender') and email.sender else ''
        
        # Clean and enhance title
        if subject:
            # Remove common email prefixes
            clean_subject = re.sub(r'^(RE:|FW:|FWD:)\s*', '', subject, flags=re.IGNORECASE).strip()
            title = f"{clean_subject}"
            if sender_name:
                title += f" (from {sender_name})"
        else:
            title = f"Email from {sender_name}" if sender_name else "Email"
        
        # Add folder context for better categorization
        if folder_name and folder_name.lower() not in ['inbox', 'root']:
            title += f" [{folder_name}]"
        
        return self.clean_string(title)
    
    def _create_enhanced_attributes(self, email, folder_name: str, account_email: str) -> List[Dict]:
        """Create comprehensive metadata attributes for better search and filtering"""
        
        # Get basic email info
        sender_email = getattr(email.sender, 'email_address', '') if hasattr(email, 'sender') and email.sender else ''
        subject = getattr(email, 'subject', '') or ''
        has_attachments = getattr(email, 'has_attachments', False)
        is_read = getattr(email, 'is_read', False)
        
        attributes = [
            # Core Q Business fields
            {'name': '_source_uri', 'value': {'stringValue': f"https://outlook.office365.com/owa/?ItemID={email.id}&exvsurl=1&viewmodel=ReadMessageItem"}},
            {'name': '_created_at', 'value': {'dateValue': self.format_datetime(getattr(email, 'datetime_created', None))}},
            {'name': '_last_updated_at', 'value': {'dateValue': self.format_datetime(getattr(email, 'last_modified_time', None))}},
            {'name': '_category', 'value': {'stringValue': 'EMAIL'}},
            
            # Enhanced searchable fields
            {'name': 'email_thread_topic', 'value': {'stringValue': self._extract_thread_topic(email)}},
            {'name': 'email_priority', 'value': {'stringValue': self._get_email_priority(email)}},
            {'name': 'email_content_type', 'value': {'stringValue': self._classify_email_content(email)}},
            {'name': 'email_participants', 'value': {'stringListValue': self._get_all_participants(email)}},
            {'name': 'email_domain_context', 'value': {'stringValue': self._extract_domain_context(email)}},
            {'name': 'email_time_period', 'value': {'stringValue': self._get_time_period(email)}},
            
            # Standard Exchange fields
            {'name': 'xchng_bccRecipient', 'value': {'stringListValue': self.get_email_addresses_list(getattr(email, 'bcc_recipients', []))}},
            {'name': 'xchng_ccRecipient', 'value': {'stringListValue': self.get_email_addresses_list(getattr(email, 'cc_recipients', []))}},
            {'name': 'xchng_hasAttachment', 'value': {'stringValue': str(has_attachments).lower()}},
            {'name': 'xchng_sendDateTime', 'value': {'dateValue': self.format_datetime(getattr(email, 'datetime_sent', None))}},
            {'name': 'xchng_importance', 'value': {'stringValue': self.clean_string(getattr(email, 'importance', ''))}},
            {'name': 'xchng_from', 'value': {'stringValue': self.clean_string(sender_email)}},
            {'name': 'xchng_to', 'value': {'stringListValue': self.get_email_addresses_list(getattr(email, 'to_recipients', []))}},
            {'name': 'xchng_receivedDateTime', 'value': {'dateValue': self.format_datetime(getattr(email, 'datetime_received', None))}},
            {'name': 'xchng_isRead', 'value': {'stringValue': str(is_read).lower()}},
            {'name': 'xchng_replyTo', 'value': {'stringValue': self.clean_string(getattr(email, 'reply_to', ''))}},
            {'name': 'xchng_folder', 'value': {'stringValue': self.clean_string(folder_name)}},
            {'name': 'xchng_title', 'value': {'stringValue': self.clean_string(subject)}},
            {'name': 'xchng_flagStatus', 'value': {'stringValue': self.clean_string(getattr(email, 'flag_status', ''))}},
            {'name': 'xchng_accountOwner', 'value': {'stringValue': self.clean_string(account_email or '')}}
        ]
        
        return attributes
    
    def _extract_thread_topic(self, email) -> str:
        """Extract clean thread topic for conversation grouping"""
        subject = getattr(email, 'subject', '') or ''
        clean_topic = re.sub(r'^(RE:|FW:|FWD:)\s*', '', subject, flags=re.IGNORECASE).strip()
        return self.clean_string(clean_topic)
    
    def _get_email_priority(self, email) -> str:
        """Determine email priority/importance"""
        importance = getattr(email, 'importance', '')
        if importance:
            return str(importance).lower()
        return 'normal'
    
    def _classify_email_content(self, email) -> str:
        """Classify email content type for better categorization"""
        subject = getattr(email, 'subject', '').lower()
        
        # Meeting/Calendar related
        if any(word in subject for word in ['meeting', 'calendar', 'appointment', 'schedule']):
            return 'meeting'
        
        # Project/Task related
        if any(word in subject for word in ['project', 'task', 'deadline', 'deliverable']):
            return 'project'
        
        # Document/Report related
        if any(word in subject for word in ['report', 'document', 'analysis', 'review']):
            return 'document'
        
        # Notification/Alert
        if any(word in subject for word in ['notification', 'alert', 'reminder', 'update']):
            return 'notification'
        
        return 'general'
    
    def _get_all_participants(self, email) -> List[str]:
        """Get all email participants for relationship mapping"""
        participants = []
        
        # Add sender
        if hasattr(email, 'sender') and email.sender:
            participants.append(getattr(email.sender, 'email_address', ''))
        
        # Add all recipients
        for recipient_list in ['to_recipients', 'cc_recipients', 'bcc_recipients']:
            if hasattr(email, recipient_list):
                recipients = getattr(email, recipient_list) or []
                participants.extend([getattr(r, 'email_address', '') for r in recipients if r and hasattr(r, 'email_address')])
        
        return list(set(filter(None, participants)))
    
    def _extract_domain_context(self, email) -> str:
        """Extract domain/organization context"""
        if hasattr(email, 'sender') and email.sender:
            sender_email = getattr(email.sender, 'email_address', '')
            if '@' in sender_email:
                domain = sender_email.split('@')[1].lower()
                return domain
        return ''
    
    def _get_time_period(self, email) -> str:
        """Categorize email by time period for temporal queries"""
        sent_date = getattr(email, 'datetime_sent', None)
        if not sent_date:
            return 'unknown'
        
        now = datetime.now(timezone.utc)
        if hasattr(sent_date, 'replace'):
            if sent_date.tzinfo is None:
                sent_date = sent_date.replace(tzinfo=timezone.utc)
        
        days_ago = (now - sent_date).days
        
        if days_ago <= 1:
            return 'today'
        elif days_ago <= 7:
            return 'this_week'
        elif days_ago <= 30:
            return 'this_month'
        elif days_ago <= 90:
            return 'last_3_months'
        elif days_ago <= 365:
            return 'this_year'
        else:
            return 'older'
    
    def _improve_content_quality(self, content: str) -> str:
        """Improve content quality for better Q Business understanding"""
        
        # Remove excessive whitespace but preserve structure
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        
        # Fix common formatting issues
        content = re.sub(r'([.!?])\s*([A-Z])', r'\1 \2', content)
        content = re.sub(r'([a-z])([A-Z])', r'\1 \2', content)
        
        # Remove email signatures and disclaimers
        content = re.sub(r'\n\s*--+\s*\n.*', '', content, flags=re.DOTALL)
        content = re.sub(r'\n\s*This email.*confidential.*', '', content, flags=re.DOTALL | re.IGNORECASE)
        
        return content.strip()   
 
    def create_qbusiness_document(self, email_item, folder_name: str, folder, account_email: str = None) -> Optional[Dict[str, Any]]:
        """Create an enhanced Q Business document from an email item with better structure and metadata"""
        try:
            full_email = folder.get(id=email_item.id)
            
            # Get email body content
            body_content = self._get_email_body_content(full_email)
            
            # Process attachments
            attachment_content = self._process_email_attachments(full_email)
            
            # Create structured content
            structured_content = self._create_structured_email_content(full_email, body_content, attachment_content, folder_name, account_email)
            
            # Create enhanced title
            enhanced_title = self._create_enhanced_title(full_email, folder_name)
            
            # Improve content quality
            structured_content = self._improve_content_quality(structured_content)

            # Check content size limit (50MB for Q Business)
            content_bytes = structured_content.encode('utf-8')
            content_byte_size = len(content_bytes)
            
            if content_byte_size > 50000000:
                print(f"Warning: Content too large ({content_byte_size} bytes), truncating...")
                max_chars = int(50000000 * 0.8)
                structured_content = structured_content[:max_chars] + "\n[Content truncated due to size limit]"
            
            content_type = 'PLAIN_TEXT'
            
            # Convert content to base64 blob
            content_bytes = structured_content.encode('utf-8')
            content_blob = base64.b64encode(content_bytes).decode('utf-8')

            # Create ACL
            access_control_list = []
            if account_email:
                access_control_list = [
                    {
                        'memberRelation': 'OR',
                        'principals': [
                            {
                                'user': {
                                    'id': account_email,
                                    'access': 'ALLOW'
                                }
                            }
                        ]
                    }
                ]

            # Create enhanced attributes
            enhanced_attributes = self._create_enhanced_attributes(full_email, folder_name, account_email)

            document = {
                'id': str(full_email.id),
                'title': enhanced_title,
                'content': {
                    'blob': content_blob
                },
                'contentType': content_type,
                'accessConfiguration': {
                    'accessControls': access_control_list
                },
                'attributes': enhanced_attributes
            }
            
            if not document.get('id'):
                raise ValueError("Document ID is required")
            if not document.get('title'):
                raise ValueError("Document title is required")
            if not document.get('content', {}).get('blob'):
                raise ValueError("Document content is required")
                
            print(f"Created enhanced document: ID={document['id'][:50]}..., Size={len(content_bytes)} bytes")
            
            return document
            
        except Exception as e:
            print(f"Error creating document for email {email_item.id}: {e}")
            return None