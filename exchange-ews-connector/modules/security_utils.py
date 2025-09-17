"""
Security Utilities Module
Provides security-focused utilities for logging, error handling, and data sanitization
"""

import re
import uuid
import time
import logging
import secrets
from typing import List

# Configure secure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sanitize_for_logging(data):
    """Remove sensitive information from log data and prevent log injection"""
    if isinstance(data, str):
        # Remove newlines and carriage returns to prevent log injection (CWE-117)
        data = re.sub(r'[\r\n]', ' ', data)
        # Redact email addresses
        data = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', data)
        # Redact sensitive keys and tokens
        data = re.sub(r'(token|secret|password|key|credential)[\s:=]+\S+', r'\1=[REDACTED]', data, flags=re.IGNORECASE)
        # Redact AWS account IDs
        data = re.sub(r'\b\d{12}\b', '[AWS_ACCOUNT]', data)
        # Redact UUIDs that might be sensitive
        data = re.sub(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', '[UUID]', data, flags=re.IGNORECASE)
        # Limit length to prevent log flooding
        if len(data) > 500:
            data = data[:500] + '[TRUNCATED]'
    return data

def handle_error_securely(error: Exception, context: str = "") -> str:
    """Handle errors without exposing sensitive information"""
    error_id = str(uuid.uuid4())[:8]
    
    # Log full error internally with sanitization
    sanitized_error = sanitize_for_logging(str(error))
    logger.error(f"Error {error_id} in {context}: {type(error).__name__}: {sanitized_error}")
    
    # Return generic message externally
    return f"Operation failed (Error ID: {error_id})"

def validate_email_content(content: str, max_size_mb: int = 10) -> bool:
    """Validate email content size and format"""
    if not content:
        return True
    
    # Check size limit
    content_size_mb = len(content.encode('utf-8')) / (1024 * 1024)
    if content_size_mb > max_size_mb:
        raise ValueError(f"Content size {content_size_mb:.2f}MB exceeds limit of {max_size_mb}MB")
    
    # Basic content validation
    if len(content.strip()) == 0:
        return False
    
    return True

def sanitize_html_content(content: str) -> str:
    """Sanitize HTML content to prevent XSS and other attacks"""
    if not content:
        return content
    
    # Remove potentially dangerous HTML tags and attributes
    dangerous_patterns = [
        r'<script[^>]*>.*?</script>',
        r'<iframe[^>]*>.*?</iframe>',
        r'<object[^>]*>.*?</object>',
        r'<embed[^>]*>.*?</embed>',
        r'<form[^>]*>.*?</form>',
        r'javascript:',
        r'vbscript:',
        r'data:text/html',
        r'on\w+\s*=',  # Event handlers like onclick, onload, etc.
    ]
    
    for pattern in dangerous_patterns:
        content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.DOTALL)
    
    return content

def generate_secure_id() -> str:
    """Generate a cryptographically secure random ID"""
    return secrets.token_urlsafe(32)

def validate_aws_response(response: dict, expected_keys: List[str] = None) -> bool:
    """Validate AWS service response structure"""
    if not isinstance(response, dict):
        return False
    
    # Check for standard AWS response metadata
    if 'ResponseMetadata' not in response:
        return False
    
    metadata = response['ResponseMetadata']
    if not isinstance(metadata, dict):
        return False
    
    # Check HTTP status code
    status_code = metadata.get('HTTPStatusCode')
    if status_code not in [200, 201, 202, 204]:
        return False
    
    # Check expected keys if provided
    if expected_keys:
        for key in expected_keys:
            if key not in response:
                return False
    
    return True

def _sanitize_text_content(content: str) -> str:
    """Sanitize text content to remove potentially harmful patterns"""
    if not content:
        return content
    
    # Remove null bytes and other control characters
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
    
    # Remove excessive whitespace but preserve structure
    content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
    content = re.sub(r'[ \t]+', ' ', content)
    
    # Remove common email signatures and disclaimers that might contain sensitive info
    content = re.sub(r'\n\s*--+\s*\n.*', '', content, flags=re.DOTALL)
    content = re.sub(r'\n\s*This email.*confidential.*', '', content, flags=re.DOTALL | re.IGNORECASE)
    
    # Limit line length to prevent buffer overflow attacks
    lines = content.split('\n')
    sanitized_lines = []
    for line in lines:
        if len(line) > 10000:  # Reasonable line length limit
            line = line[:10000] + '[Line truncated]'
        sanitized_lines.append(line)
    
    return '\n'.join(sanitized_lines).strip()

class RateLimiter:
    """Simple rate limiter for email processing"""
    
    def __init__(self, max_operations_per_minute: int = 60):
        self.max_operations = max_operations_per_minute
        self.operations = []
        self.lock = False
    
    def can_proceed(self) -> bool:
        """Check if operation can proceed based on rate limit"""
        current_time = time.time()
        
        # Remove operations older than 1 minute
        self.operations = [op_time for op_time in self.operations if current_time - op_time < 60]
        
        # Check if we're under the limit
        if len(self.operations) < self.max_operations:
            self.operations.append(current_time)
            return True
        
        return False
    
    def wait_if_needed(self) -> None:
        """Wait if rate limit is exceeded"""
        while not self.can_proceed():
            time.sleep(1)  # Wait 1 second before retrying

class ResourceMonitor:
    """Monitor resource usage to prevent exhaustion"""
    
    def __init__(self, max_memory_mb: int = 512):
        self.max_memory_mb = max_memory_mb
        self.start_time = time.time()
    
    def check_resources(self) -> bool:
        """Check if resources are within acceptable limits"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb > self.max_memory_mb:
                logger.warning(f"Memory usage {memory_mb:.1f}MB exceeds limit of {self.max_memory_mb}MB")
                return False
            
            return True
        except ImportError:
            # psutil not available, skip memory check
            return True
        except Exception as e:
            logger.warning(f"Error checking resources: {e}")
            return True