"""
Simple HTTP health check server for the Exchange EWS Connector
Runs alongside the main sync process to provide health status
"""

import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import json
import os

class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check requests."""
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/status':
            self._handle_status()
        else:
            self._handle_not_found()
    
    def _handle_health(self):
        """Handle health check endpoint."""
        # Simple health check - if we can respond, we're healthy
        response = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "exchange-ews-connector"
        }
        
        self._send_json_response(200, response)
    
    def _handle_status(self):
        """Handle status endpoint with more detailed information."""
        # Get container info if available
        container_index = os.environ.get('CONTAINER_INDEX')
        total_containers = os.environ.get('TOTAL_CONTAINERS')
        
        response = {
            "status": "running",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service": "exchange-ews-connector",
            "version": "1.0.0",
            "container_index": container_index,
            "total_containers": total_containers,
            "sync_mode": os.environ.get('SYNC_MODE', 'delta'),
            "uptime_seconds": int(time.time() - start_time)
        }
        
        self._send_json_response(200, response)
    
    def _handle_not_found(self):
        """Handle 404 responses."""
        response = {
            "error": "Not Found",
            "message": f"Path {self.path} not found"
        }
        
        self._send_json_response(404, response)
    
    def _send_json_response(self, status_code, data):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        json_data = json.dumps(data, indent=2)
        self.wfile.write(json_data.encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to reduce log noise."""
        # Only log errors, not every request
        if args and len(args) > 1 and '200' not in str(args[1]):
            super().log_message(format, *args)

class HealthServer:
    """Simple health check server."""
    
    def __init__(self, port=8080):
        self.port = port
        self.server = None
        self.thread = None
    
    def start(self):
        """Start the health server in a background thread."""
        try:
            self.server = HTTPServer(('0.0.0.0', self.port), HealthHandler)
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
            print(f"🏥 Health server started on port {self.port}")
            print(f"   Health check: http://localhost:{self.port}/health")
            print(f"   Status: http://localhost:{self.port}/status")
        except Exception as e:
            print(f"⚠️  Failed to start health server: {e}")
    
    def stop(self):
        """Stop the health server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            print("🏥 Health server stopped")

# Global start time for uptime calculation
start_time = time.time()

# Global health server instance
health_server = None

def start_health_server(port=8080):
    """Start the health server."""
    global health_server
    health_server = HealthServer(port)
    health_server.start()
    return health_server

def stop_health_server():
    """Stop the health server."""
    global health_server
    if health_server:
        health_server.stop()
        health_server = None