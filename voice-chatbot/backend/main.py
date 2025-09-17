from fastapi import FastAPI, WebSocket, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from nova_sonic_simple import SimpleNovaSonic
import logging
import os
import wave
from datetime import datetime
import uuid
import json
import time
from api.apps import routers as app_routers
from typing import Optional
import requests
from dotenv import load_dotenv
import boto3
import jwt
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import json
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create debug directory if it doesn't exist
DEBUG_DIR = "debug_audio"
os.makedirs(DEBUG_DIR, exist_ok=True)

app = FastAPI()
CHUNK_SIZE = 512

# Q Business tool - Load from environment
REGION = os.getenv('REGION')
IDC_APPLICATION_ID = os.getenv('IDC_APPLICATION_ID')
IAM_ROLE = os.getenv('IAM_ROLE')

# JWT validation configuration
JWT_ISSUER = os.getenv('JWT_ISSUER')  # e.g., https://cognito-idp.region.amazonaws.com/user_pool_id
JWT_AUDIENCE = os.getenv('JWT_AUDIENCE')  # Client ID
JWKS_URL = os.getenv('JWKS_URL')  # e.g., https://cognito-idp.region.amazonaws.com/user_pool_id/.well-known/jwks.json

id_token=None
aws_credentials = None
amazon_q = None

# Cache for JWKS keys
_jwks_cache = {}
_jwks_cache_time = 0

def get_jwks_keys():
    """Fetch and cache JWKS keys for JWT validation"""
    global _jwks_cache, _jwks_cache_time
    import time
    
    current_time = time.time()
    # Cache keys for 1 hour
    if current_time - _jwks_cache_time < 3600 and _jwks_cache:
        return _jwks_cache
    
    if not JWKS_URL:
        raise ValueError("JWKS_URL not configured")
    
    try:
        response = requests.get(JWKS_URL, timeout=10)
        response.raise_for_status()
        jwks = response.json()
        
        keys = {}
        for key in jwks.get('keys', []):
            kid = key.get('kid')
            if kid:
                keys[kid] = key
        
        _jwks_cache = keys
        _jwks_cache_time = current_time
        return keys
    except Exception as e:
        logger.error(f"Failed to fetch JWKS keys: {e}")
        raise ValueError(f"Failed to fetch JWKS keys: {e}")

def validate_jwt_token(token: str) -> dict:
    """Validate JWT token with proper signature verification"""
    if not token or not isinstance(token, str):
        raise ValueError("Invalid token format")
    
    # Basic token structure validation
    token_parts = token.split('.')
    if len(token_parts) != 3:
        raise ValueError("Invalid JWT token structure")
    
    try:
        # Get the key ID from token header
        header = jwt.get_unverified_header(token)
        kid = header.get('kid')
        if not kid:
            raise ValueError("Token missing key ID")
        
        # Get JWKS keys
        jwks_keys = get_jwks_keys()
        if kid not in jwks_keys:
            raise ValueError(f"Key ID {kid} not found in JWKS")
        
        key_data = jwks_keys[kid]
        
        # Convert JWK to PEM format for verification
        if key_data.get('kty') != 'RSA':
            raise ValueError("Only RSA keys are supported")
        
        # Construct RSA public key from JWK
        from jwt.algorithms import RSAAlgorithm
        public_key = RSAAlgorithm.from_jwk(json.dumps(key_data))
        
        # Verify and decode token
        decoded_token = jwt.decode(
            token,
            public_key,
            algorithms=['RS256'],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_aud": True,
                "verify_iss": True
            }
        )
        
        # Additional validation
        if "sts:identity_context" not in decoded_token:
            raise ValueError("Missing required identity context in token")
        
        return decoded_token
        
    except jwt.InvalidTokenError as e:
        raise ValueError(f"Invalid JWT token: {e}")
    except Exception as e:
        raise ValueError(f"Token validation failed: {e}")

def get_iam_oidc_token(id_token):
    """
    Get the IAM OIDC token using the ID token retrieved from Cognito
    """
    client = boto3.client("sso-oidc", region_name=REGION)
    response = client.create_token_with_iam(
        clientId=IDC_APPLICATION_ID,
        grantType="urn:ietf:params:oauth:grant-type:jwt-bearer",
        assertion=id_token,
    )
    return response

def assume_role_with_token(iam_token):
    global aws_credentials
    """
    Assume IAM role with the IAM OIDC idToken
    """
    # Validate JWT token with proper signature verification
    try:
        decoded_token = validate_jwt_token(iam_token)
    except ValueError as e:
        logger.error(f"JWT validation failed: {e}")
        raise ValueError(f"Authentication failed: {e}")
    sts_client = boto3.client("sts", region_name=REGION)
    response = sts_client.assume_role(
        RoleArn=IAM_ROLE,
        RoleSessionName="qapp",
        ProvidedContexts=[
            {
                "ProviderArn": "arn:aws:iam::aws:contextProvider/IdentityCenter",
                "ContextAssertion": decoded_token["sts:identity_context"],
            }
        ],
    )
    aws_credentials = response["Credentials"]

# This method create the Q client
def get_qclient(idc_id_token: str):
    global aws_credentials
    global amazon_q
    """
    Create the Q client using the identity-aware AWS Session.
    """
    if aws_credentials is None:
        assume_role_with_token(idc_id_token)
    elif aws_credentials["Expiration"] < datetime.datetime.now(datetime.UTC):
        assume_role_with_token(idc_id_token)
    
    session = boto3.Session(
        aws_access_key_id=aws_credentials["AccessKeyId"],
        aws_secret_access_key=aws_credentials["SecretAccessKey"],
        aws_session_token=aws_credentials["SessionToken"],
    )
    print("access_key_id")
    amazon_q = session.client("qbusiness", REGION)
    return amazon_q

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self, save_debug_audio=True):
        self.nova_client = None
        self.active_connection = None
        self.audio_content_started = False
        self.debug_input_file = None
        self.debug_output_file = None
        self.received_chunks = 0
        self.sent_chunks = 0
        self.current_content_name = None
        self.current_tool_use_id = None
        self.current_tool_name = None
        self.last_audio_chunk_time = 0  # Track timing of audio chunks
        self.audio_chunk_threshold = 0.1  # 100ms threshold for audio chunks
        self.save_debug_audio = save_debug_audio  # <--- New config option
        # --- Chat history ---
        self.chat_history = []  # List of dicts: {role, text, contentName}
        self.max_history = 10   # Rolling window size

    def add_history(self, role, text):
        """Add a message to the rolling chat history."""
        content_name = str(uuid.uuid4())
        self.chat_history.append({
            'role': role,
            'text': text,
            'contentName': content_name
        })
        # Keep only the last N messages
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history:]

    def get_history(self):
        """Get the current rolling chat history."""
        return self.chat_history.copy()

    def _create_debug_files(self):
        if not self.save_debug_audio:
            self.debug_input_file = None
            self.debug_output_file = None
            return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_path = os.path.join(DEBUG_DIR, f"input_{timestamp}.wav")
        output_path = os.path.join(DEBUG_DIR, f"output_{timestamp}.wav")

        # Create WAV file for input audio (16kHz)
        self.debug_input_file = wave.open(input_path, 'wb')
        self.debug_input_file.setnchannels(1)
        self.debug_input_file.setsampwidth(2)  # 16-bit
        self.debug_input_file.setframerate(16000)
        
        # Create WAV file for output audio (24kHz)
        self.debug_output_file = wave.open(output_path, 'wb')
        self.debug_output_file.setnchannels(1)
        self.debug_output_file.setsampwidth(2)  # 16-bit
        self.debug_output_file.setframerate(24000)

        logger.info(f"Created debug files: {input_path} and {output_path}")

    def _close_debug_files(self):
        if not self.save_debug_audio:
            self.debug_input_file = None
            self.debug_output_file = None
            return
        if self.debug_input_file:
            self.debug_input_file.close()
            self.debug_input_file = None
        if self.debug_output_file:
            self.debug_output_file.close()
            self.debug_output_file = None
        logger.info(f"Debug stats - Received chunks: {self.received_chunks}, Sent chunks: {self.sent_chunks}")
        self.received_chunks = 0
        self.sent_chunks = 0

    async def process_tool_use(self, tool_name, tool_use_content):
        """Process tool use requests and return results"""
        logger.info(f"Processing tool use: {tool_name}")
        
        try:
            # Use the tool manager to execute the tool
            result = await self.nova_client.tool_manager.execute_tool(tool_name, tool_use_content)
            return result
        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return {"error": f"Tool execution failed: {str(e)}"}

    async def handle_tool_use(self, event_data):
        """Handle tool use events"""
        try:
            tool_use = event_data["event"]["toolUse"]
            self.current_tool_use_id = tool_use["toolUseId"]
            self.current_tool_name = tool_use.get("toolName")
            tool_content = json.loads(tool_use.get("content", "{}"))
            
            # Process the tool use
            result = await self.process_tool_use(self.current_tool_name, tool_content)
            logger.info(f"Tool result received from implementation: {result}")
            
            if "error" not in result:
                # First send the model result
                tool_result = {
                    "event": {
                        "toolResult": {
                            "promptName": tool_use["promptName"],
                            "contentName": str(uuid.uuid4()),
                            "content": json.dumps(result["model_result"])
                        }
                    }
                }
                
                # Then prepare and send the UI result separately
                if "ui_result" in result:
                    tool_ui_result = {
                        "event": {
                            "toolUiOutput": result["ui_result"]
                        }
                    }
                
                if self.active_connection:
                    # Send model result first
                    logger.info("Sending model result")
                    await self.active_connection.send_text(json.dumps(tool_result))
                    
                    # Then send UI result if available
                    if "ui_result" in result:
                        logger.info(f"Sending UI result: {tool_ui_result}")
                        await self.active_connection.send_text(json.dumps(tool_ui_result))
                        logger.info("UI result sent")
            else:
                # Handle error case
                error_result = {
                    "event": {
                        "toolResult": {
                            "promptName": tool_use["promptName"],
                            "contentName": str(uuid.uuid4()),
                            "content": json.dumps(result)
                        }
                    }
                }
                if self.active_connection:
                    await self.active_connection.send_text(json.dumps(error_result))
                
        except Exception as e:
            logger.error(f"Error handling tool use: {e}")
            error_result = {
                "event": {
                    "toolResult": {
                        "promptName": tool_use["promptName"] if "toolUse" in event_data["event"] else str(uuid.uuid4()),
                        "contentName": str(uuid.uuid4()),
                        "content": json.dumps({"error": str(e)})
                    }
                }
            }
            if self.active_connection:
                await self.active_connection.send_text(json.dumps(error_result))

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connection = websocket
        logger.info("WebSocket connection accepted")
        
        self.nova_client = SimpleNovaSonic(amazon_q if REQUIRE_LOGIN else None)
        await self.nova_client.start_session()
        logger.info("Nova Sonic session started")

        # --- Send conversation history after system prompt ---
        history = self.get_history()
        # Only include history starting with a USER message
        while history and history[0]['role'] != 'USER':
            history.pop(0)
        if history:
            for msg in history:
                content_name = msg['contentName']
                role = msg['role']
                text = msg['text']
                # Send contentStart
                content_start = {
                    "event": {
                        "contentStart": {
                            "promptName": self.nova_client.prompt_name,
                            "contentName": content_name,
                            "type": "TEXT",
                            "interactive": False,
                            "role": role,
                            "textInputConfiguration": {
                                "mediaType": "text/plain"
                            }
                        }
                    }
                }
                await self.nova_client.send_event(json.dumps(content_start))
                # Send textInput
                text_input = {
                    "event": {
                        "textInput": {
                            "promptName": self.nova_client.prompt_name,
                            "contentName": content_name,
                            "content": text
                        }
                    }
                }
                await self.nova_client.send_event(json.dumps(text_input))
                # Send contentEnd
                content_end = {
                    "event": {
                        "contentEnd": {
                            "promptName": self.nova_client.prompt_name,
                            "contentName": content_name
                        }
                    }
                }
                await self.nova_client.send_event(json.dumps(content_end))

    async def disconnect(self):
        if self.nova_client:
            logger.info("Stopping Nova Sonic session")
            if self.audio_content_started:
                await self.stop_audio()
            self.nova_client.is_active = False
            await self.nova_client.end_session()
            self.nova_client = None
        self._close_debug_files()
        self.active_connection = None

    async def receive_audio(self, audio_data: bytes):
        if self.nova_client and self.audio_content_started:
            try:
                current_time = time.time()
                time_since_last_chunk = current_time - self.last_audio_chunk_time
                
                # Only process audio if enough time has passed (prevent overwhelming the system)
                if time_since_last_chunk >= self.audio_chunk_threshold:
                    # Save input audio to debug file
                    if self.save_debug_audio and self.debug_input_file:
                        self.debug_input_file.writeframes(audio_data)
                        self.received_chunks += 1
                        if self.received_chunks % 100 == 0:
                            logger.info(f"Received {self.received_chunks} audio chunks")

                    # Send to Nova Sonic
                    await self.nova_client.send_audio_chunk(audio_data)
                    self.last_audio_chunk_time = current_time
                    logger.debug(f"Sent audio chunk of size {len(audio_data)} bytes")
            except Exception as e:
                logger.error(f"Error sending audio chunk: {e}")

    async def start_audio(self):
        if self.nova_client and not self.audio_content_started:
            try:
                logger.info("Starting audio input")
                self._create_debug_files()  # Create new debug files for this session
                
                # Generate new unique content name
                self.current_content_name = str(uuid.uuid4())
                logger.info(f"Using new content name: {self.current_content_name}")
                
                # Start audio with new content name
                self.nova_client.audio_content_name = self.current_content_name
                await self.nova_client.start_audio_input()
                self.audio_content_started = True
            except Exception as e:
                logger.error(f"Error starting audio input: {e}")

    async def stop_audio(self):
        if self.nova_client and self.audio_content_started:
            try:
                logger.info("Stopping audio input")
                await self.nova_client.end_audio_input()
                self.audio_content_started = False
                self.current_content_name = None
                self._close_debug_files()  # Close debug files when stopping
            except Exception as e:
                logger.error(f"Error stopping audio input: {e}")

    async def process_audio_responses(self):
        if not self.nova_client or not self.active_connection:
            return

        logger.info("Started processing audio responses")
        try:
            while self.nova_client.is_active:
                try:
                    # Use timeout to allow checking barge-in status
                    audio_data = await asyncio.wait_for(
                        self.nova_client.audio_queue.get(),
                        timeout=0.1
                    )
                    
                    if audio_data:
                        # Check if we're in a barge-in state
                        if self.nova_client.barge_in:
                            logger.info("Barge-in detected, skipping audio output")
                            continue

                        # Save output audio to debug file
                        if self.save_debug_audio and self.debug_output_file:
                            self.debug_output_file.writeframes(audio_data)
                            self.sent_chunks += 1
                            if self.sent_chunks % 10 == 0:
                                logger.info(f"Sent {self.sent_chunks} response chunks")

                        # Send audio in smaller chunks for better responsiveness
                        for i in range(0, len(audio_data), CHUNK_SIZE):
                            if self.nova_client.barge_in:
                                break
                            chunk = audio_data[i:min(i + CHUNK_SIZE, len(audio_data))]
                            await self.active_connection.send_bytes(chunk)
                            await asyncio.sleep(0.001)  # Small yield

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error processing audio response: {e}")
                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Error in audio response processing loop: {e}")
        finally:
            logger.info("Stopped processing audio responses")

    async def process_events(self):
        if not self.nova_client or not self.active_connection:
            return
        try:
            while self.nova_client.is_active:
                try:
                    event_json = await asyncio.wait_for(
                        self.nova_client.event_queue.get(),
                        timeout=1.0
                    )
                    if event_json:
                        # Check for barge-in before sending events
                        event_data = json.loads(event_json)
                        if 'event' in event_data and 'textOutput' in event_data['event']:
                            text_content = event_data['event']['textOutput'].get('content', '')
                            # Add assistant message to history
                            self.add_history('ASSISTANT', text_content)
                            if '{ "interrupted" : true }' in text_content:
                                logger.info("Barge-in detected in event processing")
                                self.nova_client.barge_in = True
                                
                                # Send barge-in event to frontend
                                barge_in_event = {
                                    "event": {
                                        "toolUiOutput": {
                                            "type": "barge_in",
                                            "content": {
                                                "status": "interrupted"
                                            }
                                        }
                                    }
                                }
                                await self.active_connection.send_text(json.dumps(barge_in_event))
                                
                        await self.active_connection.send_text(event_json)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Error in event processing loop: {e}")
        finally:
            logger.info("Stopped processing events")

    async def handle_ui_interaction(self, interaction_data):
        """Handle UI interaction events by converting them to Nova Sonic events"""
        if not self.nova_client:
            logger.error("No Nova Sonic client available for handling UI interaction")
            return

        if interaction_data.get("type") == "button_click":
            # Generate unique IDs for this interaction
            prompt_name = self.nova_client.prompt_name  # Use the existing prompt name
            content_name = f"button_click_{str(uuid.uuid4())}"
            
            # Send sequence of events to Nova Sonic
            events = [
                {
                    "event": {
                        "contentStart": {
                            "promptName": prompt_name,
                            "contentName": content_name,
                            "type": "TEXT",
                            "interactive": True,
                            "role": "USER",
                            "textInputConfiguration": {
                                "mediaType": "text/plain"
                            }
                        }
                    }
                },
                {
                    "event": {
                        "textInput": {
                            "promptName": prompt_name,
                            "contentName": content_name,
                            "content": "The user clicked a button. Please acknowledge this action and respond both in text and speech."
                        }
                    }
                },
                {
                    "event": {
                        "contentEnd": {
                            "promptName": prompt_name,
                            "contentName": content_name
                        }
                    }
                }
            ]
            
            # Send events to Nova Sonic
            for event in events:
                try:
                    await self.nova_client.send_event(json.dumps(event))
                    logger.info(f"Sent UI interaction event to Nova Sonic: {event}")
                except Exception as e:
                    logger.error(f"Error sending event to Nova Sonic: {e}")
                    logger.exception(e)

# Load configuration from environment
SAVE_DEBUG_AUDIO = os.getenv('SAVE_DEBUG_AUDIO', 'false').lower() == 'true'
REQUIRE_LOGIN = os.getenv('REQUIRE_LOGIN', 'false').lower() == 'true'
COGNITO_DOMAIN = os.getenv('COGNITO_DOMAIN')
CLIENT_ID = os.getenv('CLIENT_ID')
REDIRECT_URI = os.getenv('REDIRECT_URI')

manager = ConnectionManager(save_debug_audio=SAVE_DEBUG_AUDIO)

# Cognito Authentication (only if login is required)
if REQUIRE_LOGIN:
    @app.get("/callback")
    async def callback(request: Request, code: Optional[str] = None, error: Optional[str] = None):
        print("callback:%s" % request.query_params)
        auth_code = request.query_params.get('code')
        if not auth_code:
            raise HTTPException(status_code=400, detail="No authorization code received")
        
        # Validate auth_code format to prevent injection attacks
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', auth_code) or len(auth_code) > 512:
            raise HTTPException(status_code=400, detail="Invalid authorization code format")

        # Exchange code for tokens
        token_url = f"https://{COGNITO_DOMAIN}/oauth2/token"
        token_data = {
            'grant_type': 'authorization_code',
            'client_id': CLIENT_ID,
            'code': auth_code
            ,'redirect_uri': REDIRECT_URI
        }
        print("token_url=%s" % token_url)
        print("token_data=%s" % token_data)
        response = requests.post(token_url, data=token_data)
        print(response)
        print(response.text)
        r=json.loads(response.text)   
        id_token=r['id_token']
        r=get_iam_oidc_token(id_token)
        #print("r=%s" % r)
        idToken=r['idToken']
        get_qclient(idToken)
        print("q client started")

        if response.status_code != 200:
            error_detail = response.text
            logger.error(f"Token exchange failed: {error_detail}")
            # HTML encode the error to prevent XSS
            import html
            safe_error = html.escape(error_detail)
            return f"<error>{safe_error}</error>"
        else:
             return "<ok/>"

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    logger.info("New WebSocket connection request")
    await manager.connect(websocket)
    
    # Send tool configurations
    tool_configs = manager.nova_client.tool_manager.get_tool_configs() if manager.nova_client else []
    await websocket.send_text(json.dumps({
        "event": {
            "init": {
                "toolConfigs": tool_configs
            }
        }
    }))
    
    # Start processing audio responses and events in background
    process_task = asyncio.create_task(manager.process_audio_responses())
    event_task = asyncio.create_task(manager.process_events())
    
    try:
        while True:
            message = await websocket.receive()
            
            if "bytes" in message:
                # Handle audio data
                audio_data = message["bytes"]
                logger.debug(f"Received audio data of size {len(audio_data)} bytes")
                await manager.receive_audio(audio_data)
            elif "text" in message:
                # Parse the message to check for different event types
                try:
                    event_data = json.loads(message["text"])
                    logger.info(f"Received text message: {event_data}")
                    
                    if "event" in event_data:
                        event = event_data["event"]
                        if "ui_interaction" in event:
                            logger.info(f"Handling UI interaction: {event['ui_interaction']}")
                            await manager.handle_ui_interaction(event["ui_interaction"])
                        elif "toolUse" in event:
                            logger.info(f"Handling tool use: {event['toolUse']}")
                            await manager.handle_tool_use(event_data)
                        elif "textInput" in event:
                            # Add user message to history
                            user_text = event["textInput"].get("content", "")
                            manager.add_history("USER", user_text)
                        else:
                            # Handle string commands
                            command = message["text"]
                            if command == "start_audio":
                                await manager.start_audio()
                            elif command == "stop_audio":
                                await manager.stop_audio()
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                    # Handle as regular command if not JSON
                    command = message["text"]
                    if command == "start_audio":
                        await manager.start_audio()
                    elif command == "stop_audio":
                        await manager.stop_audio()
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        logger.exception(e)  # This will print the full stack trace
    finally:
        logger.info("Cleaning up WebSocket connection")
        process_task.cancel()
        event_task.cancel()
        await manager.disconnect()

for r in app_routers:
    app.include_router(r)

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server")
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', '8000'))
    uvicorn.run(app, host=HOST, port=PORT, log_level="info") 