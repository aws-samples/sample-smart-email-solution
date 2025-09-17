# Nova Sonic Agentic Chatbot

This is a demo application showcasing Nova Sonic capabilities using a Next.js frontend with a FastAPI backend. The project demonstrates real-time audio features, and dynamic UI rendering in a dynamic interface.

## 🚀 Demo Features

- **Modular Tool System**: Demonstrates extensible tool architecture
- **Real-time Audio**: Audio capture and playback capabilities
- **UI Components**: Supports dynamic rendering of tool outputs (text, cards, images, videos, PDFs)
- **Frontend**: Built with Next.js, TypeScript, and Tailwind CSS
- **Async Backend**: FastAPI-powered backend with async tool execution
- **Barge-in Support**: For natural voice interactions

## 🖥️ Interface

The interface features two main control buttons:

- **Power Button**: Establishes WebSocket connection with the backend
- **Mic Button**: Starts/stops the Nova Sonic session for voice interaction

**Usage Flow:**
1. Click **Power** to connect to backend
2. Click **Mic** to begin voice conversation with Nova Sonic
3. Speak naturally - tool outputs appear in the display canvas

## 📁 Project Structure

```
sample-nova-sonic-agentic-chatbot
├── backend/                 # Python FastAPI backend
│   ├── api/                # API endpoints and apps
│   ├── tools/              # Modular tool system
│   │   ├── base/           # Base classes and registry
│   │   ├── categories/     # Tool categories
│   │   │   ├── utility/    # Utility tools
│   │   └── tool_manager.py # Tool registration and management
│   ├── main.py             # FastAPI application entry point
│   └── requirements.txt    # Python dependencies
├── frontend/               # Next.js frontend
│   ├── app/               # Next.js app directory
│   ├── components/        # React components
│   │   ├── ui/           # Base UI components
│   │   ├── tool-outputs/ # Tool result components
│   │   └── apps/         # Application components
│   ├── lib/              # Utility functions
│   ├── public/           # Static assets
│   └── package.json      # Node.js dependencies
└── README.md             # Project documentation
```

## 🛠️ Setup and Installation

### Prerequisites

- **Python 3.8+**
- **Node.js 18+**
- **npm or yarn**

### Backend Setup

1. **Navigate to backend directory:**
   ```bash
   cd sample-nova-sonic-agentic-chatbot/backend
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the backend server:**
   ```bash
   python main.py
   ```

### Debug Audio Configuration

The application includes an optional debug audio recording feature for development and troubleshooting purposes.

**Default Behavior:** Audio recording is **disabled** by default.

**To enable debug audio recording:**
```bash
export SAVE_DEBUG_AUDIO=true
python main.py
```

**To explicitly disable debug audio recording:**
```bash
export SAVE_DEBUG_AUDIO=false
python main.py
```

**Audio files are saved to:**
- **Input audio:** `backend/debug_audio/input_YYYYMMDD_HHMMSS.wav` (16kHz, 16-bit)
- **Output audio:** `backend/debug_audio/output_YYYYMMDD_HHMMSS.wav` (24kHz, 16-bit)

**Note:** Debug audio files are created per session and automatically timestamped. This feature is useful for debugging audio quality issues or analyzing conversation flows.

### Frontend Setup

1. **Navigate to frontend directory:**
   ```bash
   cd sample-nova-sonic-agentic-chatbot/frontend
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Run the development server:**
   ```bash
   npm run dev
   ```

4. **Access the application:**
   Open [http://localhost:3000](http://localhost:3000) in your browser

## 🏗️ Architecture

### Backend Architecture

- **FastAPI Framework**: High-performance async web framework
- **Tool Registry**: Dynamic tool discovery and registration system
- **Category-based Organization**: Tools organized by functionality
- **Async Execution**: Non-blocking tool execution with proper error handling

### Frontend Architecture

- **Next.js 14**: React framework with App Router
- **TypeScript**: Type-safe development
- **Tailwind CSS**: Utility-first CSS framework
- **Component-based**: Modular UI components for tool outputs
- **Real-time Features**: Audio capture and playback capabilities

### Tool System

The tool system demonstrates a modular architecture where each tool:
- Inherits from `BaseTool` base class
- Defines its own configuration schema
- Implements async execution logic
- Returns **dual results**: `model_result` and `ui_result`

**Dual Result Architecture:**
- **`model_result`**: Sent back to Nova Sonic for context and conversation flow
- **`ui_result`**: Sent to frontend with `type` field determining how content is displayed (cards, images, text, etc.)

## 🔧 Demo Tools

### Utility Tools
- **DateAndTimeTool**: Current date and time information (Ask chatbot: What is the date today?)
- **EmailTool**: Connects to Q Business application to query about email knowledge (e.g. via MS Exchange Server) 

## 📚 Adding New Tools

### 1. Create Tool Class

Create a new tool in the appropriate category folder:

```python
from typing import Dict, Any
from ...base.tool import BaseTool

class MyNewTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.config = {
            "name": "myNewTool",
            "description": "Tool description for Nova Sonic",
            "schema": {
                "type": "object",
                "properties": {
                    "param1": {
                        "type": "string",
                        "description": "Parameter description"
                    }
                },
                "required": ["param1"]
            }
        }

    async def execute(self, content: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Tool logic here
            result = {"data": "processed"}
            
            return self.format_response(
                model_result=result,              # Goes to Nova Sonic
                ui_result={                       # Goes to frontend UI
                    "type": "card",               # UI component type
                    "content": {
                        "title": "Result",
                        "description": "Tool output"
                    }
                }
            )
        except Exception as e:
            return self.format_response(
                {"error": str(e)},
                {"type": "text", "content": {"title": "Error", "message": str(e)}}
            )
```

### 2. Register Tool

Add your tool to `backend/tools/tool_manager.py`:

```python
from .categories.utility import MyNewTool

# In _initialize_registry method:
self.registry.register_tools([
    MyNewTool(),
    # ... other tools
])
```

### 3. Update Category Exports

Update the category's `__init__.py`:

```python
from .my_new_tool import MyNewTool
__all__ = ['MyNewTool']
```

## 🎨 UI Components

The system has some pre-built UI component types that can bee hooked up to tool responses (via ui_result):

- **Text**: Simple text output
- **Card**: Rich card with title, description, and details
- **Image**: Image display with metadata
- **Video**: Embedded video content
- **PDF**: PDF document viewer
