import os
import json
from typing import Dict, Any, List
from .base import ToolRegistry
from .categories.utility import DateAndTimeTool, EmailTool

class ToolManager:
    def __init__(self, amazon_q=None):
        self.registry = ToolRegistry()
        self.amazon_q = amazon_q
        self._initialize_registry()

    def _initialize_registry(self) -> None:
        """Initialize the tool registry with all available tools"""
        # Register all tools
        self.registry.register_tools([
            # Utility tools
            DateAndTimeTool(),
            EmailTool(amazon_q=self.amazon_q),
        ])

    async def execute_tool(self, tool_name: str, content: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool by name"""
        try:
            # All tools are now in the registry
            return await self.registry.execute_tool(tool_name, content)
        except KeyError:
            raise KeyError(f"Tool '{tool_name}' not found")

    def get_tool_configs(self) -> List[Dict[str, Any]]:
        """Get all tool configurations formatted for Nova Sonic"""
        configs = self.registry.get_tool_configs()
        # Format each tool config according to Nova Sonic's expected format
        return [
            {
                "toolSpec": {
                    "name": config["name"],
                    "description": config["description"],
                    "inputSchema": {
                        "json": json.dumps(config["schema"])
                    }
                }
            }
            for config in configs
        ] 