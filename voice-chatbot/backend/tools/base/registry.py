from typing import Dict, Any, Type, List
from .tool import BaseTool

class ToolRegistry:
    def __init__(self):
        self._tools: Dict[str, BaseTool] = {}

    def register_tool(self, tool_instance: BaseTool) -> None:
        """Register a tool instance"""
        self._tools[tool_instance.config["name"]] = tool_instance

    def register_tools(self, tools: List[BaseTool]) -> None:
        """Register multiple tool instances"""
        for tool in tools:
            self.register_tool(tool)

    async def execute_tool(self, tool_name: str, content: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool by name"""
        if tool_name not in self._tools:
            raise KeyError(f"Tool '{tool_name}' not found")
        
        tool = self._tools[tool_name]
        return await tool.execute(content)

    def get_tool_configs(self) -> List[Dict[str, Any]]:
        """Get all tool configurations"""
        return [tool.get_config() for tool in self._tools.values()]

    def get_tool(self, tool_name: str) -> BaseTool:
        """Get a tool instance by name"""
        return self._tools.get(tool_name) 