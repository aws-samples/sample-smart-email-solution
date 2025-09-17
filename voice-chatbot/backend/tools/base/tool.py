from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

class BaseTool(ABC):
    def __init__(self):
        self.name: str = self.__class__.__name__
        self.config: Dict[str, Any] = {
            "name": "",
            "description": "",
            "shortDescription": "",
            "schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }

    @abstractmethod
    async def execute(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the tool with the given content.
        Must return a dict with 'model_result' and 'ui_result' keys.
        """
        pass

    def get_config(self) -> Dict[str, Any]:
        """Return the tool configuration"""
        return self.config

    def format_response(self, model_result: Dict[str, Any], ui_result: Dict[str, Any]) -> Dict[str, Any]:
        """Format the response in the standard format expected by the system"""
        return {
            "model_result": model_result,
            "ui_result": ui_result
        } 