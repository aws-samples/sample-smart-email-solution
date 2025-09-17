import datetime
import pytz
from typing import Dict, Any
from ...base.tool import BaseTool
import time

class DateAndTimeTool(BaseTool):
    def __init__(self):
        super().__init__()
        self.config = {
            "name": "getDateAndTimeTool",
            "description": "Get information about the current date and time",
            "shortDescription": "Getting date and time information",
            "schema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }

    async def execute(self, content: Dict[str, Any] = None) -> Dict[str, Any]:
        # Get current date in PST timezone
        pst_timezone = pytz.timezone("America/Los_Angeles")
        pst_date = datetime.datetime.now(pst_timezone)

        # time.sleep(10)
        
        # Model result - detailed data for the model to use
        model_result = {
            "formattedTime": pst_date.strftime("%I:%M %p"),
            "date": pst_date.strftime("%Y-%m-%d"),
            "year": pst_date.year,
            "month": pst_date.month,
            "day": pst_date.day,
            "dayOfWeek": pst_date.strftime("%A").upper(),
            "timezone": "PST"
        }
        
        # UI result - formatted for human display using card
        ui_result = {
            "type": "card",
            "content": {
                "title": "Current Date & Time",
                "description": "Current time in Pacific Time Zone",
                "details": {
                    "Date": pst_date.strftime("%A, %B %d, %Y"),
                    "Time": pst_date.strftime("%I:%M %p"),
                    "Time Zone": "Pacific Time (PST/PDT)",
                    "Day of Week": pst_date.strftime("%A"),
                    "Month": pst_date.strftime("%B"),
                    "Year": str(pst_date.year)
                },
                "image": "https://images.unsplash.com/photo-1501139083538-0139583c060f?q=80&w=2940&auto=format&fit=crop",
                "imageAlt": "Sundial representing time",
                "footer": {
                    "text": f"Last updated: {pst_date.strftime('%I:%M:%S %p')}",
                    "action": {
                        "text": "Refresh",
                        "url": "#"
                    }
                }
            }
        }
        
        return self.format_response(model_result, ui_result) 