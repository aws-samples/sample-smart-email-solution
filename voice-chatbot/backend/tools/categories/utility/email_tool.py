import datetime
import pytz
from typing import Dict, Any
from ...base.tool import BaseTool
import time
import json
import os

class EmailTool(BaseTool):
    def __init__(self, amazon_q=None): 
        super().__init__()
        self.amazon_q = amazon_q
        print("EmailTool initialized")

        self.config = {
            "name": "getEmail",
            "description": "Get information about your emails",
            "shortDescription": "Getting details or summaries from my emails via natural language query",
            "schema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "provide the full question in the query parameter"
                    }
                },
                "required": ["query"]
            }
        }

    async def get_queue_chain(self, prompt_input, conversation_id='', parent_message_id=''):
        """
        This method is used to get the answer from the queue chain.
        """
        AMAZON_Q_APP_ID = os.getenv('AMAZON_Q_APP_ID')
        print("get_queue_chain: " + prompt_input + " conversation_id: " + conversation_id + " parent_message_id: " + parent_message_id + "\n")
        if conversation_id != "":
            answer = self.amazon_q.chat_sync(
                applicationId=AMAZON_Q_APP_ID,
                userMessage=prompt_input,
                conversationId=conversation_id,
                parentMessageId=parent_message_id,
            )
        else:
            answer = self.amazon_q.chat_sync(
                applicationId=AMAZON_Q_APP_ID, userMessage=prompt_input
            )
        print("answer=%s" % answer)
        system_message = answer.get("systemMessage", "")
        conversation_id = answer.get("conversationId", "")
        parent_message_id = answer.get("systemMessageId", "")
        result = {
            "answer": system_message,
            "conversationId": conversation_id,
            "parentMessageId": parent_message_id,
        }

        if answer.get("sourceAttributions"):
            attributions = answer["sourceAttributions"]
            valid_attributions = []

            # Generate the answer references extracting citation number,
            # the document title, and if present, the document url
            for attr in attributions:
                title = attr.get("title", "")
                url = attr.get("url", "")
                citation_number = attr.get("citationNumber", "")
                attribution_text = []
                if citation_number:
                    attribution_text.append(f"[{citation_number}]")
                if title:
                    attribution_text.append(f"Title: {title}")
                if url:
                    attribution_text.append(f", URL: {url}")

                valid_attributions.append("".join(attribution_text))

            concatenated_attributions = "\n\n".join(valid_attributions)
            result["references"] = concatenated_attributions

            # Process the citation numbers and insert them into the system message
            citations = {}
            for attr in answer["sourceAttributions"]:
                for segment in attr["textMessageSegments"]:
                    citations[segment["endOffset"]] = attr["citationNumber"]
            offset_citations = sorted(citations.items(), key=lambda x: x[0])
            modified_message = ""
            prev_offset = 0

            for offset, citation_number in offset_citations:
                modified_message += (
                    system_message[prev_offset:offset] + f"[{citation_number}]"
                )
                prev_offset = offset

            modified_message += system_message[prev_offset:]
            
            result["answer"] = modified_message

        return result

    async def execute(self, content: Dict[str, Any] = None) -> Dict[str, Any]:
        query = content.get("query", "")
        
        # Use the moved method to get email information
        if self.amazon_q:
            result = await self.get_queue_chain(query)
            res = result.get("answer", "")
        else:
            res = "Amazon Q client not available"
        
        model_result = {
            "answer": res
        }
        
        # UI result - formatted for human display using card
        ui_result = {
            "type": "text",
            "content": {
                "title": "Email Query Result",
                "message": res
            }
        }
        
        return self.format_response(model_result, ui_result) 