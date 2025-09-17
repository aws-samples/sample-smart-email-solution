from fastapi import APIRouter, Request
import random
import string

router = APIRouter()

@router.post("/api/apps/echo")
async def echo_text(request: Request):
    data = await request.json()
    text = data.get("text", "")
    salt = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
    return {"echoed": f"{text} [{salt}]"} 