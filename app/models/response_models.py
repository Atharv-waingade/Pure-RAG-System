from pydantic import BaseModel
from typing import List

class SyncResponse(BaseModel):
    status: str
    pages_updated: int

class ChatResponse(BaseModel):
    response_text: str
    confidence_score: float
    sources: List[str]