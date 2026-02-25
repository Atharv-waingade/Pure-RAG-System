from pydantic import BaseModel
from typing import List

class Page(BaseModel):
    url: str
    content: str

class SyncRequest(BaseModel):
    client_id: str
    pages: List[Page]

class ChatRequest(BaseModel):
    client_id: str
    user_query: str