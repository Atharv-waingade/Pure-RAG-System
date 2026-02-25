from fastapi import APIRouter, HTTPException
from app.models.request_models import SyncRequest
from app.models.response_models import SyncResponse
from app.core.retriever import RetrieverEngine
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/sync", response_model=SyncResponse)
def sync_knowledge_base(request: SyncRequest):
    try:
        engine = RetrieverEngine(request.client_id)
        count = engine.sync_content([p.dict() for p in request.pages])
        return SyncResponse(status="indexed", pages_updated=count)
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))