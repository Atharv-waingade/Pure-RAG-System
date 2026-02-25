from fastapi import APIRouter, HTTPException
from app.models.request_models import ChatRequest
from app.models.response_models import ChatResponse
from app.core.retriever import RetrieverEngine
from app.core.formatter import format_step_response

router = APIRouter()

SIMILARITY_THRESHOLD = 0.45

@router.post("/chat", response_model=ChatResponse)
def chat_endpoint(request: ChatRequest):
    engine = RetrieverEngine(request.client_id)

    # 1. Search
    chunks, scores = engine.search(request.user_query, top_k=1)

    # 2. Guard Logic
    if not chunks or scores[0] < SIMILARITY_THRESHOLD:
        return ChatResponse(
            response_text="I couldn’t find that information on this website.",
            confidence_score=scores[0] if scores else 0.0,
            sources=[]
        )

    best_chunk = chunks[0]
    best_score = scores[0]

    # 3. Format
    formatted_text = format_step_response(best_chunk['text'])

    return ChatResponse(
        response_text=formatted_text,
        confidence_score=best_score,
        sources=[best_chunk['url']]
    )