from pydantic import BaseModel
from fastapi import APIRouter
from app.models.models import IncomingMessage, QueryRequest
from app.services.query_orchestrator import get_orchestrator



router = APIRouter(tags=["query"])



@router.post("/query")
async def direct_query(req: QueryRequest):
    """
    POST { "question": "..." }  →  returns structured response + raw result.
    Useful for testing and integration with other systems.
    """
    msg = IncomingMessage(
        platform=req.platform,
        user_id=req.user_id,
        text=req.question,
    )
    orchestrator = get_orchestrator()
    response_text = await orchestrator.handle(msg)
    return {"response": response_text, "question": req.question}