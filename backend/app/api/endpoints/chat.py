"""
Chat API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any
import uuid
from datetime import datetime
from app.db.session import get_db
from app.core.config import settings
from app.schemas.chat import (
    ChatQueryRequest,
    ChatQueryResponse,
    ConversationCreate,
    Conversation,
    ChatMessage
)
from app.services.query_engine import QueryEngine
from app.services.rag_engine import RAGEngine

router = APIRouter()

# In-memory conversation storage (replace with Redis/DB in production)
conversations: Dict[str, Dict[str, Any]] = {}


@router.post("/query", response_model=ChatQueryResponse)
async def process_chat_query(
    request: ChatQueryRequest,
    db: Session = Depends(get_db)
) -> ChatQueryResponse:
    """
    Process a chat query using RAGEngine.

    Steps:
    1. Retrieve top documents from vector store
    2. Build context string
    3. Optionally perform SQL-based calculation if question matches known metrics
    4. Generate answer using LLM
    """

    # 2️⃣ Query RAG engine
    rag_engine = RAGEngine(db=db)  # Inject DB if needed for metadata filters
    response = await rag_engine.query(
        question=request.query,
        top_k=settings.TOP_K_RESULTS,
        fund_id=request.fund_id
    )

    # 3️⃣ Update conversation history
    if request.conversation_id:
        if request.conversation_id not in conversations:
            conversations[request.conversation_id] = {
                "fund_id": request.fund_id,
                "messages": [],
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        # Append messages
        conversations[request.conversation_id]["messages"].extend([
            {"role": "user", "content": request.query, "timestamp": datetime.utcnow()},
            {"role": "assistant", "content": response["answer"], "timestamp": datetime.utcnow()}
        ])
        conversations[request.conversation_id]["updated_at"] = datetime.utcnow()

    # 4️⃣ Return structured response
    return ChatQueryResponse(
        answer=response.get("answer", ""),
        sources=response.get("sources", [])
    )


@router.post("/conversations", response_model=Conversation)
async def create_conversation(request: ConversationCreate):
    """Create a new conversation"""
    conversation_id = str(uuid.uuid4())
    
    conversations[conversation_id] = {
        "fund_id": request.fund_id,
        "messages": [],
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    
    return Conversation(
        conversation_id=conversation_id,
        fund_id=request.fund_id,
        messages=[],
        created_at=conversations[conversation_id]["created_at"],
        updated_at=conversations[conversation_id]["updated_at"]
    )


@router.get("/conversations/{conversation_id}", response_model=Conversation)
async def get_conversation(conversation_id: str):
    """Get conversation history"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")
    
    conv = conversations[conversation_id]
    
    return Conversation(
        conversation_id=conversation_id,
        fund_id=conv["fund_id"],
        messages=[ChatMessage(**msg) for msg in conv["messages"]],
        created_at=conv["created_at"],
        updated_at=conv["updated_at"]
    )


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str):
    """Delete a conversation"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")
    
    del conversations[conversation_id]
    
    return {"message": "Conversation deleted successfully"}
