"""
RAG (Retrieval-Augmented Generation) Engine
Handles retrieving relevant documents and generating responses via LLM
"""
import asyncio
from typing import List, Dict, Any, Optional
from app.services.vector_store import VectorStore
from app.core.config import settings
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama


class RAGEngine:
    """Retrieval-Augmented Generation (RAG) pipeline for fund queries."""

    def __init__(self, db=None):
        """
        Args:
            db: Optional SQLAlchemy session if vector store needs DB access for filtering
        """
        self.vector_store = VectorStore(db=db)
        self.llm = self._initialize_llm()

    def _initialize_llm(self):
        """Initialize LLM based on configuration."""
        if settings.OPENAI_API_KEY:
            return ChatOpenAI(
                model=settings.OPENAI_MODEL,
                temperature=1,
                openai_api_key=settings.OPENAI_API_KEY
            )
        else:
            # Ollama local/remote endpoint
            return Ollama(model="llama2:latest", base_url=settings.LLM_BASE_URL)

    async def query(
        self,
        question: str,
        top_k: int = 3,
        fund_id: Optional[int] = None,
        conversation_history: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Query the RAG system to get context-aware answers.

        Args:
            question: The user's query
            top_k: Number of top documents to retrieve
            fund_id: Optional fund filter for vector search
            conversation_history: Optional list of previous messages for context

        Returns:
            Dict containing:
                - 'answer': Generated answer from LLM
                - 'sources': List of retrieved documents
        """
        # Step 1: Retrieve relevant documents from vector store
        try:
            filter_metadata = {"fund_id": fund_id} if fund_id else None
            relevant_docs = await self.vector_store.similarity_search(
                query=question,
                k=top_k,
                filter_metadata=filter_metadata
            )
        except Exception as e:
            relevant_docs = []
            print(f"[RAGEngine] Error retrieving documents: {e}")

        # Step 2: Build context string for LLM
        context_str = "\n\n".join([
            f"[Source {i+1}]\n{doc['content']}" for i, doc in enumerate(relevant_docs)
        ]) if relevant_docs else "No relevant documents found."

        # Include conversation history if any
        history_str = ""
        if conversation_history:
            for msg in conversation_history:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                history_str += f"{role.capitalize()}: {content}\n"
            history_str += "\n"

        prompt = f"""You are a financial analyst assistant.

{history_str}Use the context below to answer the question.

Context:
{context_str}

Question:
{question}

Answer:"""

        # Step 3: Generate answer using LLM (handle async/sync)
        answer = ""
        try:
            if asyncio.iscoroutinefunction(self.llm.invoke):
                response = await self.llm.invoke(prompt)
            else:
                response = self.llm.invoke(prompt)

            # Extract response content
            if hasattr(response, "content"):
                answer = response.content
            elif isinstance(response, dict) and "content" in response:
                answer = response["content"]
            else:
                answer = str(response)

        except Exception as e:
            answer = f"Error during generation: {e}"

        # Step 4: Return structured result
        return {
            "answer": answer,
            "sources": relevant_docs
        }
