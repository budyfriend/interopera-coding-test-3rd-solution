"""
RAG Engine: uses VectorStore + LLM + optional SQL metrics to answer queries.
Supports:
- top_k and similarity_threshold (from settings or defaults)
- conversation_history for multi-turn
"""
import asyncio
from typing import List, Dict, Any, Optional
from app.core.config import settings
from app.services.vector_store import VectorStore
from app.services.metrics_calculator import MetricsCalculator
from app.db.session import SessionLocal
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama

TOP_K_DEFAULT = getattr(settings, "RAG_TOP_K_RESULTS", 5)
SIMILARITY_THRESHOLD = getattr(settings, "SIMILARITY_THRESHOLD", 0.70)

class RAGEngine:
    def __init__(self, db=None):
        self.db = db or SessionLocal()
        self.vector_store = VectorStore(self.db)
        self.metrics = MetricsCalculator(self.db)
        self.llm = self._init_llm()

    def _init_llm(self):
        if settings.OPENAI_API_KEY:
            return ChatOpenAI(model=settings.OPENAI_MODEL, temperature=1, openai_api_key=settings.OPENAI_API_KEY)
        return Ollama(model="llama2:latest", base_url=settings.LLM_BASE_URL)

    async def query(
        self,
        question: str,
        fund_id: Optional[int] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        top_k: int = TOP_K_DEFAULT,
        similarity_threshold: float = SIMILARITY_THRESHOLD
    ) -> Dict[str, Any]:

        # 1) retrieve candidates
        candidates = await self.vector_store.similarity_search(question, k=top_k, filter_metadata={"fund_id": fund_id} if fund_id else None)

        # 2) filter by similarity threshold
        filtered = [d for d in candidates if (d.get("score") is not None and d["score"] >= similarity_threshold)]
        # fallback: if none pass threshold, keep top 1-3 depending on top_k
        if not filtered:
            filtered = candidates[: min(3, len(candidates))]

        # 3) prepare context
        context_text = "\n\n".join([f"[Source {i+1} | score={d.get('score')}]\n{d['content']}" for i,d in enumerate(filtered)]) if filtered else "No relevant documents found."

        # 4) optional SQL-driven quick answers for metric queries
        sql_answer = None
        if fund_id is not None:
            qlower = question.lower()
            if any(k in qlower for k in ["dpi", "paid-in", "paid in capital", "pic"]):
                metrics = self.metrics.calculate_all_metrics(fund_id)
                sql_answer = f"DPI: {metrics.get('dpi')}, PIC: {metrics.get('pic')}, Total distributions: {metrics.get('total_distributions')}"
            elif "irr" in qlower:
                irr = self.metrics.calculate_irr(fund_id)
                if irr is not None:
                    sql_answer = f"IRR: {irr}%"
        if sql_answer:
            return {"answer": sql_answer, "sources": filtered, "metrics": metrics}

        # 5) build prompt with conversation history
        history_text = ""
        if conversation_history:
            history_text = "\n".join([f"{m.get('role', 'user').capitalize()}: {m.get('content','')}" for m in conversation_history])

        prompt = f"""You are a financial analyst assistant specialized in private equity fund reporting.
Conversation History:
{history_text}

Context:
{context_text}

Question:
{question}

Provide a concise, source-cited answer. Cite sources like [Source 1]. If numbers are present, show calculation steps when possible.
"""

        # 6) call LLM (sync or async)
        try:
            if asyncio.iscoroutinefunction(self.llm.invoke):
                resp = await self.llm.invoke(prompt)
            else:
                resp = self.llm.invoke(prompt)

            if hasattr(resp, "content"):
                answer = resp.content
            elif isinstance(resp, dict) and "content" in resp:
                answer = resp["content"]
            else:
                answer = str(resp)
        except Exception as e:
            answer = f"LLM generation error: {e}"

        return {"answer": answer, "sources": filtered}
