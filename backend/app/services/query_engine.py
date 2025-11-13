"""
QueryEngine: orchestrator that classifies intent, optionally computes metrics,
retrieves RAG answers, and returns combined structured response.
"""
from typing import Dict, Any, List, Optional
from app.services.rag_engine import RAGEngine
from app.services.metrics_calculator import MetricsCalculator
from app.db.session import SessionLocal

class QueryEngine:
    def __init__(self, db=None):
        self.db = db or SessionLocal()
        self.rag = RAGEngine(db=self.db)
        self.metrics = MetricsCalculator(self.db)

    def _classify_intent(self, query: str) -> str:
        q = query.lower()
        calc_keywords = ["calculate", "what is the", "dpi", "irr", "tvpi", "rvpi", "pic", "paid-in"]
        if any(k in q for k in calc_keywords):
            return "calculation"
        ret_keywords = ["show", "list", "find", "when", "which", "how many"]
        if any(k in q for k in ret_keywords):
            return "retrieval"
        return "general"

    async def process_query(
        self,
        query: str,
        fund_id: Optional[int] = None,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        top_k: int = None
    ) -> Dict[str, Any]:
        intent = self._classify_intent(query)

        if intent == "calculation" and fund_id is not None:
            # compute metrics and return
            metrics = self.metrics.calculate_all_metrics(fund_id)
            # optionally still ask rag for textual context
            rag_res = await self.rag.query(query, fund_id=fund_id, conversation_history=conversation_history, top_k=(top_k or 3))
            return {
                "answer": rag_res.get("answer"),
                "sources": rag_res.get("sources"),
                "metrics": metrics
            }

        # else general retrieval via RAG
        rag_res = await self.rag.query(query, fund_id=fund_id, conversation_history=conversation_history, top_k=(top_k or 3))
        return {
            "answer": rag_res.get("answer"),
            "sources": rag_res.get("sources"),
            "metrics": None
        }
