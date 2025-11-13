"""
Vector store service using pgvector (PostgreSQL extension)

Provides:
- add_document(content, metadata)
- similarity_search(query, k, filter_metadata)
- clear(fund_id)
"""
from typing import List, Dict, Any, Optional
import json
import numpy as np
import asyncio
from sqlalchemy import text
from app.core.config import settings
from app.db.session import SessionLocal

# Choose embedding backend wrappers as available in your environment.
# We attempt to use OpenAIEmbeddings or HuggingFaceEmbeddings if present; fallback to dummy.
try:
    from langchain_openai import OpenAIEmbeddings
except Exception:
    OpenAIEmbeddings = None
try:
    from langchain_community.embeddings import HuggingFaceEmbeddings
except Exception:
    HuggingFaceEmbeddings = None

class VectorStore:
    def __init__(self, db=None):
        self.db = db or SessionLocal()
        # choose embedding model and dimension
        if settings.OPENAI_API_KEY and OpenAIEmbeddings is not None:
            self.embeddings = OpenAIEmbeddings(model=settings.OPENAI_EMBEDDING_MODEL, openai_api_key=settings.OPENAI_API_KEY)
            self.dimension = 1536
        elif HuggingFaceEmbeddings is not None:
            self.embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
            self.dimension = 384
        else:
            self.embeddings = None
            self.dimension = getattr(settings, "EMBED_DIM", 384)

        # ensure pgvector extension and table exist
        self._ensure_extension_and_table()

    def _ensure_extension_and_table(self):
        try:
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        except Exception:
            # extension may need superuser; ignore if fails
            pass

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS document_embeddings (
            id SERIAL PRIMARY KEY,
            document_id INTEGER,
            fund_id INTEGER,
            content TEXT NOT NULL,
            embedding VECTOR({self.dimension}),
            metadata JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            self.db.execute(text(create_table_sql))
            # create ivfflat index if not exists (note: may need REINDEX after populate)
            try:
                self.db.execute(text("""
                CREATE INDEX IF NOT EXISTS document_embeddings_embedding_idx
                ON document_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
                """))
            except Exception:
                pass
            self.db.commit()
        except Exception as e:
            print(f"[VectorStore] ensure table error: {e}")
            self.db.rollback()

    async def _compute_embedding(self, text: str) -> np.ndarray:
        # run embedding in thread to avoid blocking event loop
        if self.embeddings is None:
            # dummy random vector (not ideal in production)
            return np.zeros(self.dimension, dtype=np.float32)
        def sync_embed():
            # compatible with different embedding wrappers
            if hasattr(self.embeddings, "embed_query"):
                return self.embeddings.embed_query(text)
            if hasattr(self.embeddings, "encode"):
                return self.embeddings.encode(text)
            # fallback
            return np.zeros(self.dimension, dtype=np.float32)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, sync_embed)
        return np.array(result, dtype=np.float32)

    async def add_document(self, content: str, metadata: Dict[str, Any]):
        try:
            emb = await self._compute_embedding(content)
            emb_str = "[" + ",".join(map(str, emb.tolist())) + "]"
            insert_sql = text("""
                INSERT INTO document_embeddings (document_id, fund_id, content, embedding, metadata)
                VALUES (:document_id, :fund_id, :content, :embedding, :metadata)
            """)
            params = {
                "document_id": metadata.get("document_id"),
                "fund_id": metadata.get("fund_id"),
                "content": content,
                "embedding": emb_str,
                "metadata": json.dumps(metadata)
            }
            self.db.execute(insert_sql, params)
            self.db.commit()
            return True
        except Exception as e:
            print(f"[VectorStore] add_document error: {e}")
            self.db.rollback()
            raise

    async def similarity_search(self, query: str, k: int = 5, filter_metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            qemb = await self._compute_embedding(query)
            emb_str = "[" + ",".join(map(str, qemb.tolist())) + "]"
            params = {"embedding": emb_str, "k": k}
            where_clause = ""
            if filter_metadata:
                conds = []
                for kf, vf in filter_metadata.items():
                    if kf in ("fund_id", "document_id"):
                        conds.append(f"{kf} = :{kf}")
                        params[kf] = vf
                if conds:
                    where_clause = "WHERE " + " AND ".join(conds)

            sql = text(f"""
                SELECT id, document_id, fund_id, content, metadata,
                    1 - (embedding <=> CAST(:embedding AS vector)) AS similarity_score
                FROM document_embeddings
                {where_clause}
                ORDER BY embedding <=> CAST(:embedding AS vector)
                LIMIT :k
            """)
            result = self.db.execute(sql, params)
            rows = result.fetchall()
            out = []
            for r in rows:
                out.append({
                    "id": r[0],
                    "document_id": r[1],
                    "fund_id": r[2],
                    "content": r[3],
                    "metadata": r[4],
                    "score": float(r[5]) if r[5] is not None else None
                })
            return out
        except Exception as e:
            print(f"[VectorStore] similarity_search error: {e}")
            return []

    def clear(self, fund_id: Optional[int] = None):
        try:
            if fund_id:
                self.db.execute(text("DELETE FROM document_embeddings WHERE fund_id = :fund_id"), {"fund_id": fund_id})
            else:
                self.db.execute(text("DELETE FROM document_embeddings"))
            self.db.commit()
        except Exception as e:
            print(f"[VectorStore] clear error: {e}")
            self.db.rollback()
