"""
Improved Document processing service.
Processes PDF/Excel/CSV files:
- Parse tables
- Auto-detect & extract capital calls, distributions, adjustments
- Normalize rows
- Save to database
- Optional vector embeddings for semantic search
"""

import io
import asyncio
import traceback
from typing import Dict, Any, List
import pandas as pd
import re
import json
from app.core.config import settings
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama

from app.services.table_parser import TableParser
from app.services.vector_store import VectorStore
from app.db.session import SessionLocal
from app.models.transaction import CapitalCall, Distribution, Adjustment


class DocumentProcessor:
    def __init__(self):
        self.table_parser = TableParser()
        self.vector_store = VectorStore()
        self.llm = self._init_llm()

    def _init_llm(self):
        if settings.OPENAI_API_KEY:
            return ChatOpenAI(model=settings.OPENAI_MODEL, temperature=1, openai_api_key=settings.OPENAI_API_KEY)
        return Ollama(model="llama2:latest", base_url=settings.LLM_BASE_URL)

    # ===============================================================
    # MAIN ENTRYPOINT
    # ===============================================================
    async def process_document(
        self,
        file_bytes: bytes,
        file_type: str,
        fund_id: int,
        document_id: int
    ) -> Dict[str, Any]:

        result = {
            "status": "processing",
            "progress": 0,
            "error": None
        }

        db = SessionLocal()

        try:
            if not file_bytes:
                raise ValueError("File is empty or unreadable")
            result["progress"] = 5

            # -------------------------------------------------------
            # 1. Parse tables into normalized rows
            # -------------------------------------------------------
            tables = self.table_parser.parse(file_bytes, file_type)
            result["progress"] = 30

            if not tables:
                print("[WARN] No tables detected.")
            else:
                print(f"[INFO] Parsed {len(tables)} tables.")

            result["progress"] = 55

            # -------------------------------------------------------
            # 2. Embed text for semantic search
            # -------------------------------------------------------
            text_content = self._extract_text(file_bytes, file_type)
            if text_content:
                await self.vector_store.add_document(
                    content=text_content,
                    metadata={"document_id": document_id, "fund_id": fund_id}
                )
            result["progress"] = 75

            # -------------------------------------------------------
            # 3. extract structured JSON from LLM and save to DB
            # -------------------------------------------------------

            # extract structured JSON from LLM
            parsed_transactions = await self._extract_transactions(text_content)

            # save
            self._save_transactions(db, parsed_transactions, fund_id)
            result["progress"] = 95

            result["status"] = "completed"
            result["progress"] = 100

        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            print("[ERROR] Document processing failed:", e)
            traceback.print_exc()

        finally:
            db.close()

        return result

    # ===============================================================
    # TEXT EXTRACTION
    # ===============================================================
    def _extract_text(self, file_bytes: bytes, file_type: str) -> str:
        if file_type in ["txt", "csv"]:
            return file_bytes.decode("utf-8", errors="ignore")

        if file_type in ["xlsx", "xls"]:
            xls = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
            text = ""
            for sheet, df in xls.items():
                text += f"\nSheet: {sheet}\n"
                text += df.fillna("").to_csv(index=False)
            return text

        if file_type == "pdf":
            try:
                import pdfplumber
                text = ""
                with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                    for page in pdf.pages:
                        text += (page.extract_text() or "") + "\n"
                return text
            except:
                return "PDF text extraction unavailable."

        return ""

    # ===============================================================
    # EXTRACT + CLASSIFY TRANSACTIONS
    # ===============================================================
    async def _extract_transactions(self, text: str) -> Dict[str, Any]:
        PROMPT_TEMPLATE = """
You are a structured data extraction engine.  
Extract ONLY the structured tables from the following fund performance text.

Return the output as a JSON object with EXACTLY these keys:
- "capital_calls": array of capital call objects
- "distributions": array of distribution objects
- "adjustments": array of adjustment objects

Use these strict schemas:

capital_calls:
{
  "call_date": "YYYY-MM-DD",
  "call_type": string or null,
  "amount": number,
  "description": string or null
}

distributions:
{
  "distribution_date": "YYYY-MM-DD",
  "distribution_type": string,
  "is_recallable": boolean,
  "amount": number,
  "description": string or null
}

adjustments:
{
  "adjustment_date": "YYYY-MM-DD",
  "adjustment_type": string,
  "category": string or null,
  "amount": number,
  "is_contribution_adjustment": boolean,
  "description": string or null
}

Rules:
- ONLY extract rows that appear under the relevant sections: “Capital Calls”, “Distributions”, “Adjustments”.
- Convert number strings like "$5,000,000" into numeric format: 5000000.
- Convert "Yes"/"No" into true/false.
- If a field does not exist in the text, return null.
- NEVER invent data that does not exist in the document.
- Ignore all other sections (Performance Summary, Key Definitions, narrative text).

Return only valid JSON.
Do not wrap the JSON in markdown.

Here is the document text:
<<<DOCUMENT>>>
"""
        # call LLM (sync or async)
        prompt = PROMPT_TEMPLATE.replace("<<<DOCUMENT>>>", text)
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
            print("LLM returned invalid JSON:", answer)
            raise ValueError("Failed to parse JSON from LLM: " + str(e))
        
        parsed_json = json.loads(answer)

        # pastikan keys selalu ada
        return {
            "capital_calls": parsed_json.get("capital_calls", []),
            "distributions": parsed_json.get("distributions", []),
            "adjustments": parsed_json.get("adjustments", []),
        }

    # ===============================================================
    # DB INSERT LOGIC
    # ===============================================================
    def _save_transactions(self, db, data: Dict[str, Any], fund_id: int):

        # CAPITAL CALLS
        for row in data.get("capital_calls", []):
            db.add(CapitalCall(
                call_date=row.get("call_date"),
                call_type=row.get("call_type"),
                amount=row.get("amount", 0),
                description=row.get("description"),
                fund_id=fund_id
            ))

        # DISTRIBUTIONS
        for row in data.get("distributions", []):
            db.add(Distribution(
                distribution_date=row.get("distribution_date"),
                distribution_type=row.get("distribution_type"),
                is_recallable=row.get("is_recallable", False),
                amount=row.get("amount", 0),
                description=row.get("description"),
                fund_id=fund_id
            ))

        # ADJUSTMENTS
        for row in data.get("adjustments", []):
            db.add(Adjustment(
                adjustment_date=row.get("adjustment_date"),
                adjustment_type=row.get("adjustment_type"),
                category=row.get("category"),
                is_contribution_adjustment=row.get("is_contribution_adjustment", False),
                amount=row.get("amount", 0),
                description=row.get("description"),
                fund_id=fund_id
            ))

        db.commit()
