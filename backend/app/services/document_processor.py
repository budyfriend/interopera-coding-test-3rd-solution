"""
Document processing service.
Processes PDF/Excel/CSV files:
- Parse tables
- Extract transactions (capital calls, distributions, adjustments)
- Save to database
- Optional vector embeddings for semantic search
"""

import io
import traceback
from typing import Dict, Any, List
import pandas as pd

from app.services.table_parser import TableParser
from app.services.vector_store import VectorStore  # jika embedding aktif
from app.db.session import SessionLocal
from app.models.transaction import CapitalCall, Distribution, Adjustment


class DocumentProcessor:
    def __init__(self):
        self.table_parser = TableParser()
        self.vector_store = VectorStore()  # pastikan sudah ada vector_store.py

    async def process_document(
        self,
        file_bytes: bytes,
        file_type: str,
        fund_id: int,
        document_id: int
    ) -> Dict[str, Any]:
        """
        Process document end-to-end and return status, progress, error
        """
        result = {
            "status": "processing",
            "progress": 0,
            "error": None
        }

        db = SessionLocal()

        try:
            # 1️⃣ Validasi file
            if not file_bytes:
                raise ValueError("File is empty or unreadable.")
            result["progress"] = 5

            # 2️⃣ Parse tables
            tables = self.table_parser.parse(file_bytes, file_type)
            result["progress"] = 30

            if not tables:
                print("[WARN] No tables found in document.")
            else:
                print(f"[INFO] {len(tables)} tables parsed.")

            # 3️⃣ Extract transactions
            parsed_transactions = self._extract_transactions(tables, fund_id)
            result["progress"] = 50

            # 4️⃣ Save transactions to database
            self._save_transactions(db, parsed_transactions, document_id)
            result["progress"] = 70

            # 5️⃣ Optional: Embed text content for semantic search
            text_content = self._extract_text(file_bytes, file_type)
            if text_content:
                await self.vector_store.add_document(
                    content=text_content,
                    metadata={"document_id": document_id, "fund_id": fund_id}
                )
            result["progress"] = 90

            # 6️⃣ Done
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

    def _extract_text(self, file_bytes: bytes, file_type: str) -> str:
        """Extract raw text from file for embedding"""
        if file_type in ["txt", "csv"]:
            return file_bytes.decode("utf-8", errors="ignore")
        elif file_type in ["xlsx", "xls"]:
            xls = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
            content = ""
            for sheet, df in xls.items():
                content += f"\nSheet: {sheet}\n"
                content += df.fillna("").to_csv(index=False)
            return content
        elif file_type == "pdf":
            # Bisa pakai pdfplumber atau PyMuPDF di sini
            try:
                import pdfplumber
                content = ""
                with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                    for page in pdf.pages:
                        content += page.extract_text() or ""
                return content
            except ImportError:
                return "PDF content extraction requires pdfplumber"
        else:
            return ""

    def _extract_transactions(self, tables: List[Dict], fund_id: int) -> List[Dict]:
        """
        Convert parsed tables into structured transactions
        Example output:
        [
            {"type": "capital_call", "date": ..., "amount": ..., "fund_id": fund_id, "description": ...},
            ...
        ]
        """
        transactions = []

        for table in tables:
            rows = table.get("rows", [])
            for row in rows:
                # Tentukan jenis transaksi berdasarkan header kolom
                if "capital" in (k.lower() for k in row.keys()):
                    transactions.append({
                        "type": "capital_call",
                        "date": row.get("date"),
                        "amount": row.get("amount"),
                        "fund_id": fund_id,
                        "description": row.get("description")
                    })
                elif "distribution" in (k.lower() for k in row.keys()):
                    transactions.append({
                        "type": "distribution",
                        "date": row.get("date"),
                        "amount": row.get("amount"),
                        "fund_id": fund_id,
                        "description": row.get("description")
                    })
                elif "adjustment" in (k.lower() for k in row.keys()):
                    transactions.append({
                        "type": "adjustment",
                        "date": row.get("date"),
                        "amount": row.get("amount"),
                        "fund_id": fund_id,
                        "description": row.get("description")
                    })
        return transactions

    def _save_transactions(self, db, transactions: List[Dict], document_id: int):
        """Save transactions into DB tables"""
        for tr in transactions:
            if tr["type"] == "capital_call":
                db.add(CapitalCall(
                    call_date=tr["date"],
                    amount=tr["amount"],
                    fund_id=tr["fund_id"],
                    description=tr.get("description"),
                    document_id=document_id
                ))
            elif tr["type"] == "distribution":
                db.add(Distribution(
                    distribution_date=tr["date"],
                    amount=tr["amount"],
                    fund_id=tr["fund_id"],
                    description=tr.get("description"),
                    document_id=document_id
                ))
            elif tr["type"] == "adjustment":
                db.add(Adjustment(
                    adjustment_date=tr["date"],
                    amount=tr["amount"],
                    fund_id=tr["fund_id"],
                    adjustment_type=tr.get("type"),
                    description=tr.get("description"),
                    document_id=document_id
                ))
        db.commit()
