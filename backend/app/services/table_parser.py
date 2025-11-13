"""
Table parsing service for document ingestion.
Normalizes tables into a consistent list-of-dicts format:
[ { "sheet": "...", "rows": [ {col: val}, ... ] }, ... ]
Supports: pdf, xlsx/xls, csv, txt
"""
from typing import List, Dict, Any
import io
import re
import pandas as pd

class TableParser:
    def __init__(self):
        pass

    def parse(self, file_bytes: bytes, file_type: str) -> List[Dict[str, Any]]:
        ft = (file_type or "").lower().lstrip(".")
        try:
            if ft in ("xlsx", "xls"):
                return self._parse_excel(file_bytes)
            if ft == "csv":
                return self._parse_csv(file_bytes)
            if ft == "txt":
                return self._parse_text(file_bytes)
            if ft == "pdf":
                # For PDF, caller (DocumentProcessor) will use pdfplumber-specific parser.
                # But keep a placeholder to allow fallback.
                return []
        except Exception as e:
            print(f"[TableParser] parse error for {file_type}: {e}")
        return []

    def _parse_excel(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        tables: List[Dict[str, Any]] = []
        data = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
        for sheet_name, df in data.items():
            rows = df.fillna("").to_dict(orient="records")
            tables.append({"sheet": sheet_name, "rows": rows})
        return tables

    def _parse_csv(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        df = pd.read_csv(io.BytesIO(file_bytes))
        rows = df.fillna("").to_dict(orient="records")
        return [{"sheet": "csv", "rows": rows}]

    def _parse_text(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        text = file_bytes.decode("utf-8", errors="ignore")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            return []
        # split on two-or-more spaces or tabs
        split_lines = [re.split(r"\s{2,}|\t", ln.strip()) for ln in lines]
        headers = split_lines[0]
        rows = []
        for row in split_lines[1:]:
            if len(row) != len(headers):
                # try pad/truncate
                if len(row) < len(headers):
                    row += [""] * (len(headers) - len(row))
                else:
                    row = row[: len(headers)]
            rows.append({headers[i]: row[i] for i in range(len(headers))})
        return [{"sheet": "text", "rows": rows}]
