"""
Table parsing service for document ingestion.
Extracts tabular data from documents (PDF, Excel, or text).
"""
import pandas as pd
from typing import List, Dict, Any, Union
import io
import re


class TableParser:
    """Extract tables from uploaded documents."""

    def __init__(self):
        pass

    def parse(self, file_bytes: bytes, file_type: str) -> List[Dict[str, Any]]:
        """
        Parse tables depending on file type.

        Args:
            file_bytes: File content in bytes.
            file_type: MIME type or extension ('pdf', 'xlsx', 'csv', 'txt', etc.)

        Returns:
            List of parsed tables as dictionaries.
        """
        try:
            if file_type in ["xlsx", "xls"]:
                return self._parse_excel(file_bytes)
            elif file_type == "csv":
                return self._parse_csv(file_bytes)
            elif file_type == "txt":
                return self._parse_text(file_bytes)
            else:
                # For PDF or unsupported, fallback to empty
                return []
        except Exception as e:
            print(f"Error parsing {file_type}: {e}")
            return []

    def _parse_excel(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract all sheets as tables."""
        tables = []
        excel_data = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
        for sheet_name, df in excel_data.items():
            tables.append({
                "sheet": sheet_name,
                "rows": df.fillna("").to_dict(orient="records")
            })
        return tables

    def _parse_csv(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract CSV as a single table."""
        df = pd.read_csv(io.BytesIO(file_bytes))
        return [{
            "sheet": "csv_data",
            "rows": df.fillna("").to_dict(orient="records")
        }]

    def _parse_text(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Extract tabular-like text lines (very basic)."""
        text = file_bytes.decode("utf-8", errors="ignore")
        lines = [re.split(r"\s{2,}|\t", line.strip()) for line in text.splitlines() if line.strip()]
        if not lines:
            return []
        headers = lines[0]
        rows = [dict(zip(headers, row)) for row in lines[1:] if len(row) == len(headers)]
        return [{
            "sheet": "text_data",
            "rows": rows
        }]
    
    def parse_table(self, table_data, page_number: int, fund_id: int) -> Dict[str, Any]:
        """
        Parse dan klasifikasikan tabel PDF.
        """
        # contoh deteksi sederhana:
        if any("Capital" in str(cell) for row in table_data for cell in row):
            category = "capital_call"
        elif any("Distribution" in str(cell) for row in table_data for cell in row):
            category = "distribution"
        elif any("Adjustment" in str(cell) for row in table_data for cell in row):
            category = "adjustment"
        else:
            category = "unknown"

        return {
            "fund_id": fund_id,
            "page": page_number,
            "category": category,
            "rows": table_data
        }
