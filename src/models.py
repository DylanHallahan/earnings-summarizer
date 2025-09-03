from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List

@dataclass
class CompanyInfo:
    symbol: str
    name: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[str] = None

@dataclass
class EarningsCallData:
    company_symbol: str
    company_name: str
    quarter: str
    year: int
    call_date: datetime
    transcript_url: Optional[str] = None
    raw_transcript: Optional[str] = None
    processed_transcript: Optional[str] = None

@dataclass
class SummaryResult:
    executive_summary: str
    key_metrics: Dict[str, Any]
    financial_highlights: List[str]
    guidance: str
    risks_concerns: List[str]
    sentiment_score: float
    confidence_score: float