#!/usr/bin/env python3

import sys
import requests
import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from loguru import logger
from typing import Dict, Any, List, Optional

sys.path.append('.')
from config.database import EarningsCall, Summary, Company, engine
from config.settings import settings

class OllamaClient:
    def __init__(self, host: str = "http://localhost:11434"):
        self.host = host
        self.model = "llama3.1:8b"
    
    def is_available(self) -> bool:
        """Check if Ollama is running and model is available"""
        try:
            response = requests.get(f"{self.host}/api/tags")
            if response.status_code == 200:
                models = response.json().get("models", [])
                model_names = [model["name"] for model in models]
                if self.model in model_names:
                    logger.info(f"Ollama available with {self.model}")
                    return True
                else:
                    logger.warning(f"Model {self.model} not found. Available models: {model_names}")
                    return False
            return False
        except Exception as e:
            logger.error(f"Ollama not available: {e}")
            return False
    
    def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        """Generate text using Ollama"""
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_predict": max_tokens,
                    "temperature": 0.3,
                    "top_p": 0.9
                }
            }
            
            response = requests.post(
                f"{self.host}/api/generate",
                json=payload,
                timeout=120
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("response", "")
            else:
                logger.error(f"Ollama API error: {response.status_code} - {response.text}")
                return ""
                
        except Exception as e:
            logger.error(f"Error generating with Ollama: {e}")
            return ""

class EarningsSummarizer:
    def __init__(self):
        self.ollama = OllamaClient(settings.OLLAMA_HOST)
        self.session = sessionmaker(bind=engine)()
    
    def get_company_info(self, symbol: str) -> Optional[Company]:
        """Get company info from database"""
        return self.session.query(Company).filter(Company.symbol == symbol).first()
    
    def chunk_transcript(self, transcript: str, max_chunk_size: int = 4000) -> List[str]:
        """Split transcript into manageable chunks"""
        words = transcript.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            word_length = len(word) + 1
            
            if current_length + word_length > max_chunk_size and current_chunk:
                chunks.append(" ".join(current_chunk))
                overlap_size = min(100, len(current_chunk) // 4)
                current_chunk = current_chunk[-overlap_size:]
                current_length = sum(len(w) + 1 for w in current_chunk)
            
            current_chunk.append(word)
            current_length += word_length
        
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        
        return chunks
    
    def generate_executive_summary(self, earnings_call: EarningsCall) -> str:
        """Generate comprehensive executive summary"""
        company_info = self.get_company_info(earnings_call.company_symbol)
        company_context = f"{company_info.name} ({earnings_call.company_symbol})" if company_info else earnings_call.company_symbol
        
        chunks = self.chunk_transcript(earnings_call.raw_transcript, max_chunk_size=5000)
        
        # Process multiple chunks (limit to first 8 chunks for performance)
        chunks_to_process = chunks[:8]  # Limit to avoid very long processing times
        chunk_summaries = []
        
        for i, chunk in enumerate(chunks_to_process):
            logger.info(f"Processing executive summary chunk {i+1}/{len(chunks_to_process)} (of {len(chunks)} total)")
            
            prompt = f"""
Summarize the key strategic and business insights from this section of a {company_context} {earnings_call.quarter} {earnings_call.year} earnings call:

{chunk}

Focus on:
- Strategic initiatives and business developments
- Management commentary and outlook
- Operational changes or market insights
- Forward-looking statements

Provide a concise summary of the main strategic points in this section:
"""
            summary = self.ollama.generate(prompt, max_tokens=300)
            if summary:
                chunk_summaries.append(summary)
        
        combined = "\n\n".join(chunk_summaries)
        
        prompt = f"""
These are summaries from different sections of a {company_context} {earnings_call.quarter} {earnings_call.year} earnings call. Create a cohesive executive summary:

{combined}

Create a comprehensive executive summary covering:
- Overall business performance themes and strategic direction
- Key initiatives, investments, and operational changes
- Management's outlook and forward-looking commentary
- Market opportunities, challenges, and competitive positioning
- Significant business developments or strategic shifts

Write a professional, analytical summary in 450-550 words that provides strategic business intelligence:
"""
        
        return self.ollama.generate(prompt, max_tokens=600)
    
    def extract_forward_guidance(self, earnings_call: EarningsCall) -> str:
        """Extract forward-looking guidance and strategic initiatives"""
        company_info = self.get_company_info(earnings_call.company_symbol)
        company_context = f"{company_info.name} ({earnings_call.company_symbol})" if company_info else earnings_call.company_symbol
        
        # Look for forward-looking content
        transcript = earnings_call.raw_transcript
        forward_keywords = settings.FORWARD_GUIDANCE_KEYWORDS
        
        # Extract sections that likely contain forward guidance
        lines = transcript.split('\n')
        guidance_sections = []
        
        for i, line in enumerate(lines):
            if any(keyword in line.lower() for keyword in forward_keywords):
                # Include context around forward-looking statements
                start = max(0, i-2)
                end = min(len(lines), i+3)
                guidance_sections.extend(lines[start:end])
        
        guidance_text = '\n'.join(guidance_sections[:3000]) if guidance_sections else transcript[:3000]
        
        prompt = f"""
Extract and analyze the forward-looking guidance and strategic direction from this {company_context} {earnings_call.quarter} {earnings_call.year} earnings call:

{guidance_text}

Focus on synthesizing:
1. **Financial Guidance**: Revenue expectations, margin outlook, growth targets, and financial projections
2. **Strategic Initiatives**: New investments, expansion plans, product launches, and business development
3. **Market Outlook**: Management's view on industry trends, competitive dynamics, and market conditions  
4. **Operational Plans**: Efficiency programs, capacity changes, technology investments, and operational improvements
5. **Risk Factors**: Challenges, headwinds, or uncertainties management highlighted

Provide a structured analysis that combines both quantitative guidance (when mentioned) and strategic direction. Write in a professional tone suitable for investment analysis.

Provide a comprehensive forward guidance analysis in 350-450 words:
"""
        
        return self.ollama.generate(prompt, max_tokens=500)
    
    def determine_outlook_sentiment(self, earnings_call: EarningsCall) -> str:
        """Determine overall sentiment in one word"""
        company_info = self.get_company_info(earnings_call.company_symbol)
        company_context = f"{company_info.name} ({earnings_call.company_symbol})" if company_info else earnings_call.company_symbol
        
        # Extract key sentiment-bearing sections
        transcript = earnings_call.raw_transcript.lower()
        
        # Count sentiment indicators
        positive_count = sum(1 for word in settings.SENTIMENT_KEYWORDS['positive'] if word in transcript)
        negative_count = sum(1 for word in settings.SENTIMENT_KEYWORDS['negative'] if word in transcript)
        neutral_count = sum(1 for word in settings.SENTIMENT_KEYWORDS['neutral'] if word in transcript)
        
        # Also analyze management statements for tone
        management_sections = []
        lines = earnings_call.raw_transcript.split('\n')
        
        for line in lines:
            if any(title in line.lower() for title in settings.MANAGEMENT_TITLES):
                management_sections.append(line)
        
        # Take key management statements for analysis
        management_text = '\n'.join(management_sections[:10]) if management_sections else transcript[:1000]
        
        prompt = f"""
Analyze the overall sentiment and outlook from this {company_context} {earnings_call.quarter} {earnings_call.year} earnings call.

Key management statements:
{management_text}

Based on management tone, forward guidance, and overall messaging, determine the predominant sentiment.

Respond with EXACTLY ONE WORD from these options:
- Positive (optimistic, confident, strong outlook)
- Negative (cautious, concerned, challenging outlook)  
- Neutral (balanced, steady, mixed signals)
- Cautious (careful optimism, measured outlook)
- Bullish (very positive, aggressive growth outlook)
- Bearish (pessimistic, defensive outlook)

One word only:
"""
        
        result = self.ollama.generate(prompt, max_tokens=10).strip()
        
        # Ensure we get a single word response
        if result:
            # Extract just the first word and clean it
            first_word = result.split()[0].strip('.,!?').title()
            # Validate it's a reasonable sentiment word
            valid_sentiments = ['Positive', 'Negative', 'Neutral', 'Cautious', 'Bullish', 'Bearish', 
                              'Optimistic', 'Pessimistic', 'Mixed', 'Strong', 'Weak', 'Confident']
            if first_word in valid_sentiments:
                return first_word
        
        # Fallback: use keyword counting
        if positive_count > negative_count and positive_count > neutral_count:
            return "Positive"
        elif negative_count > positive_count and negative_count > neutral_count:
            return "Negative" 
        else:
            return "Neutral"
    
    def summarize_earnings_call(self, earnings_call: EarningsCall) -> Dict[str, str]:
        """Generate comprehensive summaries for an earnings call"""
        logger.info(f"Summarizing {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
        
        if not earnings_call.raw_transcript:
            logger.warning("No transcript available for summarization")
            return {}
        
        summaries = {}
        
        # Generate the 3 new summary types
        logger.info("Generating executive summary...")
        summaries['executive'] = self.generate_executive_summary(earnings_call)
        
        logger.info("Extracting forward guidance...")
        summaries['forward_guidance'] = self.extract_forward_guidance(earnings_call)
        
        logger.info("Determining outlook sentiment...")
        summaries['outlook_sentiment'] = self.determine_outlook_sentiment(earnings_call)
        
        return summaries
    
    def save_summaries(self, earnings_call: EarningsCall, summaries: Dict[str, str]):
        """Save summaries to database"""
        try:
            # Remove existing summaries for this call
            self.session.query(Summary).filter(
                Summary.earnings_call_id == earnings_call.id
            ).delete()
            
            # Save new summaries
            for summary_type, content in summaries.items():
                if content and content.strip():
                    summary = Summary(
                        earnings_call_id=earnings_call.id,
                        summary_type=summary_type,
                        content=content.strip(),
                        confidence_score=0.8,
                        created_at=datetime.utcnow()
                    )
                    self.session.add(summary)
            
            self.session.commit()
            logger.info(f"Saved {len(summaries)} summaries for {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
            
        except Exception as e:
            logger.error(f"Error saving summaries: {e}")
            self.session.rollback()
    
    def process_earnings_calls(self, symbol: str = None, limit: int = None, 
                             year: int = None, quarter: str = None):
        """Process earnings calls for summarization"""
        query = self.session.query(EarningsCall)
        
        # If no symbol specified, default to all companies (backward compatible)
        if symbol:
            query = query.filter(EarningsCall.company_symbol == symbol.upper())
        
        query = query.order_by(EarningsCall.year.desc(), EarningsCall.quarter.desc())
        
        if year:
            query = query.filter(EarningsCall.year == year)
        if quarter:
            query = query.filter(EarningsCall.quarter == quarter.upper())
        if limit:
            query = query.limit(limit)
        
        earnings_calls = query.all()
        
        if not earnings_calls:
            logger.warning("No earnings calls found matching criteria")
            return
        
        logger.info(f"Processing {len(earnings_calls)} earnings calls for summarization")
        
        for earnings_call in earnings_calls:
            # Check if summaries already exist
            existing_summaries = self.session.query(Summary).filter(
                Summary.earnings_call_id == earnings_call.id
            ).count()
            
            if existing_summaries > 0:
                logger.info(f"Skipping {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter} - summaries already exist")
                continue
            
            try:
                summaries = self.summarize_earnings_call(earnings_call)
                if summaries:
                    self.save_summaries(earnings_call, summaries)
                    logger.info(f"Completed {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
                    logger.info(f"Generated summaries: {list(summaries.keys())}")
                else:
                    logger.warning(f"No summaries generated for {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
            
            except Exception as e:
                logger.error(f"Error processing {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}: {e}")
                continue
    
    def list_available_companies(self) -> List[str]:
        """List all companies with earnings call data"""
        result = self.session.query(EarningsCall.company_symbol).distinct().all()
        return [row[0] for row in result]
    
    def close(self):
        """Close database session"""
        self.session.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate earnings call summaries")
    parser.add_argument("--symbol", type=str, help="Stock symbol to process (e.g., DLTR, AAPL)")
    parser.add_argument("--limit", type=int, help="Limit number of calls to process")
    parser.add_argument("--year", type=int, help="Process specific year")
    parser.add_argument("--quarter", type=str, help="Process specific quarter (Q1, Q2, Q3, Q4)")
    parser.add_argument("--test", action="store_true", help="Test Ollama connection only")
    parser.add_argument("--list", action="store_true", help="List available companies")
    
    args = parser.parse_args()
    
    # Setup logging
    logger.add(
        settings.LOGS_DIR / "earnings_summarizer.log",
        rotation="10 MB",
        retention="1 month",
        level="INFO"
    )
    
    summarizer = EarningsSummarizer()
    
    try:
        if args.test:
            if summarizer.ollama.is_available():
                logger.info("✓ Ollama test successful - ready for summarization")
                print("✓ Ollama connection successful")
            else:
                logger.error("✗ Ollama test failed - check if Ollama is running and llama3.1:8b is installed")
                print("✗ Ollama connection failed")
            return
        
        if args.list:
            companies = summarizer.list_available_companies()
            if companies:
                print("Available companies with earnings call data:")
                for company in sorted(companies):
                    print(f"  - {company}")
            else:
                print("No companies found in database")
            return
        
        # Check Ollama availability
        if not summarizer.ollama.is_available():
            logger.error("Ollama not available. Please ensure Ollama is running with llama3.1:8b model")
            print("Error: Ollama not available. Please ensure Ollama is running.")
            return
        
        # Process earnings calls
        summarizer.process_earnings_calls(
            symbol=args.symbol,
            limit=args.limit,
            year=args.year,
            quarter=args.quarter
        )
        
        print("✅ Earnings summarization completed successfully")
        
    finally:
        summarizer.close()
    
    logger.info("Earnings summarization completed")

if __name__ == "__main__":
    main()