#!/usr/bin/env python3

import sys
import json
import os
import time
from datetime import datetime
from pathlib import Path

# Add the earnings-summarizer path
sys.path.append('/home/aiuser/earnings-summarizer')

def update_progress_file(symbol, step, current, details, status="processing"):
    """Update progress in a JSON file that the API can read"""
    progress_dir = Path('/tmp/ticker_setup_progress')
    progress_dir.mkdir(exist_ok=True)
    
    progress_file = progress_dir / f"{symbol}.json"
    progress_data = {
        "status": status,
        "current_step": step,
        "completed_steps": current,
        "total_steps": 7,
        "details": details,
        "updated_at": datetime.now().isoformat()
    }
    
    with open(progress_file, 'w') as f:
        json.dump(progress_data, f)
    
    print(f"[{current}/6] {step}: {details}")

from src.api_client import DefeatBetaClient
from config.database import SessionLocal, Company, EarningsCall, StockPrice

def setup_ticker(symbol):
    """Comprehensive setup for a new ticker"""
    client = DefeatBetaClient()
    results = {}
    
    try:
        update_progress_file(symbol, "Verifying ticker symbol", 1, f"Checking if {symbol} exists...")
        
        # Step 1: Verify ticker and get company info
        company_info = client.get_company_info(symbol)
        if not company_info:
            update_progress_file(symbol, "Ticker verification failed", 1, f"Ticker {symbol} not found or invalid", "failed")
            return {"success": False, "error": f"Invalid ticker symbol: {symbol}"}
        results["company_info"] = True
        update_progress_file(symbol, "Ticker verified", 1, f"{symbol} is a valid ticker symbol")
        
        # Step 2: Add to companies table
        update_progress_file(symbol, "Adding to database", 2, "Adding company to database...")
        db = SessionLocal()
        existing = db.query(Company).filter(Company.symbol == symbol).first()
        if not existing:
            new_company = Company(
                symbol=symbol,
                name=f"{symbol} Corporation",
                created_at=datetime.utcnow()
            )
            db.add(new_company)
            db.commit()
            update_progress_file(symbol, "Added to database", 2, "Company added to database")
        else:
            update_progress_file(symbol, "Database updated", 2, "Company already exists in database")
        db.close()
        results["database"] = True
        
        # Step 3: Load earnings call transcripts
        update_progress_file(symbol, "Loading earnings transcripts", 3, "Fetching earnings call data...")
        transcripts = client.get_earnings_transcripts(symbol)
        if transcripts:
            db = SessionLocal()
            saved_count = 0
            for transcript in transcripts:
                existing_call = db.query(EarningsCall).filter(
                    EarningsCall.company_symbol == transcript.company_symbol,
                    EarningsCall.quarter == transcript.quarter,
                    EarningsCall.year == transcript.year
                ).first()
                
                if not existing_call:
                    # Clean text to handle encoding issues
                    def clean_text(text):
                        if text is None:
                            return None
                        text = str(text)
                        # Replace problematic Unicode characters
                        text = text.replace(''', "'").replace(''', "'")
                        text = text.replace('"', '"').replace('"', '"')
                        text = text.replace('–', '-').replace('—', '-')
                        text = text.replace('…', '...')
                        # Remove any remaining non-ASCII characters
                        text = text.encode('ascii', 'ignore').decode('ascii')
                        return text
                    
                    earnings_call = EarningsCall(
                        company_symbol=transcript.company_symbol,
                        company_name=clean_text(transcript.company_name),
                        quarter=transcript.quarter,
                        year=transcript.year,
                        call_date=transcript.call_date,
                        transcript_url=clean_text(transcript.transcript_url),
                        raw_transcript=clean_text(transcript.raw_transcript),
                        created_at=datetime.utcnow()
                    )
                    db.add(earnings_call)
                    saved_count += 1
            
            db.commit()
            db.close()
            results["earnings"] = saved_count
            update_progress_file(symbol, "Earnings transcripts loaded", 3, f"Saved {saved_count} earnings call transcripts")
        else:
            results["earnings"] = 0
            update_progress_file(symbol, "No earnings found", 3, "No earnings transcripts available")
        
        # Step 4: Load financial news
        update_progress_file(symbol, "Loading financial news", 4, "Fetching recent news articles...")
        news_articles = client.get_financial_news(symbol, limit=50)
        if news_articles:
            client.save_news_to_db(news_articles)
            results["news"] = len(news_articles)
            update_progress_file(symbol, "Financial news loaded", 4, f"Saved {len(news_articles)} news articles")
        else:
            results["news"] = 0
            update_progress_file(symbol, "No news found", 4, "No news articles available")
        
        # Step 5: Load stock prices
        update_progress_file(symbol, "Loading stock prices", 5, "Fetching historical stock data...")
        try:
            import pandas as pd
            from defeatbeta_api.data.ticker import Ticker
            
            ticker = Ticker(symbol)
            price_data = ticker.price()
            
            if not price_data.empty:
                db = SessionLocal()
                saved_prices = 0
                
                for date, row in price_data.tail(730).iterrows():  # Last 2 years
                    existing_price = db.query(StockPrice).filter(
                        StockPrice.symbol == symbol,
                        StockPrice.date == date.date()
                    ).first()
                    
                    if not existing_price:
                        stock_price = StockPrice(
                            symbol=symbol,
                            date=date.date(),
                            open_price=float(row['open']) if pd.notna(row['open']) else None,
                            close_price=float(row['close']) if pd.notna(row['close']) else None,
                            high_price=float(row['high']) if pd.notna(row['high']) else None,
                            low_price=float(row['low']) if pd.notna(row['low']) else None,
                            volume=int(row['volume']) if pd.notna(row['volume']) else None
                        )
                        db.add(stock_price)
                        saved_prices += 1
                
                db.commit()
                db.close()
                results["stock_prices"] = saved_prices
                update_progress_file(symbol, "Stock prices loaded", 5, f"Saved {saved_prices} stock price records")
            else:
                results["stock_prices"] = 0
                update_progress_file(symbol, "No stock prices found", 5, "No stock price data available")
        except Exception as e:
            results["stock_prices"] = f"Error: {str(e)}"
            update_progress_file(symbol, "Stock prices error", 5, f"Stock prices error: {e}")
        
        # Step 6: Load financial metrics
        update_progress_file(symbol, "Loading financial metrics", 6, "Fetching financial ratios and metrics...")
        try:
            metrics = client.get_financial_metrics(symbol)
            if metrics:
                client.save_financial_metrics_to_db(symbol, metrics)
                results["financial_metrics"] = True
                update_progress_file(symbol, "Financial metrics loaded", 6, "Saved financial metrics")
            else:
                results["financial_metrics"] = False
                update_progress_file(symbol, "No financial metrics found", 6, "No financial metrics available")
        except Exception as e:
            results["financial_metrics"] = f"Error: {str(e)}"
            update_progress_file(symbol, "Financial metrics error", 6, f"Financial metrics error: {e}")
        
        # Step 7: Generate AI earnings summaries for last 2 years
        update_progress_file(symbol, "Generating AI summaries", 7, "Creating AI summaries for earnings calls...")
        try:
            from earnings_summarizer import EarningsSummarizer
            from datetime import datetime
            
            current_year = datetime.now().year
            two_years_ago = current_year - 2
            
            summarizer = EarningsSummarizer()
            
            # Check if Ollama is available
            if summarizer.ollama.is_available():
                # Get earnings calls for the last 2 years
                db = SessionLocal()
                recent_calls = db.query(EarningsCall).filter(
                    EarningsCall.company_symbol == symbol,
                    EarningsCall.year >= two_years_ago
                ).order_by(EarningsCall.year.desc(), EarningsCall.quarter.desc()).all()
                db.close()
                
                if recent_calls:
                    summarized_count = 0
                    for call in recent_calls:
                        # Check if summaries already exist
                        db = SessionLocal()
                        existing_summaries = db.query(Summary).filter(
                            Summary.earnings_call_id == call.id
                        ).count()
                        db.close()
                        
                        if existing_summaries == 0:
                            summaries = summarizer.summarize_earnings_call(call)
                            if summaries:
                                summarizer.save_summaries(call, summaries)
                                summarized_count += 1
                    
                    results["ai_summaries"] = summarized_count
                    if summarized_count > 0:
                        update_progress_file(symbol, "AI summaries generated", 7, f"Generated AI summaries for {summarized_count} earnings calls")
                    else:
                        update_progress_file(symbol, "AI summaries skipped", 7, "All earnings calls already have summaries")
                else:
                    results["ai_summaries"] = 0
                    update_progress_file(symbol, "No earnings for summaries", 7, "No earnings calls found for summarization")
            else:
                results["ai_summaries"] = "Ollama unavailable"
                update_progress_file(symbol, "AI summaries skipped", 7, "Ollama not available for summarization")
            
            summarizer.close()
        except Exception as e:
            results["ai_summaries"] = f"Error: {str(e)}"
            update_progress_file(symbol, "AI summaries error", 7, f"AI summaries error: {e}")
        
        update_progress_file(symbol, "Setup completed successfully!", 7, f"{symbol} has been added with all available data and AI summaries", "completed")
        print("FINAL_RESULT:", json.dumps({"success": True, "results": results}))
        return {"success": True, "results": results}
        
    except Exception as e:
        update_progress_file(symbol, "Setup failed", 1, f"Error: {str(e)}", "failed")
        print("FINAL_RESULT:", json.dumps({"success": False, "error": str(e)}))
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python setup_ticker.py <SYMBOL>")
        sys.exit(1)
    
    symbol = sys.argv[1].upper()
    setup_ticker(symbol)