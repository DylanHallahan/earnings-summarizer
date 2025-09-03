#!/usr/bin/env python3

import sys
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from defeatbeta_api.data.ticker import Ticker
from loguru import logger
import argparse
from typing import Optional

sys.path.append('.')
from config.database import Base, EarningsCall, StockPrice, Company, engine
from config.settings import settings

def setup_database():
    """Create all database tables"""
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully")

def add_or_update_company(session, symbol: str, name: str = None) -> Company:
    """Add or update company information"""
    company = session.query(Company).filter(Company.symbol == symbol).first()
    
    if not company:
        company = Company(
            symbol=symbol,
            name=name or f"{symbol} Inc.",
            created_at=datetime.utcnow()
        )
        session.add(company)
        logger.info(f"Added new company: {symbol}")
    else:
        if name and company.name != name:
            company.name = name
            logger.info(f"Updated company name: {symbol} -> {name}")
    
    session.commit()
    return company

def clean_transcript_text(text: str) -> str:
    """Clean transcript text for database storage"""
    if not text:
        return ""
    
    try:
        # Convert to ASCII, ignoring problematic characters
        cleaned = str(text).encode('ascii', errors='ignore').decode('ascii')
        # Remove excessive whitespace
        cleaned = ' '.join(cleaned.split())
        return cleaned
    except Exception as e:
        logger.warning(f"Error cleaning transcript text: {e}")
        return str(text)[:10000]  # Fallback: truncate if all else fails

def load_price_data(symbol: str, start_year: int = 2017) -> bool:
    """Load stock price data for a symbol from specified year onwards"""
    logger.info(f"Loading {symbol} price data from {start_year}...")
    
    try:
        ticker = Ticker(symbol)
        price_data = ticker.price()
        
        if price_data.empty:
            logger.warning(f"No price data found for {symbol}")
            return False
        
        # Filter for specified year onwards
        price_data['report_date'] = pd.to_datetime(price_data['report_date'])
        filtered_data = price_data[price_data['report_date'] >= f'{start_year}-01-01'].copy()
        
        logger.info(f"Found {len(filtered_data)} price records from {start_year} onwards")
        
        if filtered_data.empty:
            logger.warning(f"No price data found for {symbol} from {start_year} onwards")
            return False
        
        # Create database session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Clear existing price data for this symbol
            session.query(StockPrice).filter(StockPrice.symbol == symbol).delete()
            session.commit()
            
            # Insert price data
            price_records = []
            for _, row in filtered_data.iterrows():
                try:
                    price_record = StockPrice(
                        symbol=symbol,
                        date=row['report_date'].date(),
                        open_price=float(row['open']) if pd.notna(row['open']) else None,
                        close_price=float(row['close']) if pd.notna(row['close']) else None,
                        high_price=float(row['high']) if pd.notna(row['high']) else None,
                        low_price=float(row['low']) if pd.notna(row['low']) else None,
                        volume=int(row['volume']) if pd.notna(row['volume']) else None
                    )
                    price_records.append(price_record)
                except Exception as e:
                    logger.warning(f"Skipping price record due to error: {e}")
                    continue
            
            if price_records:
                # Batch insert
                session.add_all(price_records)
                session.commit()
                
                logger.info(f"Successfully loaded {len(price_records)} {symbol} price records")
                return True
            else:
                logger.warning(f"No valid price records found for {symbol}")
                return False
        
        finally:
            session.close()
                
    except Exception as e:
        logger.error(f"Error loading {symbol} price data: {e}")
        return False

def load_earnings_transcripts(symbol: str, start_year: int = 2017) -> bool:
    """Load earnings transcripts for a symbol from specified year onwards"""
    logger.info(f"Loading {symbol} earnings transcripts from {start_year}...")
    
    try:
        ticker = Ticker(symbol)
        transcripts_api = ticker.earning_call_transcripts()
        transcripts_list = transcripts_api.get_transcripts_list()
        
        if transcripts_list.empty:
            logger.warning(f"No earnings transcripts found for {symbol}")
            return False
        
        # Filter for specified year onwards
        filtered_transcripts = transcripts_list[transcripts_list['fiscal_year'] >= start_year].copy()
        
        logger.info(f"Found {len(filtered_transcripts)} earnings calls from {start_year} onwards")
        
        if filtered_transcripts.empty:
            logger.warning(f"No earnings transcripts found for {symbol} from {start_year} onwards")
            return False
        
        # Create database session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Clear existing earnings data for this symbol
            session.query(EarningsCall).filter(EarningsCall.company_symbol == symbol).delete()
            session.commit()
            
            earnings_records = []
            
            for _, row in filtered_transcripts.iterrows():
                logger.info(f"Processing {symbol} {row['fiscal_year']} Q{row['fiscal_quarter']}...")
                
                try:
                    # Get the actual transcript content
                    transcript_data = transcripts_api.get_transcript(
                        row['fiscal_year'], 
                        row['fiscal_quarter']
                    )
                    
                    if transcript_data.empty:
                        logger.warning(f"  - No transcript content found")
                        continue
                    
                    # Combine all transcript content
                    full_transcript = ""
                    for _, content_row in transcript_data.iterrows():
                        speaker = clean_transcript_text(str(content_row['speaker']))
                        content = clean_transcript_text(str(content_row['content']))
                        full_transcript += f"{speaker}: {content}\n\n"
                    
                    if not full_transcript.strip():
                        logger.warning(f"  - Empty transcript after cleaning")
                        continue
                    
                    earnings_call = EarningsCall(
                        company_symbol=symbol,
                        company_name=f"{symbol} Inc.",  # Could be enhanced with real company names
                        quarter=f"Q{row['fiscal_quarter']}",
                        year=int(row['fiscal_year']),
                        call_date=pd.to_datetime(row['report_date']),
                        transcript_url=None,
                        raw_transcript=full_transcript,
                        word_count=len(full_transcript.split()),
                        processing_status='loaded',
                        created_at=datetime.utcnow()
                    )
                    
                    earnings_records.append(earnings_call)
                    logger.info(f"  - Loaded transcript ({len(full_transcript)} chars, {earnings_call.word_count} words)")
                    
                except Exception as e:
                    logger.warning(f"  - Failed to load transcript: {e}")
                    continue
            
            if earnings_records:
                # Batch insert earnings records
                session.add_all(earnings_records)
                session.commit()
                
                logger.info(f"Successfully loaded {len(earnings_records)} {symbol} earnings call transcripts")
                return True
            else:
                logger.warning(f"No valid earnings transcripts found for {symbol}")
                return False
        
        finally:
            session.close()
                
    except Exception as e:
        logger.error(f"Error loading {symbol} earnings transcripts: {e}")
        return False

def test_symbol_availability(symbol: str) -> bool:
    """Test if a symbol has data available"""
    try:
        ticker = Ticker(symbol)
        price_data = ticker.price()
        if not price_data.empty:
            logger.info(f"✓ {symbol} - data available")
            return True
        else:
            logger.warning(f"✗ {symbol} - no data available")
            return False
    except Exception as e:
        logger.warning(f"✗ {symbol} - error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Load earnings and price data for any ticker")
    parser.add_argument("symbol", nargs="?", default="DLTR", help="Stock symbol to load (default: DLTR)")
    parser.add_argument("--start-year", type=int, default=2017, help="Starting year for data (default: 2017)")
    parser.add_argument("--no-prices", action="store_true", help="Skip price data loading")
    parser.add_argument("--no-earnings", action="store_true", help="Skip earnings transcript loading")
    parser.add_argument("--test-symbol", type=str, help="Test if a symbol is available")
    
    args = parser.parse_args()
    
    # Set up logging
    logger.add(
        settings.LOGS_DIR / "data_loader.log",
        rotation="10 MB",
        retention="1 month",
        level="INFO"
    )
    
    logger.info(f"Starting data loading process...")
    
    # Create directories
    settings.DATA_DIR.mkdir(exist_ok=True)
    settings.LOGS_DIR.mkdir(exist_ok=True)
    
    # Setup database
    setup_database()
    
    # Test symbol availability if requested
    if args.test_symbol:
        if test_symbol_availability(args.test_symbol.upper()):
            print(f"✓ {args.test_symbol.upper()} is available for loading")
        else:
            print(f"✗ {args.test_symbol.upper()} is not available")
        return
    
    symbol = args.symbol.upper()
    logger.info(f"Processing symbol: {symbol}")
    
    # Add company to database
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        add_or_update_company(session, symbol)
    finally:
        session.close()
    
    success = True
    
    # Load price data
    if not args.no_prices:
        if load_price_data(symbol, args.start_year):
            logger.info(f"✓ {symbol} price data loaded successfully")
        else:
            logger.error(f"✗ Failed to load {symbol} price data")
            success = False
    
    # Load earnings transcripts
    if not args.no_earnings:
        if load_earnings_transcripts(symbol, args.start_year):
            logger.info(f"✓ {symbol} earnings transcripts loaded successfully")
        else:
            logger.error(f"✗ Failed to load {symbol} earnings transcripts")
            success = False
    
    if success:
        logger.info(f"🎉 {symbol} data loading completed!")
        print(f"\n✅ Successfully loaded data for {symbol}")
        print(f"📊 To generate summaries, run:")
        print(f"   python earnings_summarizer.py --symbol {symbol}")
    else:
        logger.error(f"❌ {symbol} data loading failed")

if __name__ == "__main__":
    main()