#!/usr/bin/env python3

import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from config.database import init_database, test_connection
from src.api_client import DefeatBetaClient, test_api_connection
from config.settings import settings
from loguru import logger

def setup_project():
    """Initialize project setup"""
    logger.info("Setting up Earnings Summarizer...")
    
    # Create necessary directories
    settings.DATA_DIR.mkdir(exist_ok=True)
    (settings.DATA_DIR / "raw").mkdir(exist_ok=True)
    settings.LOGS_DIR.mkdir(exist_ok=True)
    
    # Test database connection
    logger.info("Testing database connection...")
    if test_connection():
        logger.info("Database connection successful")
        init_database()
    else:
        logger.error("Database connection failed")
        return False
    
    # Test API connection
    logger.info("Testing Defeat Beta API connection...")
    if test_api_connection():
        logger.info("API connection successful")
    else:
        logger.error("API connection failed")
        return False
    
    logger.info("Project setup complete!")
    return True

def fetch_company_transcripts(symbol: str):
    """Fetch earnings transcripts for a specific company"""
    client = DefeatBetaClient()
    
    logger.info(f"Fetching earnings transcripts for {symbol}...")
    
    try:
        # Get company info
        company = client.get_company_info(symbol)
        if not company:
            logger.error(f"Company {symbol} not found")
            return
        
        # Get transcripts
        transcripts = client.get_earnings_transcripts(symbol)
        
        if transcripts:
            logger.info(f"Retrieved {len(transcripts)} transcripts for {symbol}")
            
            # Show the latest transcript preview
            latest = transcripts[-1] if transcripts else None
            if latest:
                logger.info(f"Latest: {latest.quarter} {latest.year}")
                logger.info(f"Transcript length: {len(latest.raw_transcript)} characters")
                
                # Preview first 500 characters
                preview = latest.raw_transcript[:500] + "..." if len(latest.raw_transcript) > 500 else latest.raw_transcript
                logger.info(f"Preview: {preview}")
                
        else:
            logger.warning(f"No transcripts found for {symbol}")
            
    except Exception as e:
        logger.error(f"Error fetching transcripts for {symbol}: {e}")

def main():
    # Set up logging
    logger.add(
        settings.LOGS_DIR / "earnings_summarizer.log",
        rotation="10 MB",
        retention="1 month",
        level="INFO"
    )
    
    parser = argparse.ArgumentParser(description="Earnings Call Summarizer")
    parser.add_argument("--setup", action="store_true", help="Setup project and test connections")
    parser.add_argument("--symbol", type=str, help="Fetch transcripts for specific company symbol")
    parser.add_argument("--test", action="store_true", help="Test API and database connections")
    
    args = parser.parse_args()
    
    if args.setup:
        setup_project()
    elif args.symbol:
        fetch_company_transcripts(args.symbol.upper())
    elif args.test:
        logger.info("Testing connections...")
        if test_connection():
            logger.info("Database connection: OK")
        else:
            logger.error("Database connection: Failed")
            
        if test_api_connection():
            logger.info("API connection: OK") 
        else:
            logger.error("API connection: Failed")
    else:
        logger.info("Use --help to see available commands")
        logger.info("Example usage:")
        logger.info("  python main.py --setup          # Initial setup")
        logger.info("  python main.py --test           # Test connections") 
        logger.info("  python main.py --symbol TSLA    # Get Tesla transcripts")

if __name__ == "__main__":
    main()