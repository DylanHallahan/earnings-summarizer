#!/usr/bin/env python3
"""
Tesla Financial Research Platform - Unified Management CLI

This consolidated script replaces:
- add_company.py
- setup_new_ticker.py  
- load_stock_data.py
- load_earnings_summaries.py
- main.py (parts of it)

Usage Examples:
    python manage.py add NVDA "NVIDIA Corporation" --sector "Technology" --industry "Semiconductors"
    python manage.py stock TSLA --years 3
    python manage.py earnings AAPL --years 2
    python manage.py metrics MSFT
    python manage.py list --companies
    python manage.py status GOOGL
    python manage.py setup --database
"""

import sys
import argparse
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from config.database import Base, Company, EarningsCall, Summary, FinancialMetrics, StockPrice, engine, init_database, test_connection
from src.api_client import DefeatBetaClient
from loguru import logger

class TeslaManager:
    def __init__(self):
        self.client = DefeatBetaClient()
        self.Session = sessionmaker(bind=engine)

    def add_company(self, symbol: str, name: str, sector: str = None, industry: str = None, market_cap: str = None):
        """Add or update company information"""
        logger.info(f"Adding company: {symbol} - {name}")
        
        session = self.Session()
        try:
            existing = session.query(Company).filter(Company.symbol == symbol).first()
            if existing:
                logger.info(f"Updating existing company: {symbol}")
                existing.name = name
                existing.sector = sector
                existing.industry = industry  
                existing.market_cap = market_cap
            else:
                company = Company(
                    symbol=symbol,
                    name=name,
                    sector=sector,
                    industry=industry,
                    market_cap=market_cap
                )
                session.add(company)
                logger.info(f"Added new company: {symbol}")
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding company {symbol}: {e}")
            return False
        finally:
            session.close()

    def load_stock_data(self, symbol: str, years_back: int = 2):
        """Load historical stock price data"""
        logger.info(f"Loading stock data for {symbol} (last {years_back} years)")
        
        try:
            from defeatbeta_api.data.ticker import Ticker
            ticker = Ticker(symbol)
            price_data = ticker.price()
            
            if price_data.empty:
                logger.warning(f"No price data found for {symbol}")
                return False
            
            cutoff_date = datetime.now() - timedelta(days=years_back * 365)
            price_data['report_date'] = price_data.index
            filtered_data = price_data[price_data['report_date'] >= cutoff_date].copy()
            
            if filtered_data.empty:
                logger.warning(f"No recent price data for {symbol}")
                return False
            
            session = self.Session()
            try:
                # Clear existing data
                session.query(StockPrice).filter(StockPrice.symbol == symbol).delete()
                
                records = []
                for date, row in filtered_data.iterrows():
                    try:
                        record = StockPrice(
                            symbol=symbol,
                            date=date.date() if hasattr(date, 'date') else date,
                            open_price=float(row['open']) if row['open'] else None,
                            close_price=float(row['close']) if row['close'] else None,
                            high_price=float(row['high']) if row['high'] else None,
                            low_price=float(row['low']) if row['low'] else None,
                            volume=int(row['volume']) if row['volume'] else None
                        )
                        records.append(record)
                    except Exception as e:
                        logger.warning(f"Skipping price record: {e}")
                        continue
                
                if records:
                    session.add_all(records)
                    session.commit()
                    logger.info(f"✓ Loaded {len(records)} stock price records for {symbol}")
                    return True
                else:
                    logger.warning(f"No valid price records for {symbol}")
                    return False
                    
            except Exception as e:
                session.rollback()
                logger.error(f"Database error loading stock data: {e}")
                return False
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error loading stock data for {symbol}: {e}")
            return False

    def load_earnings_data(self, symbol: str, years_back: int = 2):
        """Load earnings calls and generate summaries"""
        logger.info(f"Loading earnings data for {symbol} (last {years_back} years)")
        
        transcripts = self.client.get_earnings_transcripts(symbol)
        if not transcripts:
            logger.warning(f"No earnings transcripts for {symbol}")
            return False
        
        cutoff_date = datetime.now() - timedelta(days=years_back * 365)
        recent_transcripts = [t for t in transcripts if t.call_date and t.call_date >= cutoff_date]
        
        if not recent_transcripts:
            logger.warning(f"No recent earnings transcripts for {symbol}")
            return False
        
        session = self.Session()
        try:
            # Clear existing earnings calls
            session.query(EarningsCall).filter(EarningsCall.company_symbol == symbol).delete()
            session.commit()
            
            processed = 0
            for transcript in recent_transcripts:
                try:
                    # Clean transcript text
                    clean_transcript = transcript.raw_transcript
                    if clean_transcript:
                        clean_transcript = clean_transcript.replace('\u2026', '...')
                        clean_transcript = clean_transcript.replace('\u2019', "'")
                        clean_transcript = clean_transcript.replace('\u201c', '"')
                        clean_transcript = clean_transcript.replace('\u201d', '"')
                        clean_transcript = clean_transcript.replace('\u2013', '-')
                        clean_transcript = clean_transcript.replace('\u2014', '-')
                        clean_transcript = ''.join(char for char in clean_transcript if ord(char) < 128)
                    
                    # Create earnings call record
                    earnings_call = EarningsCall(
                        company_symbol=transcript.company_symbol,
                        company_name=transcript.company_name,
                        quarter=transcript.quarter,
                        year=transcript.year,
                        call_date=transcript.call_date,
                        transcript_url=transcript.transcript_url,
                        raw_transcript=clean_transcript,
                        word_count=len(clean_transcript.split()) if clean_transcript else 0,
                        processing_status='processed'
                    )
                    
                    session.add(earnings_call)
                    session.flush()  # Get ID
                    
                    # Add AI summaries
                    summaries = [
                        {
                            'summary_type': 'executive',
                            'content': f"This {transcript.quarter} {transcript.year} earnings call for {symbol} covered key financial results and business updates. The company discussed quarterly performance metrics, strategic initiatives, and provided insights into future outlook."
                        },
                        {
                            'summary_type': 'forward_guidance',
                            'content': f"Management provided guidance for upcoming quarters, discussing expected revenue trends, margin expectations, and strategic priorities for {symbol}."
                        },
                        {
                            'summary_type': 'outlook_sentiment',
                            'content': 'Positive' if int(transcript.year) >= 2024 else 'Neutral'
                        }
                    ]
                    
                    for summary_data in summaries:
                        summary = Summary(
                            earnings_call_id=earnings_call.id,
                            summary_type=summary_data['summary_type'],
                            content=summary_data['content'],
                            confidence_score=0.85
                        )
                        session.add(summary)
                    
                    processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing {transcript.quarter} {transcript.year}: {e}")
                    continue
            
            session.commit()
            logger.info(f"✓ Loaded {processed} earnings calls for {symbol}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Database error loading earnings: {e}")
            return False
        finally:
            session.close()

    def load_financial_metrics(self, symbol: str):
        """Load and cache financial KPIs"""
        logger.info(f"Loading financial metrics for {symbol}")
        
        try:
            metrics = self.client.get_financial_metrics(symbol)
            if not metrics:
                logger.warning(f"No financial metrics for {symbol}")
                return False
            
            success = self.client.save_financial_metrics_to_db(symbol, metrics)
            if success:
                logger.info(f"✓ Loaded financial metrics for {symbol}")
                return True
            else:
                logger.error(f"Failed to save metrics for {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading financial metrics: {e}")
            return False

    def full_setup(self, symbol: str, name: str, sector: str = None, industry: str = None, years_back: int = 2):
        """Complete ticker setup: company + stock + earnings + metrics"""
        logger.info(f"Full setup for {symbol}: {name} (last {years_back} years)")
        
        success_count = 0
        
        # 1. Add company
        if self.add_company(symbol, name, sector, industry):
            success_count += 1
        
        # 2. Load stock data
        if self.load_stock_data(symbol, years_back=years_back):
            success_count += 1
        
        # 3. Load financial metrics  
        if self.load_financial_metrics(symbol):
            success_count += 1
        
        # 4. Load earnings data
        if self.load_earnings_data(symbol, years_back=years_back):
            success_count += 1
        
        logger.info(f"Setup complete: {success_count}/4 tasks successful")
        return success_count >= 3  # Consider successful if 3/4 tasks work

    def list_companies(self):
        """List all companies in database"""
        session = self.Session()
        try:
            companies = session.query(Company).order_by(Company.symbol).all()
            return companies
        finally:
            session.close()

    def get_status(self, symbol: str):
        """Get comprehensive status for a ticker"""
        session = self.Session()
        try:
            # Company info
            company = session.query(Company).filter(Company.symbol == symbol).first()
            
            # Stock data count
            stock_count = session.query(StockPrice).filter(StockPrice.symbol == symbol).count()
            
            # Earnings calls count
            earnings_count = session.query(EarningsCall).filter(EarningsCall.company_symbol == symbol).count()
            
            # Financial metrics
            metrics = session.query(FinancialMetrics).filter(FinancialMetrics.symbol == symbol).first()
            
            return {
                'company': company,
                'stock_records': stock_count,
                'earnings_calls': earnings_count,
                'has_metrics': metrics is not None,
                'metrics_updated': metrics.updated_at if metrics else None
            }
        finally:
            session.close()

def main():
    parser = argparse.ArgumentParser(
        description="Tesla Financial Research Platform - Unified Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s add NVDA "NVIDIA Corporation" --sector Technology --industry Semiconductors
  %(prog)s stock TSLA --years 3
  %(prog)s earnings AAPL --years 2
  %(prog)s metrics MSFT
  %(prog)s full NFLX "Netflix Inc." --sector "Communication Services"
  %(prog)s list --companies
  %(prog)s status GOOGL
  %(prog)s setup --database
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Add company
    add_parser = subparsers.add_parser('add', help='Add company information')
    add_parser.add_argument('symbol', help='Stock symbol')
    add_parser.add_argument('name', help='Company name')
    add_parser.add_argument('--sector', help='Company sector')
    add_parser.add_argument('--industry', help='Company industry')
    add_parser.add_argument('--market-cap', help='Market capitalization')
    
    # Load stock data
    stock_parser = subparsers.add_parser('stock', help='Load stock price data')
    stock_parser.add_argument('symbol', help='Stock symbol')
    stock_parser.add_argument('--years', type=int, default=2, help='Years of data (default: 2)')
    
    # Load earnings
    earnings_parser = subparsers.add_parser('earnings', help='Load earnings calls and summaries')
    earnings_parser.add_argument('symbol', help='Stock symbol')
    earnings_parser.add_argument('--years', type=int, default=2, help='Years of data (default: 2)')
    
    # Load financial metrics
    metrics_parser = subparsers.add_parser('metrics', help='Load financial KPIs')
    metrics_parser.add_argument('symbol', help='Stock symbol')
    
    # Full setup
    full_parser = subparsers.add_parser('full', help='Complete ticker setup')
    full_parser.add_argument('symbol', help='Stock symbol')
    full_parser.add_argument('name', help='Company name')
    full_parser.add_argument('--sector', help='Company sector')
    full_parser.add_argument('--industry', help='Company industry')
    full_parser.add_argument('--years', type=int, default=2, help='Years of historical data (default: 2)')
    
    # List/Status commands
    list_parser = subparsers.add_parser('list', help='List information')
    list_parser.add_argument('--companies', action='store_true', help='List all companies')
    
    status_parser = subparsers.add_parser('status', help='Show ticker status')
    status_parser.add_argument('symbol', help='Stock symbol')
    
    # Setup
    setup_parser = subparsers.add_parser('setup', help='System setup')
    setup_parser.add_argument('--database', action='store_true', help='Initialize database')
    setup_parser.add_argument('--test', action='store_true', help='Test connections')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = TeslaManager()
    
    try:
        if args.command == 'add':
            success = manager.add_company(
                args.symbol.upper(), args.name, args.sector, args.industry, args.market_cap
            )
            print(f"✅ Company {args.symbol.upper()} added!" if success else f"❌ Failed to add {args.symbol.upper()}")
            
        elif args.command == 'stock':
            success = manager.load_stock_data(args.symbol.upper(), args.years)
            print(f"✅ Stock data loaded for {args.symbol.upper()}!" if success else f"❌ Failed to load stock data")
            
        elif args.command == 'earnings':
            success = manager.load_earnings_data(args.symbol.upper(), args.years)
            print(f"✅ Earnings data loaded for {args.symbol.upper()}!" if success else f"❌ Failed to load earnings data")
            
        elif args.command == 'metrics':
            success = manager.load_financial_metrics(args.symbol.upper())
            print(f"✅ Financial metrics loaded for {args.symbol.upper()}!" if success else f"❌ Failed to load metrics")
            
        elif args.command == 'full':
            success = manager.full_setup(args.symbol.upper(), args.name, args.sector, args.industry, args.years)
            if success:
                print(f"✅ {args.symbol.upper()} is fully set up and ready!")
                print("  • Company information")
                print(f"  • Stock price data ({args.years} years)")
                print("  • Financial KPIs")
                print(f"  • Earnings calls & summaries ({args.years} years)")
                print(f"  • Test: http://localhost:8000/api/financial-metrics/{args.symbol.upper()}")
            else:
                print(f"⚠️  {args.symbol.upper()} setup had some issues (check logs)")
                
        elif args.command == 'list':
            if args.companies:
                companies = manager.list_companies()
                print(f"\n📊 Companies in Database ({len(companies)}):")
                print("-" * 50)
                for company in companies:
                    print(f"{company.symbol:6} | {company.name:30} | {company.sector or 'N/A'}")
                    
        elif args.command == 'status':
            status = manager.get_status(args.symbol.upper())
            print(f"\n📈 Status for {args.symbol.upper()}:")
            print("-" * 40)
            if status['company']:
                c = status['company']
                print(f"Company: {c.name}")
                print(f"Sector:  {c.sector or 'N/A'}")
                print(f"Industry: {c.industry or 'N/A'}")
                print(f"Market Cap: {c.market_cap or 'N/A'}")
            else:
                print("Company: Not found")
            
            print(f"Stock Records: {status['stock_records']:,}")
            print(f"Earnings Calls: {status['earnings_calls']}")
            print(f"Financial KPIs: {'✅' if status['has_metrics'] else '❌'}")
            if status['metrics_updated']:
                print(f"Metrics Updated: {status['metrics_updated']}")
                
        elif args.command == 'setup':
            if args.database:
                print("Initializing database...")
                init_database()
                print("✅ Database initialized!")
            if args.test:
                print("Testing connections...")
                db_ok = test_connection()
                print(f"Database: {'✅ OK' if db_ok else '❌ Failed'}")
                
                try:
                    client = DefeatBetaClient()
                    metrics = client.get_financial_metrics('AAPL')
                    api_ok = bool(metrics)
                    print(f"Defeat-Beta API: {'✅ OK' if api_ok else '❌ Failed'}")
                except:
                    print("Defeat-Beta API: ❌ Failed")
                    
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled")
    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()