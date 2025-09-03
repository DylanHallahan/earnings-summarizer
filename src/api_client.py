from defeatbeta_api.data.ticker import Ticker
from typing import List, Dict, Any, Optional
from datetime import datetime
from config.settings import settings
from src.models import CompanyInfo, EarningsCallData
import pandas as pd
from loguru import logger

class DefeatBetaClient:
    def __init__(self):
        pass
    
    def get_company_info(self, symbol: str) -> Optional[CompanyInfo]:
        """Get basic company information"""
        try:
            ticker = Ticker(symbol)
            # Get basic price data to verify symbol exists
            price_data = ticker.price()
            if price_data.empty:
                return None
                
            return CompanyInfo(
                symbol=symbol,
                name=f"{symbol} Corporation",  # Defeat Beta API doesn't provide company names
                sector=None,
                industry=None,
                market_cap=None
            )
        except Exception as e:
            logger.error(f"Error getting company info for {symbol}: {e}")
            return None
    
    def get_earnings_transcripts(self, symbol: str) -> List[EarningsCallData]:
        """Get all available earnings call transcripts for a company"""
        try:
            ticker = Ticker(symbol)
            transcripts_api = ticker.earning_call_transcripts()
            transcripts_list = transcripts_api.get_transcripts_list()
            
            earnings_data = []
            
            for _, row in transcripts_list.iterrows():
                # Convert the transcript data to our format
                transcript_data = transcripts_api.get_transcript(
                    row['fiscal_year'], 
                    row['fiscal_quarter']
                )
                
                # Combine all transcript content
                full_transcript = ""
                for _, content_row in transcript_data.iterrows():
                    speaker = content_row['speaker']
                    content = content_row['content']
                    full_transcript += f"{speaker}: {content}\n\n"
                
                earnings_call = EarningsCallData(
                    company_symbol=symbol,
                    company_name=f"{symbol} Corporation",
                    quarter=f"Q{row['fiscal_quarter']}",
                    year=row['fiscal_year'],
                    call_date=pd.to_datetime(row['report_date']),
                    transcript_url=None,
                    raw_transcript=full_transcript
                )
                
                earnings_data.append(earnings_call)
            
            logger.info(f"Retrieved {len(earnings_data)} earnings transcripts for {symbol}")
            return earnings_data
            
        except Exception as e:
            logger.error(f"Error getting earnings transcripts for {symbol}: {e}")
            return []
    
    def get_latest_earnings_transcript(self, symbol: str) -> Optional[EarningsCallData]:
        """Get the most recent earnings call transcript"""
        transcripts = self.get_earnings_transcripts(symbol)
        if transcripts:
            # Sort by year and quarter to get the most recent
            latest = max(transcripts, key=lambda x: (x.year, int(x.quarter.replace('Q', ''))))
            return latest
        return None
    
    def get_financial_metrics(self, symbol: str) -> Dict[str, Any]:
        """Get key financial metrics and ratios for a company"""
        try:
            ticker = Ticker(symbol)
            
            metrics = {}
            
            # Start with what we know works - price data (same pattern as load_stock_data.py)
            price_data = ticker.price()
            if not price_data.empty:
                metrics['current_price'] = float(price_data['close'].iloc[-1])
                logger.info(f"Got current price for {symbol}: ${metrics['current_price']}")
            
            # Try TTM EPS - this method exists based on the dir() output we saw
            try:
                ttm_eps_data = ticker.ttm_eps()
                if not ttm_eps_data.empty:
                    metrics['eps_ttm'] = float(ttm_eps_data.iloc[-1]['eps'])
                    logger.info(f"Got TTM EPS for {symbol}: {metrics['eps_ttm']}")
            except Exception as e:
                logger.warning(f"Could not get TTM EPS for {symbol}: {e}")
            
            # Try TTM P/E ratio
            try:
                ttm_pe_data = ticker.ttm_pe()
                if not ttm_pe_data.empty:
                    metrics['pe_ratio'] = float(ttm_pe_data.iloc[-1]['ttm_pe'])
                    logger.info(f"Got P/E ratio for {symbol}: {metrics['pe_ratio']}")
            except Exception as e:
                logger.warning(f"Could not get P/E ratio for {symbol}: {e}")
            
            # Try P/S and P/B ratios if methods exist
            try:
                ps_data = ticker.ps_ratio()
                if not ps_data.empty:
                    metrics['ps_ratio'] = float(ps_data.iloc[-1]['ps_ratio'])
                    logger.info(f"Got P/S ratio for {symbol}: {metrics['ps_ratio']}")
            except Exception as e:
                logger.warning(f"Could not get P/S ratio for {symbol}: {e}")
            
            try:
                pb_data = ticker.pb_ratio()
                if not pb_data.empty:
                    metrics['pb_ratio'] = float(pb_data.iloc[-1]['pb_ratio'])
                    logger.info(f"Got P/B ratio for {symbol}: {metrics['pb_ratio']}")
            except Exception as e:
                logger.warning(f"Could not get P/B ratio for {symbol}: {e}")
            
            # Try to get market cap
            try:
                market_cap_data = ticker.market_capitalization()
                if not market_cap_data.empty:
                    metrics['market_cap'] = float(market_cap_data.iloc[-1]['market_capitalization'])
                    logger.info(f"Got market cap for {symbol}: ${metrics['market_cap']/1_000_000_000:.1f}B")
            except Exception as e:
                logger.warning(f"Could not get market cap for {symbol}: {e}")
            
            # For now, let's skip the complex margin calculations and just return what we have
            # We can add placeholder values that look realistic
            if metrics.get('current_price') and metrics.get('eps_ttm'):
                # Calculate basic metrics if we have the data
                if metrics['eps_ttm'] > 0:
                    metrics['calculated_pe'] = metrics['current_price'] / metrics['eps_ttm']
                
            # Note: gross_margin, net_margin, and revenue_ttm would need additional API calls
            # For now, we'll leave them as None if not available from the basic API
            
            # Add timestamp
            metrics['updated_at'] = datetime.now().isoformat()
            
            logger.info(f"Retrieved financial metrics for {symbol}: {list(metrics.keys())}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting financial metrics for {symbol}: {e}")
            return {}

    def save_financial_metrics_to_db(self, symbol: str, metrics: Dict[str, Any]) -> bool:
        """Save financial metrics to the database"""
        try:
            from config.database import SessionLocal, FinancialMetrics
            
            db = SessionLocal()
            
            # Check if metrics already exist for this symbol
            existing = db.query(FinancialMetrics).filter(FinancialMetrics.symbol == symbol).first()
            
            if existing:
                # Update existing record
                existing.pe_ratio = metrics.get('pe_ratio')
                existing.ps_ratio = metrics.get('ps_ratio')
                existing.pb_ratio = metrics.get('pb_ratio')
                existing.eps_ttm = metrics.get('eps_ttm')
                existing.revenue_ttm = metrics.get('revenue_ttm')
                existing.gross_margin = metrics.get('gross_margin')
                existing.net_margin = metrics.get('net_margin')
                existing.current_price = metrics.get('current_price')
                existing.market_cap = metrics.get('market_cap')
                existing.updated_at = datetime.now()
            else:
                # Create new record
                financial_metrics = FinancialMetrics(
                    symbol=symbol,
                    pe_ratio=metrics.get('pe_ratio'),
                    ps_ratio=metrics.get('ps_ratio'),
                    pb_ratio=metrics.get('pb_ratio'),
                    eps_ttm=metrics.get('eps_ttm'),
                    revenue_ttm=metrics.get('revenue_ttm'),
                    gross_margin=metrics.get('gross_margin'),
                    net_margin=metrics.get('net_margin'),
                    current_price=metrics.get('current_price'),
                    market_cap=metrics.get('market_cap')
                )
                db.add(financial_metrics)
            
            db.commit()
            db.close()
            
            logger.info(f"Saved financial metrics to database for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving financial metrics to database for {symbol}: {e}")
            return False
    
    def get_stock_price_data(self, symbol: str, days: int = 730) -> List[Dict[str, Any]]:
        """Get historical stock price data"""
        try:
            ticker = Ticker(symbol)
            price_data = ticker.price()
            
            if price_data.empty:
                return []
            
            # Convert to list of dictionaries for API response
            stock_prices = []
            for date, row in price_data.tail(days).iterrows():
                stock_prices.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'open_price': float(row['open']),
                    'close_price': float(row['close']),
                    'high_price': float(row['high']),
                    'low_price': float(row['low']),
                    'volume': int(row['volume']) if pd.notna(row['volume']) else 0
                })
            
            logger.info(f"Retrieved {len(stock_prices)} price data points for {symbol}")
            return stock_prices
            
        except Exception as e:
            logger.error(f"Error getting stock price data for {symbol}: {e}")
            return []
    
    def get_financial_news(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get financial news articles for a company"""
        try:
            ticker = Ticker(symbol)
            news_obj = ticker.news()
            news_list = news_obj.get_news_list()
            
            if news_list.empty:
                return []
            
            news_articles = []
            for _, row in news_list.head(limit).iterrows():
                article = self._process_news_row(row, symbol)
                if article:
                    news_articles.append(article)
            
            logger.info(f"Retrieved {len(news_articles)} news articles for {symbol}")
            return news_articles
            
        except Exception as e:
            logger.error(f"Error getting financial news for {symbol}: {e}")
            return []
    
    def _process_news_row(self, row: pd.Series, symbol: str) -> Optional[Dict[str, Any]]:
        """Process a single news row into article format"""
        try:
            content_text = self._extract_news_content(row)
            related_symbols = self._extract_related_symbols(row)
            
            return {
                'uuid': str(row['uuid']),
                'title': str(row['title']),
                'publisher': str(row['publisher']) if pd.notna(row['publisher']) else 'Unknown',
                'published_date': str(row['report_date']) if pd.notna(row['report_date']) else None,
                'url': str(row['link']) if pd.notna(row['link']) else None,
                'news_type': str(row['type']) if pd.notna(row['type']) else 'NEWS',
                'related_symbols': related_symbols,
                'content': content_text.strip(),
                'symbol': symbol
            }
        except Exception as e:
            logger.warning(f"Error processing news row: {e}")
            return None
    
    def _extract_news_content(self, row: pd.Series) -> str:
        """Extract content from news field"""
        content_text = ""
        try:
            news_data = row['news']
            if news_data is not None:
                if hasattr(news_data, 'tolist'):
                    news_data = news_data.tolist()
                
                if isinstance(news_data, list):
                    for item in news_data:
                        if isinstance(item, dict) and 'paragraph' in item:
                            paragraph = item['paragraph']
                            if paragraph and paragraph.strip():
                                content_text += paragraph.strip() + "\n\n"
        except Exception as e:
            logger.warning(f"Error extracting news content: {e}")
        
        return content_text
    
    def _extract_related_symbols(self, row: pd.Series) -> List[str]:
        """Extract related symbols from row"""
        try:
            if pd.notna(row['related_symbols']):
                if isinstance(row['related_symbols'], list):
                    return row['related_symbols']
                else:
                    return [str(row['related_symbols'])]
        except Exception:
            pass
        return []
    
    def get_detailed_news_content(self, symbol: str, uuid: str) -> Optional[Dict[str, Any]]:
        """Get detailed content for a specific news article"""
        try:
            ticker = Ticker(symbol)
            news_obj = ticker.news()
            detailed_news = news_obj.get_news(uuid)
            
            if detailed_news.empty:
                return None
            
            # Extract detailed content
            row = detailed_news.iloc[0]
            content_text = ""
            
            if row['news'] and isinstance(row['news'], list):
                for item in row['news']:
                    if isinstance(item, dict) and 'paragraph' in item:
                        content_text += item['paragraph'] + "\n\n"
            
            return {
                'uuid': row['uuid'],
                'title': row['title'],
                'publisher': row['publisher'],
                'published_date': row['report_date'],
                'url': row['link'],
                'news_type': row['type'],
                'related_symbols': row['related_symbols'],
                'content': content_text.strip(),
                'symbol': symbol
            }
            
        except Exception as e:
            logger.error(f"Error getting detailed news content for {symbol}/{uuid}: {e}")
            return None
    
    def save_news_to_db(self, news_articles: List[Dict[str, Any]]) -> bool:
        """Save news articles to the database"""
        try:
            from config.database import SessionLocal, FinancialNews
            from datetime import datetime
            
            db = SessionLocal()
            
            saved_count = 0
            for article in news_articles:
                # Check if article already exists
                existing = db.query(FinancialNews).filter(FinancialNews.uuid == article['uuid']).first()
                
                if not existing:
                    # Parse date
                    published_date = None
                    if article['published_date']:
                        try:
                            published_date = pd.to_datetime(article['published_date']).date()
                        except:
                            pass
                    
                    # Clean text fields to handle encoding issues
                    def clean_text(text):
                        if text is None:
                            return None
                        # Convert to string and replace problematic Unicode characters
                        text = str(text)
                        # Replace smart quotes and dashes with regular ones
                        text = text.replace(''', "'").replace(''', "'")
                        text = text.replace('"', '"').replace('"', '"')
                        text = text.replace('–', '-').replace('—', '-')
                        text = text.replace('…', '...')
                        # Remove any remaining non-ASCII characters
                        text = text.encode('ascii', 'ignore').decode('ascii')
                        return text
                    
                    news_record = FinancialNews(
                        uuid=clean_text(article['uuid']),
                        symbol=clean_text(article['symbol']),
                        title=clean_text(article['title']),
                        content=clean_text(article['content']),
                        publisher=clean_text(article['publisher']),
                        published_date=published_date,
                        url=clean_text(article['url']),
                        news_type=clean_text(article['news_type']),
                        related_symbols=article['related_symbols']
                    )
                    db.add(news_record)
                    saved_count += 1
            
            db.commit()
            db.close()
            
            logger.info(f"Saved {saved_count} new news articles to database")
            return True
            
        except Exception as e:
            logger.error(f"Error saving news articles to database: {e}")
            return False

# Test function
def test_api_connection():
    """Test Defeat Beta API connection"""
    try:
        client = DefeatBetaClient()
        # Test with Tesla
        company = client.get_company_info("TSLA")
        if company:
            logger.info(f"API connection successful. Retrieved: {company.symbol}")
            
            # Test getting transcripts
            transcripts = client.get_earnings_transcripts("TSLA")
            if transcripts:
                latest = transcripts[-1]  # Get the last one
                logger.info(f"Retrieved transcript: {latest.company_symbol} {latest.quarter} {latest.year}")
                logger.info(f"Transcript length: {len(latest.raw_transcript)} characters")
                return True
        
        logger.error("API connection failed: No data returned")
        return False
        
    except Exception as e:
        logger.error(f"API connection failed: {e}")
        return False