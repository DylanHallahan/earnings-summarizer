#!/usr/bin/env python3

import sys
import requests
import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from loguru import logger
from typing import Dict, Any, List, Optional

sys.path.append('.')
from config.database import FinancialNews, NewsSummary, engine
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

class NewsSummarizer:
    def __init__(self):
        self.ollama = OllamaClient(settings.OLLAMA_HOST)
        self.session = sessionmaker(bind=engine)()
    
    def chunk_content(self, content: str, max_chunk_size: int = 3000) -> List[str]:
        """Split content into manageable chunks"""
        sentences = content.split('. ')
        chunks = []
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sentence_length = len(sentence) + 2  # +2 for ". "
            
            if current_length + sentence_length > max_chunk_size and current_chunk:
                chunks.append('. '.join(current_chunk) + '.')
                # Keep some overlap
                overlap_size = min(2, len(current_chunk) // 4)
                current_chunk = current_chunk[-overlap_size:]
                current_length = sum(len(s) + 2 for s in current_chunk)
            
            current_chunk.append(sentence)
            current_length += sentence_length
        
        if current_chunk:
            chunks.append('. '.join(current_chunk) + '.')
        
        return chunks
    
    def extract_key_points(self, news_article: FinancialNews) -> str:
        """Extract key business points from news article"""
        if not news_article.content:
            return ""
        
        chunks = self.chunk_content(news_article.content, max_chunk_size=3000)
        
        if len(chunks) == 1:
            prompt = f"""
Analyze this financial news article about {news_article.symbol} and extract the key business points.

Article Title: {news_article.title}
Publisher: {news_article.publisher}
Date: {news_article.published_date}

Article Content:
{chunks[0]}

Extract and summarize the most important business points including:
1. Key business developments or announcements
2. Financial performance highlights or concerns
3. Strategic initiatives or operational changes
4. Market impact or competitive implications
5. Management commentary or analyst insights

Provide a clear, structured summary in 3-5 key points that would be valuable for investors:
"""
        else:
            # Process multiple chunks
            chunk_summaries = []
            
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing key points chunk {i+1}/{len(chunks)}")
                
                prompt = f"""
Extract key business points from this section of a financial news article about {news_article.symbol}:

{chunk}

Focus on:
- Business developments and announcements
- Financial performance data
- Strategic or operational changes
- Market implications

Provide concise key points from this section:
"""
                summary = self.ollama.generate(prompt, max_tokens=300)
                if summary:
                    chunk_summaries.append(summary)
            
            combined = "\n\n".join(chunk_summaries)
            
            prompt = f"""
These are key points from different sections of a financial news article about {news_article.symbol}. 
Title: {news_article.title}

{combined}

Create a cohesive summary of the most important business points in 3-5 structured key points:
"""
        
        return self.ollama.generate(prompt, max_tokens=400)
    
    def analyze_market_impact(self, news_article: FinancialNews) -> str:
        """Analyze potential market and business impact"""
        if not news_article.content:
            return ""
        
        # Focus on impact-relevant content
        content = news_article.content[:3000]  # First 3000 chars for impact analysis
        
        prompt = f"""
Analyze the potential market and business impact of this financial news about {news_article.symbol}.

Article: {news_article.title}
Publisher: {news_article.publisher}
Date: {news_article.published_date}

Content:
{content}

Provide an analysis covering:
1. **Stock Price Impact**: Potential short-term and medium-term effects on stock price
2. **Business Operations**: How this news affects the company's operations or strategy
3. **Competitive Position**: Impact on the company's market position vs competitors
4. **Investor Sentiment**: How this might influence investor perception
5. **Future Outlook**: Implications for future performance or business direction

Write a professional analysis in 250-350 words suitable for investment research:
"""
        
        return self.ollama.generate(prompt, max_tokens=400)
    
    def determine_news_sentiment(self, news_article: FinancialNews) -> str:
        """Determine overall sentiment in one word"""
        if not news_article.content:
            return "Neutral"
        
        # Use title and first part of content for sentiment
        content_sample = f"Title: {news_article.title}\n\nContent: {news_article.content[:1500]}"
        
        prompt = f"""
Analyze the sentiment of this financial news article about {news_article.symbol}.

{content_sample}

Based on the tone, content, and implications for the company, determine the overall sentiment.

Respond with EXACTLY ONE WORD from these options:
- Positive (good news, growth, success)
- Negative (bad news, declines, problems)
- Neutral (balanced, factual, mixed)
- Bullish (very positive, strong growth)
- Bearish (very negative, significant concerns)

One word only:
"""
        
        result = self.ollama.generate(prompt, max_tokens=10).strip()
        
        # Ensure we get a single word response
        if result:
            # Extract just the first word and clean it
            first_word = result.split()[0].strip('.,!?').title()
            # Validate it's a reasonable sentiment word
            valid_sentiments = ['Positive', 'Negative', 'Neutral', 'Bullish', 'Bearish']
            if first_word in valid_sentiments:
                return first_word
        
        return "Neutral"  # Default fallback
    
    def summarize_news_article(self, news_article: FinancialNews) -> Dict[str, str]:
        """Generate comprehensive summaries for a news article"""
        logger.info(f"Summarizing news: {news_article.title[:50]}...")
        
        if not news_article.content:
            logger.warning("No content available for summarization")
            return {}
        
        summaries = {}
        
        # Generate the 3 summary types
        logger.info("Extracting key points...")
        summaries['key_points'] = self.extract_key_points(news_article)
        
        logger.info("Analyzing market impact...")
        summaries['impact_analysis'] = self.analyze_market_impact(news_article)
        
        logger.info("Determining sentiment...")
        summaries['sentiment'] = self.determine_news_sentiment(news_article)
        
        return summaries
    
    def save_summaries(self, news_article: FinancialNews, summaries: Dict[str, str]):
        """Save summaries to database"""
        try:
            # Remove existing summaries for this news article
            self.session.query(NewsSummary).filter(
                NewsSummary.news_id == news_article.id
            ).delete()
            
            # Save new summaries
            for summary_type, content in summaries.items():
                if content and content.strip():
                    summary = NewsSummary(
                        news_id=news_article.id,
                        summary_type=summary_type,
                        content=content.strip(),
                        confidence_score=0.8,
                        created_at=datetime.utcnow()
                    )
                    self.session.add(summary)
            
            self.session.commit()
            logger.info(f"Saved {len(summaries)} summaries for news article {news_article.id}")
            
        except Exception as e:
            logger.error(f"Error saving summaries: {e}")
            self.session.rollback()
    
    def process_news_articles(self, symbol: str = None, limit: int = None):
        """Process news articles for summarization"""
        query = self.session.query(FinancialNews)
        
        if symbol:
            query = query.filter(FinancialNews.symbol == symbol.upper())
        
        query = query.order_by(FinancialNews.published_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        news_articles = query.all()
        
        if not news_articles:
            logger.warning("No news articles found matching criteria")
            return
        
        logger.info(f"Processing {len(news_articles)} news articles for summarization")
        
        for news_article in news_articles:
            # Check if summaries already exist
            existing_summaries = self.session.query(NewsSummary).filter(
                NewsSummary.news_id == news_article.id
            ).count()
            
            if existing_summaries > 0:
                logger.info(f"Skipping news {news_article.id} - summaries already exist")
                continue
            
            try:
                summaries = self.summarize_news_article(news_article)
                if summaries:
                    self.save_summaries(news_article, summaries)
                    logger.info(f"Completed news article {news_article.id}")
                    logger.info(f"Generated summaries: {list(summaries.keys())}")
                else:
                    logger.warning(f"No summaries generated for news article {news_article.id}")
            
            except Exception as e:
                logger.error(f"Error processing news article {news_article.id}: {e}")
                continue
    
    def list_available_symbols(self) -> List[str]:
        """List all symbols with news data"""
        result = self.session.query(FinancialNews.symbol).distinct().all()
        return [row[0] for row in result]
    
    def close(self):
        """Close database session"""
        self.session.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate financial news summaries")
    parser.add_argument("--symbol", type=str, help="Stock symbol to process (e.g., AAPL, TSLA)")
    parser.add_argument("--limit", type=int, help="Limit number of articles to process")
    parser.add_argument("--test", action="store_true", help="Test Ollama connection only")
    parser.add_argument("--list", action="store_true", help="List available symbols with news")
    
    args = parser.parse_args()
    
    # Setup logging
    logger.add(
        settings.LOGS_DIR / "news_summarizer.log",
        rotation="10 MB",
        retention="1 month",
        level="INFO"
    )
    
    summarizer = NewsSummarizer()
    
    try:
        if args.test:
            if summarizer.ollama.is_available():
                logger.info("✓ Ollama test successful - ready for news summarization")
                print("✓ Ollama connection successful")
            else:
                logger.error("✗ Ollama test failed - check if Ollama is running and llama3.1:8b is installed")
                print("✗ Ollama connection failed")
            return
        
        if args.list:
            symbols = summarizer.list_available_symbols()
            if symbols:
                print("Available symbols with news data:")
                for symbol in sorted(symbols):
                    print(f"  - {symbol}")
            else:
                print("No news found in database")
            return
        
        # Check Ollama availability
        if not summarizer.ollama.is_available():
            logger.error("Ollama not available. Please ensure Ollama is running with llama3.1:8b model")
            print("Error: Ollama not available. Please ensure Ollama is running.")
            return
        
        # Process news articles
        summarizer.process_news_articles(
            symbol=args.symbol,
            limit=args.limit
        )
        
        print("✅ News summarization completed successfully")
        
    finally:
        summarizer.close()
    
    logger.info("News summarization completed")

if __name__ == "__main__":
    main()