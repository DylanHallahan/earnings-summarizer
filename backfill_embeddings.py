#!/usr/bin/env python3

import argparse
import time
from typing import List
import sys
from loguru import logger

sys.path.append('.')
from config.database import SessionLocal, EarningsCall, FinancialNews
from src.rag_service import PostgresRAGService


def backfill_earnings_embeddings(
    symbol: str = None, 
    limit: int = None, 
    skip_existing: bool = True
) -> int:
    """
    Backfill embeddings for existing earnings calls
    
    Args:
        symbol: Optional company symbol to filter by
        limit: Maximum number of earnings calls to process
        skip_existing: Skip earnings calls that already have embeddings
    
    Returns:
        Number of earnings calls processed
    """
    logger.info("Starting earnings embeddings backfill...")
    
    rag = PostgresRAGService()
    if not rag.embedder:
        logger.error("❌ Embedding model not available. Cannot proceed.")
        return 0
    
    db = SessionLocal()
    processed = 0
    
    try:
        # Build query for earnings calls
        query = db.query(EarningsCall).filter(EarningsCall.raw_transcript.isnot(None))
        
        if symbol:
            query = query.filter(EarningsCall.company_symbol == symbol.upper())
        
        # Order by most recent first
        query = query.order_by(EarningsCall.year.desc(), EarningsCall.quarter.desc())
        
        if limit:
            query = query.limit(limit)
        
        earnings_calls = query.all()
        
        if not earnings_calls:
            logger.warning("No earnings calls found matching criteria")
            return 0
        
        logger.info(f"Found {len(earnings_calls)} earnings calls to process")
        
        for i, earnings_call in enumerate(earnings_calls, 1):
            try:
                # Check if embeddings already exist
                if skip_existing:
                    from config.database import DocumentChunk
                    existing_count = db.query(DocumentChunk).filter(
                        DocumentChunk.earnings_call_id == earnings_call.id
                    ).count()
                    
                    if existing_count > 0:
                        logger.info(f"⏭️  Skipping {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter} - {existing_count} embeddings exist")
                        continue
                
                logger.info(f"📄 Processing {i}/{len(earnings_calls)}: {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
                
                start_time = time.time()
                chunks_added = rag.add_earnings_embeddings(earnings_call.id)
                processing_time = time.time() - start_time
                
                if chunks_added > 0:
                    logger.info(f"✅ Added {chunks_added} chunks in {processing_time:.1f}s")
                    processed += 1
                else:
                    logger.warning(f"⚠️  No chunks added for {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
                
                # Small delay to prevent overwhelming the system
                if i % 5 == 0:
                    logger.info(f"Processed {i}/{len(earnings_calls)} calls. Taking a short break...")
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"❌ Error processing {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}: {e}")
                continue
        
        logger.info(f"🎉 Backfill completed! Processed {processed}/{len(earnings_calls)} earnings calls")
        return processed
        
    except Exception as e:
        logger.error(f"❌ Error in earnings embeddings backfill: {e}")
        return 0
    finally:
        db.close()


def backfill_news_embeddings(
    symbol: str = None,
    limit: int = 100,
    skip_existing: bool = True
) -> int:
    """
    Backfill embeddings for financial news articles
    
    Args:
        symbol: Optional company symbol to filter by
        limit: Maximum number of news articles to process
        skip_existing: Skip news articles that already have embeddings
    
    Returns:
        Number of news articles processed
    """
    logger.info("Starting news embeddings backfill...")
    
    rag = PostgresRAGService()
    if not rag.embedder:
        logger.error("❌ Embedding model not available. Cannot proceed.")
        return 0
    
    db = SessionLocal()
    processed = 0
    
    try:
        # Build query for news articles with content
        query = db.query(FinancialNews).filter(
            FinancialNews.content.isnot(None),
            FinancialNews.content != ""
        )
        
        if symbol:
            query = query.filter(FinancialNews.symbol == symbol.upper())
        
        # Order by most recent first
        query = query.order_by(FinancialNews.published_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        news_articles = query.all()
        
        if not news_articles:
            logger.warning("No news articles found matching criteria")
            return 0
        
        logger.info(f"Found {len(news_articles)} news articles to process")
        
        for i, news in enumerate(news_articles, 1):
            try:
                # Check if embeddings already exist
                if skip_existing:
                    from config.database import NewsChunk
                    existing_count = db.query(NewsChunk).filter(
                        NewsChunk.news_id == news.id
                    ).count()
                    
                    if existing_count > 0:
                        logger.info(f"⏭️  Skipping news {news.id} - {existing_count} embeddings exist")
                        continue
                
                logger.info(f"📰 Processing {i}/{len(news_articles)}: {news.symbol} - {news.title[:50]}...")
                
                start_time = time.time()
                chunks_added = rag.add_news_embeddings(news.id)
                processing_time = time.time() - start_time
                
                if chunks_added > 0:
                    logger.info(f"✅ Added {chunks_added} chunks in {processing_time:.1f}s")
                    processed += 1
                else:
                    logger.warning(f"⚠️  No chunks added for news {news.id}")
                
                # Small delay every 10 articles
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{len(news_articles)} articles. Taking a short break...")
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"❌ Error processing news {news.id}: {e}")
                continue
        
        logger.info(f"🎉 News backfill completed! Processed {processed}/{len(news_articles)} articles")
        return processed
        
    except Exception as e:
        logger.error(f"❌ Error in news embeddings backfill: {e}")
        return 0
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Backfill RAG embeddings for existing data")
    parser.add_argument("--type", choices=['earnings', 'news', 'all'], default='all',
                       help="Type of data to backfill")
    parser.add_argument("--symbol", type=str, help="Company symbol to filter by (e.g., AAPL)")
    parser.add_argument("--limit", type=int, help="Limit number of items to process")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                       help="Skip items that already have embeddings")
    parser.add_argument("--force", action="store_true",
                       help="Reprocess items even if embeddings exist")
    
    args = parser.parse_args()
    
    # Setup logging
    from config.settings import settings
    logger.add(
        settings.LOGS_DIR / "embedding_backfill.log",
        rotation="10 MB",
        retention="1 week",
        level="INFO"
    )
    
    skip_existing = args.skip_existing and not args.force
    
    if args.type in ['earnings', 'all']:
        logger.info("🚀 Starting earnings embeddings backfill...")
        earnings_processed = backfill_earnings_embeddings(
            symbol=args.symbol,
            limit=args.limit,
            skip_existing=skip_existing
        )
        print(f"✅ Processed {earnings_processed} earnings calls")
    
    if args.type in ['news', 'all']:
        logger.info("🚀 Starting news embeddings backfill...")
        news_limit = args.limit or 100  # Default limit for news
        news_processed = backfill_news_embeddings(
            symbol=args.symbol,
            limit=news_limit,
            skip_existing=skip_existing
        )
        print(f"✅ Processed {news_processed} news articles")
    
    # Show final stats
    rag = PostgresRAGService()
    if rag.embedder:
        stats = rag.get_embedding_stats()
        print("\n📊 Current RAG Statistics:")
        print(f"  Total document chunks: {stats.get('total_document_chunks', 0)}")
        print(f"  Earnings calls with embeddings: {stats.get('earnings_calls_with_embeddings', 0)}")
        print(f"  News articles with embeddings: {stats.get('news_articles_with_embeddings', 0)}")


if __name__ == "__main__":
    main()