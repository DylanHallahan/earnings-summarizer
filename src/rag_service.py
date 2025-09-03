#!/usr/bin/env python3

from typing import List, Dict, Any, Optional, Tuple
import sys
import time
from loguru import logger

sys.path.append('..')
from config.database import SessionLocal, EarningsCall, DocumentChunk, NewsChunk, FinancialNews


class PostgresRAGService:
    """RAG service using PostgreSQL with pgvector for embeddings"""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self.embedder = None
        self._load_embedding_model()
    
    def _load_embedding_model(self):
        """Lazy load the embedding model"""
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Loading embedding model: {self.model_name}")
            self.embedder = SentenceTransformer(self.model_name)
            logger.info("✓ Embedding model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            self.embedder = None
    
    def chunk_text(self, text: str, max_chunk_size: int = 1000, overlap: int = 100) -> List[str]:
        """Split text into overlapping chunks for better context preservation"""
        if not text:
            return []
            
        words = text.split()
        chunks = []
        
        for i in range(0, len(words), max_chunk_size - overlap):
            chunk_words = words[i:i + max_chunk_size]
            chunk_text = ' '.join(chunk_words)
            chunks.append(chunk_text)
            
            # Stop if we've reached the end
            if i + max_chunk_size >= len(words):
                break
        
        return chunks
    
    def add_earnings_embeddings(self, earnings_call_id: int, chunk_transcripts: bool = True, chunk_summaries: bool = True) -> int:
        """Process and store embeddings for an earnings call"""
        if not self.embedder:
            logger.error("Embedding model not available")
            return 0
            
        db = SessionLocal()
        chunks_added = 0
        
        try:
            # Get earnings call
            earnings_call = db.query(EarningsCall).filter(EarningsCall.id == earnings_call_id).first()
            if not earnings_call:
                logger.error(f"Earnings call {earnings_call_id} not found")
                return 0
            
            logger.info(f"Processing embeddings for {earnings_call.company_symbol} {earnings_call.year} {earnings_call.quarter}")
            
            # Remove existing chunks for this earnings call
            db.query(DocumentChunk).filter(DocumentChunk.earnings_call_id == earnings_call_id).delete()
            db.commit()
            
            # Chunk and embed transcript
            if chunk_transcripts and earnings_call.raw_transcript:
                transcript_chunks = self.chunk_text(earnings_call.raw_transcript)
                logger.info(f"Created {len(transcript_chunks)} transcript chunks")
                
                # Process chunks in batches for memory efficiency
                batch_size = 10
                for i in range(0, len(transcript_chunks), batch_size):
                    batch_chunks = transcript_chunks[i:i + batch_size]
                    
                    # Generate embeddings for batch
                    embeddings = self.embedder.encode(batch_chunks, show_progress_bar=False)
                    
                    # Store chunks with embeddings
                    for j, (chunk_text, embedding) in enumerate(zip(batch_chunks, embeddings)):
                        doc_chunk = DocumentChunk(
                            earnings_call_id=earnings_call_id,
                            chunk_text=chunk_text,
                            chunk_index=i + j,
                            chunk_type='transcript',
                            embedding=embedding.tolist()
                        )
                        db.add(doc_chunk)
                        chunks_added += 1
                    
                    # Commit batch to avoid memory issues
                    db.commit()
            
            # Chunk and embed summaries if available
            if chunk_summaries:
                from config.database import Summary
                summaries = db.query(Summary).filter(Summary.earnings_call_id == earnings_call_id).all()
                
                for summary in summaries:
                    if summary.content:
                        summary_chunks = self.chunk_text(summary.content, max_chunk_size=500)
                        logger.info(f"Created {len(summary_chunks)} chunks for {summary.summary_type} summary")
                        
                        embeddings = self.embedder.encode(summary_chunks, show_progress_bar=False)
                        
                        for j, (chunk_text, embedding) in enumerate(zip(summary_chunks, embeddings)):
                            doc_chunk = DocumentChunk(
                                earnings_call_id=earnings_call_id,
                                chunk_text=chunk_text,
                                chunk_index=j,
                                chunk_type=f'summary_{summary.summary_type}',
                                embedding=embedding.tolist()
                            )
                            db.add(doc_chunk)
                            chunks_added += 1
                        
                        db.commit()
            
            logger.info(f"✓ Added {chunks_added} embedding chunks for earnings call {earnings_call_id}")
            return chunks_added
            
        except Exception as e:
            logger.error(f"Error processing embeddings for earnings call {earnings_call_id}: {e}")
            db.rollback()
            return 0
        finally:
            db.close()
    
    def add_news_embeddings(self, news_id: int) -> int:
        """Process and store embeddings for a news article"""
        if not self.embedder:
            logger.error("Embedding model not available")
            return 0
            
        db = SessionLocal()
        chunks_added = 0
        
        try:
            # Get news article
            news = db.query(FinancialNews).filter(FinancialNews.id == news_id).first()
            if not news or not news.content:
                logger.warning(f"News article {news_id} not found or has no content")
                return 0
            
            logger.info(f"Processing embeddings for news: {news.title[:50]}...")
            
            # Remove existing chunks for this news article
            db.query(NewsChunk).filter(NewsChunk.news_id == news_id).delete()
            
            # Chunk the news content
            news_chunks = self.chunk_text(news.content, max_chunk_size=800)
            logger.info(f"Created {len(news_chunks)} news chunks")
            
            # Generate embeddings
            embeddings = self.embedder.encode(news_chunks, show_progress_bar=False)
            
            # Store chunks with embeddings
            for i, (chunk_text, embedding) in enumerate(zip(news_chunks, embeddings)):
                news_chunk = NewsChunk(
                    news_id=news_id,
                    chunk_text=chunk_text,
                    chunk_index=i,
                    embedding=embedding.tolist()
                )
                db.add(news_chunk)
                chunks_added += 1
            
            db.commit()
            logger.info(f"✓ Added {chunks_added} embedding chunks for news {news_id}")
            return chunks_added
            
        except Exception as e:
            logger.error(f"Error processing embeddings for news {news_id}: {e}")
            db.rollback()
            return 0
        finally:
            db.close()
    
    def semantic_search(self, 
                       query: str, 
                       company_symbol: str = None, 
                       search_type: str = "all",  # 'transcript', 'summary', 'news', 'all'
                       limit: int = 5,
                       similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """
        Retrieve relevant chunks using vector similarity search
        
        Args:
            query: Search query text
            company_symbol: Optional company filter
            search_type: Type of content to search ('transcript', 'summary', 'news', 'all')
            limit: Maximum number of results
            similarity_threshold: Minimum cosine similarity score (0-1)
        
        Returns:
            List of relevant chunks with metadata
        """
        if not self.embedder:
            logger.error("Embedding model not available")
            return []
            
        db = SessionLocal()
        results = []
        
        try:
            # Generate query embedding
            query_embedding = self.embedder.encode([query], show_progress_bar=False)[0]
            
            # Search document chunks (earnings calls)
            if search_type in ['all', 'transcript', 'summary']:
                doc_query = db.query(
                    DocumentChunk.chunk_text,
                    DocumentChunk.chunk_type,
                    DocumentChunk.earnings_call_id,
                    EarningsCall.company_symbol,
                    EarningsCall.quarter,
                    EarningsCall.year,
                    EarningsCall.call_date,
                    (1 - DocumentChunk.embedding.cosine_distance(query_embedding)).label('similarity')
                ).join(EarningsCall)
                
                # Apply filters
                if company_symbol:
                    doc_query = doc_query.filter(EarningsCall.company_symbol == company_symbol)
                
                if search_type == 'transcript':
                    doc_query = doc_query.filter(DocumentChunk.chunk_type == 'transcript')
                elif search_type == 'summary':
                    doc_query = doc_query.filter(DocumentChunk.chunk_type.like('summary_%'))
                
                doc_results = doc_query.filter(
                    (1 - DocumentChunk.embedding.cosine_distance(query_embedding)) >= similarity_threshold
                ).order_by(
                    (1 - DocumentChunk.embedding.cosine_distance(query_embedding)).desc()
                ).limit(limit).all()
                
                for result in doc_results:
                    results.append({
                        'content': result.chunk_text,
                        'type': result.chunk_type,
                        'source': 'earnings_call',
                        'company_symbol': result.company_symbol,
                        'quarter': result.quarter,
                        'year': result.year,
                        'date': result.call_date,
                        'similarity': float(result.similarity),
                        'earnings_call_id': result.earnings_call_id
                    })
            
            # Search news chunks
            if search_type in ['all', 'news']:
                news_query = db.query(
                    NewsChunk.chunk_text,
                    FinancialNews.symbol,
                    FinancialNews.title,
                    FinancialNews.publisher,
                    FinancialNews.published_date,
                    FinancialNews.url,
                    (1 - NewsChunk.embedding.cosine_distance(query_embedding)).label('similarity')
                ).join(FinancialNews)
                
                if company_symbol:
                    news_query = news_query.filter(FinancialNews.symbol == company_symbol)
                
                news_results = news_query.filter(
                    (1 - NewsChunk.embedding.cosine_distance(query_embedding)) >= similarity_threshold
                ).order_by(
                    (1 - NewsChunk.embedding.cosine_distance(query_embedding)).desc()
                ).limit(limit).all()
                
                for result in news_results:
                    results.append({
                        'content': result.chunk_text,
                        'type': 'news',
                        'source': 'financial_news',
                        'company_symbol': result.symbol,
                        'title': result.title,
                        'publisher': result.publisher,
                        'date': result.published_date,
                        'url': result.url,
                        'similarity': float(result.similarity)
                    })
            
            # Sort all results by similarity and limit
            results.sort(key=lambda x: x['similarity'], reverse=True)
            results = results[:limit]
            
            logger.info(f"Found {len(results)} relevant chunks for query: '{query[:50]}...'")
            return results
            
        except Exception as e:
            logger.error(f"Error in semantic search: {e}")
            return []
        finally:
            db.close()
    
    def get_embedding_stats(self) -> Dict[str, Any]:
        """Get statistics about embedded documents"""
        db = SessionLocal()
        
        try:
            # Count document chunks
            doc_chunks = db.query(DocumentChunk).count()
            earnings_calls_with_embeddings = db.query(DocumentChunk.earnings_call_id).distinct().count()
            
            # Count news chunks
            news_chunks = db.query(NewsChunk).count()
            news_with_embeddings = db.query(NewsChunk.news_id).distinct().count()
            
            # Get chunk type breakdown
            from sqlalchemy import func
            chunk_types = db.query(
                DocumentChunk.chunk_type,
                func.count(DocumentChunk.id).label('count')
            ).group_by(DocumentChunk.chunk_type).all()
            
            return {
                'total_document_chunks': doc_chunks,
                'earnings_calls_with_embeddings': earnings_calls_with_embeddings,
                'total_news_chunks': news_chunks,
                'news_articles_with_embeddings': news_with_embeddings,
                'chunk_types': {ct.chunk_type: ct.count for ct in chunk_types},
                'embedding_model': self.model_name,
                'embedding_dimension': 384 if 'MiniLM-L6-v2' in self.model_name else 'unknown'
            }
            
        except Exception as e:
            logger.error(f"Error getting embedding stats: {e}")
            return {}
        finally:
            db.close()


def main():
    """Test the RAG service"""
    rag = PostgresRAGService()
    
    if not rag.embedder:
        print("❌ Embedding model not available")
        return
    
    stats = rag.get_embedding_stats()
    print("📊 Current RAG Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Test search if we have data
    if stats.get('total_document_chunks', 0) > 0:
        print("\n🔍 Testing semantic search...")
        results = rag.semantic_search("revenue growth", limit=3)
        
        for i, result in enumerate(results, 1):
            print(f"\n{i}. [{result['source']}] {result['company_symbol']} - Similarity: {result['similarity']:.3f}")
            print(f"   Content: {result['content'][:100]}...")


if __name__ == "__main__":
    main()