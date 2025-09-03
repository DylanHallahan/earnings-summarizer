#!/usr/bin/env python3

"""
Test script to verify RAG implementation is working correctly
"""

import sys
from loguru import logger

sys.path.append('.')


def test_database_connection():
    """Test basic database connectivity"""
    print("🔌 Testing database connection...")
    try:
        from config.database import test_connection, init_database
        
        if test_connection():
            print("✅ Database connection successful")
            
            # Try to create tables
            init_database()
            print("✅ Database tables initialized")
            return True
        else:
            print("❌ Database connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Database test error: {e}")
        return False


def test_pgvector_extension():
    """Test if pgvector extension is available"""
    print("\n🧩 Testing pgvector extension...")
    try:
        import psycopg2
        from config.settings import settings
        
        conn = psycopg2.connect(
            host='192.168.1.175',
            port=5432,
            database='earnings_db',
            user='aiuser',
            password='Wahine123!'
        )
        cursor = conn.cursor()
        
        # Check if vector extension exists
        cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector';")
        result = cursor.fetchone()
        
        if result:
            print("✅ pgvector extension is installed")
            
            # Test vector operations
            cursor.execute("SELECT '[1,2,3]'::vector <-> '[4,5,6]'::vector as distance;")
            distance = cursor.fetchone()[0]
            print(f"✅ Vector operations working (test distance: {distance:.3f})")
            
            cursor.close()
            conn.close()
            return True
        else:
            print("❌ pgvector extension not found")
            cursor.close()
            conn.close()
            return False
            
    except Exception as e:
        print(f"❌ pgvector test error: {e}")
        return False


def test_embedding_model():
    """Test if embedding model loads correctly"""
    print("\n🤖 Testing embedding model...")
    try:
        from src.rag_service import PostgresRAGService
        
        rag = PostgresRAGService()
        
        if rag.embedder:
            print(f"✅ Embedding model loaded: {rag.model_name}")
            
            # Test embedding generation
            test_text = "This is a test sentence for embedding."
            embedding = rag.embedder.encode([test_text])[0]
            print(f"✅ Embedding generation working (dim: {len(embedding)})")
            return True
        else:
            print("❌ Failed to load embedding model")
            return False
            
    except Exception as e:
        print(f"❌ Embedding model test error: {e}")
        return False


def test_data_availability():
    """Check what data is available for embeddings"""
    print("\n📊 Checking available data...")
    try:
        from config.database import SessionLocal, EarningsCall, FinancialNews
        
        db = SessionLocal()
        
        # Count earnings calls
        earnings_count = db.query(EarningsCall).filter(EarningsCall.raw_transcript.isnot(None)).count()
        print(f"📄 Earnings calls with transcripts: {earnings_count}")
        
        # Count news articles
        news_count = db.query(FinancialNews).filter(
            FinancialNews.content.isnot(None),
            FinancialNews.content != ""
        ).count()
        print(f"📰 News articles with content: {news_count}")
        
        if earnings_count > 0 or news_count > 0:
            # Show sample companies
            companies = db.query(EarningsCall.company_symbol).distinct().limit(5).all()
            company_list = [c[0] for c in companies]
            print(f"📈 Sample companies: {', '.join(company_list)}")
            
        db.close()
        return earnings_count > 0 or news_count > 0
        
    except Exception as e:
        print(f"❌ Data availability check error: {e}")
        return False


def test_rag_search():
    """Test semantic search functionality"""
    print("\n🔍 Testing semantic search...")
    try:
        from src.rag_service import PostgresRAGService
        
        rag = PostgresRAGService()
        
        if not rag.embedder:
            print("⚠️  Skipping search test - embedding model not available")
            return False
        
        # Get current embedding stats
        stats = rag.get_embedding_stats()
        print(f"📊 Current embeddings: {stats.get('total_document_chunks', 0)} document chunks, {stats.get('total_news_chunks', 0)} news chunks")
        
        if stats.get('total_document_chunks', 0) == 0 and stats.get('total_news_chunks', 0) == 0:
            print("⚠️  No embeddings found - run backfill first")
            return False
        
        # Test search
        test_query = "revenue growth"
        results = rag.semantic_search(test_query, limit=2)
        
        if results:
            print(f"✅ Search returned {len(results)} results for '{test_query}'")
            for i, result in enumerate(results, 1):
                print(f"   {i}. [{result['source']}] {result.get('company_symbol', 'N/A')} - Similarity: {result['similarity']:.3f}")
                print(f"      {result['content'][:80]}...")
            return True
        else:
            print("⚠️  Search returned no results")
            return False
            
    except Exception as e:
        print(f"❌ RAG search test error: {e}")
        return False


def main():
    """Run all tests"""
    print("🧪 RAG Setup Test Suite")
    print("=" * 50)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("pgvector Extension", test_pgvector_extension),
        ("Embedding Model", test_embedding_model),
        ("Data Availability", test_data_availability),
        ("RAG Search", test_rag_search),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"📋 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! RAG implementation is ready.")
        print("\n📚 Next steps:")
        print("   1. Run: python backfill_embeddings.py --type earnings --limit 5")
        print("   2. Test chat with RAG via web API")
        print("   3. Add more embeddings with: python backfill_embeddings.py --type all")
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        
        if not test_pgvector_extension():
            print("\n💡 To fix pgvector issues:")
            print("   Run on database server: CREATE EXTENSION IF NOT EXISTS vector;")


if __name__ == "__main__":
    main()