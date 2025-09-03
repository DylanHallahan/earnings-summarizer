from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Boolean, JSON, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from config.settings import settings
from pgvector.sqlalchemy import Vector

Base = declarative_base()

# Database Models
class Company(Base):
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), unique=True, nullable=False)
    name = Column(String(200), nullable=False)
    sector = Column(String(100))
    industry = Column(String(100))
    market_cap = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

class EarningsCall(Base):
    __tablename__ = "earnings_calls"
    
    id = Column(Integer, primary_key=True)
    company_symbol = Column(String(10), nullable=False)
    company_name = Column(String(200))
    quarter = Column(String(10), nullable=False)
    year = Column(Integer, nullable=False)
    call_date = Column(DateTime)
    transcript_url = Column(String(500))
    raw_transcript = Column(Text)
    processed_transcript = Column(Text)
    summary = Column(Text)
    key_metrics = Column(JSON)
    sentiment_score = Column(Float)
    word_count = Column(Integer)
    processing_status = Column(String(50), default='pending')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Summary(Base):
    __tablename__ = "summaries"
    
    id = Column(Integer, primary_key=True)
    earnings_call_id = Column(Integer, nullable=False)
    summary_type = Column(String(50))  # 'executive', 'financial', 'guidance', etc.
    content = Column(Text)
    confidence_score = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

# Database Setup
engine = create_engine(settings.get_database_url())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_database():
    """Create all database tables"""
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully")

def test_connection():
    """Test database connection"""
    try:
        connection = engine.connect()
        connection.close()
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False
    

class StockPrice(Base):
    __tablename__ = "stock_prices"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)
    open_price = Column(Float)
    close_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    volume = Column(Integer)

class FinancialMetrics(Base):
    __tablename__ = "financial_metrics"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False)
    pe_ratio = Column(Float)
    ps_ratio = Column(Float)
    pb_ratio = Column(Float)
    eps_ttm = Column(Float)
    revenue_ttm = Column(Float)
    gross_margin = Column(Float)
    net_margin = Column(Float)
    current_price = Column(Float)
    market_cap = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

class FinancialNews(Base):
    __tablename__ = "financial_news"
    
    id = Column(Integer, primary_key=True)
    uuid = Column(String(100), unique=True, nullable=False)
    symbol = Column(String(10), nullable=False)
    title = Column(String(500), nullable=False)
    content = Column(Text)
    publisher = Column(String(100))
    published_date = Column(Date)
    url = Column(String(1000))
    news_type = Column(String(50))
    related_symbols = Column(JSON)
    sentiment_score = Column(Float)
    processing_status = Column(String(50), default='pending')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class NewsSummary(Base):
    __tablename__ = "news_summaries"
    
    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, nullable=False)
    summary_type = Column(String(50))  # 'key_points', 'impact_analysis', 'sentiment'
    content = Column(Text)
    confidence_score = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

# RAG Embedding Tables
class DocumentChunk(Base):
    __tablename__ = "document_chunks"
    
    id = Column(Integer, primary_key=True)
    earnings_call_id = Column(Integer, ForeignKey("earnings_calls.id"), nullable=False)
    chunk_text = Column(Text, nullable=False)
    chunk_index = Column(Integer, nullable=False)
    chunk_type = Column(String(50), default='transcript')  # 'transcript', 'summary'
    embedding = Column(Vector(384))  # MiniLM-L6-v2 dimension
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to earnings call
    earnings_call = relationship("EarningsCall")

class NewsChunk(Base):
    __tablename__ = "news_chunks"
    
    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey("financial_news.id"), nullable=False)
    chunk_text = Column(Text, nullable=False)
    chunk_index = Column(Integer, nullable=False)
    embedding = Column(Vector(384))  # MiniLM-L6-v2 dimension
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to financial news
    news = relationship("FinancialNews")