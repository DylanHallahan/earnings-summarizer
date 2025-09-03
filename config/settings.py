import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings:
    # Database Configuration
    DATABASE_URL = os.getenv("DATABASE_URL")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "earnings_db")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    # Ollama Configuration
    OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
    
    # Processing Configuration
    MAX_CHUNK_SIZE = int(os.getenv("MAX_CHUNK_SIZE", "3000"))
    CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "100"))
    
    # Updated Summary Types - Streamlined to 3 categories
    SUMMARY_TYPES = {
        'executive': {
            'name': 'Executive Summary',
            'description': 'Comprehensive strategic overview of the earnings call',
            'max_tokens': 600,
            'priority': 1
        },
        'forward_guidance': {
            'name': 'Forward Guidance',
            'description': 'Combined outlook, guidance, and strategic initiatives',
            'max_tokens': 500,
            'priority': 2
        },
        'outlook_sentiment': {
            'name': 'Outlook Sentiment',
            'description': 'One word sentiment summary',
            'max_tokens': 10,
            'priority': 3
        }
    }
    
    # Analysis Keywords - Updated for new categories
    FORWARD_GUIDANCE_KEYWORDS = [
        'guidance', 'outlook', 'expect', 'forecast', 'project', 'anticipate',
        'looking ahead', 'going forward', 'next quarter', 'fiscal year',
        'full year', 'remainder of', 'second half', 'target', 'goal',
        'strategy', 'initiative', 'investment', 'expansion', 'acquisition',
        'product launch', 'market entry', 'partnership', 'innovation',
        'transformation', 'restructuring', 'efficiency', 'digital',
        'technology', 'platform', 'capability', 'footprint'
    ]
    
    SENTIMENT_KEYWORDS = {
        'positive': [
            'optimistic', 'confident', 'strong', 'growth', 'momentum', 
            'pleased', 'excited', 'bullish', 'opportunity', 'progress',
            'improved', 'solid', 'robust', 'excellent'
        ],
        'negative': [
            'challenging', 'difficult', 'headwinds', 'pressure', 'decline',
            'concerned', 'cautious', 'weakness', 'slowdown', 'uncertainty',
            'disappointing', 'volatile', 'risk', 'struggle'
        ],
        'neutral': [
            'stable', 'steady', 'mixed', 'balanced', 'moderate',
            'consistent', 'maintaining', 'monitoring', 'watchful'
        ]
    }
    
    MANAGEMENT_TITLES = [
        'ceo:', 'chief executive', 'president:', 'cfo:', 'chief financial',
        'chairman:', 'founder:', 'co-ceo:', 'managing director:', 'coo:',
        'chief operating', 'chief technology', 'cto:', 'head of'
    ]
    
    # Data Loading Configuration
    DEFAULT_START_YEAR = int(os.getenv("DEFAULT_START_YEAR", "2017"))
    
    # File and Directory Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"
    CONFIG_DIR = PROJECT_ROOT / "config"
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_ROTATION = os.getenv("LOG_ROTATION", "10 MB")
    LOG_RETENTION = os.getenv("LOG_RETENTION", "1 month")
    
    # API Configuration
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", "120"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
    
    # LLM Configuration
    LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.3"))
    LLM_TOP_P = float(os.getenv("LLM_TOP_P", "0.9"))
    
    @classmethod
    def get_database_url(cls):
        """Construct database URL from components if not provided directly"""
        if cls.DATABASE_URL:
            return cls.DATABASE_URL
        
        if not all([cls.DB_USER, cls.DB_PASSWORD, cls.DB_HOST, cls.DB_NAME]):
            raise ValueError("DATABASE_URL not provided and missing required database components")
        
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def create_directories(cls):
        """Create necessary directories if they don't exist"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.LOGS_DIR.mkdir(exist_ok=True)
        cls.CONFIG_DIR.mkdir(exist_ok=True)
        (cls.DATA_DIR / "raw").mkdir(exist_ok=True)
    
    @classmethod
    def validate_settings(cls):
        """Validate critical settings"""
        try:
            cls.get_database_url()
        except ValueError as e:
            raise ValueError(f"Database configuration invalid: {e}")
        
        return True

settings = Settings()

# Auto-create directories and validate on import
try:
    settings.create_directories()
    settings.validate_settings()
except Exception as e:
    print(f"Warning: Settings initialization issue: {e}")