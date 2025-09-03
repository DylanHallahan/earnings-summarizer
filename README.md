# Earnings Summarizer

A Python application for summarizing earnings calls and financial news using RAG (Retrieval-Augmented Generation) technology.

## Features

- Earnings call transcript processing and summarization
- News article analysis and summarization
- RAG-based question answering
- Database integration for storing and retrieving financial data
- API client for fetching financial data

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure your environment variables in `.env`

3. Run the application:
   ```bash
   python main.py
   ```

## Usage

The application provides various modules for:
- Loading and processing financial data (`load_data.py`)
- Setting up new tickers (`setup_ticker.py`)
- Summarizing earnings calls (`earnings_summarizer.py`)
- Processing news articles (`news_summarizer.py`)
- Managing the system (`manage.py`)

## Configuration

Configuration settings can be found in the `config/` directory:
- `settings.py` - Application settings
- `database.py` - Database configuration

## Testing

Run tests using:
```bash
python test_news_api.py
python test_rag_setup.py
```