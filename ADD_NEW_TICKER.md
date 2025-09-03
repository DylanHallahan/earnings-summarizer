# How to Add New Tickers to Your Financial Research Platform

## Quick Command (Recommended)

To add a new ticker with complete setup (company info + stock data + financial KPIs + earnings calls):

```bash
cd /home/aiuser/earnings-summarizer
source venv/bin/activate
python manage.py full <SYMBOL> "<COMPANY_NAME>" --sector "<SECTOR>" --industry "<INDUSTRY>" --years <YEARS>
```

### Examples:

```bash
# Add NVIDIA with 3 years of data
python manage.py full NVDA "NVIDIA Corporation" --sector "Technology" --industry "Semiconductors" --years 3

# Add Netflix with default 2 years of data
python manage.py full NFLX "Netflix Inc." --sector "Communication Services" --industry "Entertainment"

# Add Johnson & Johnson  
python manage.py full JNJ "Johnson & Johnson" --sector "Healthcare" --industry "Drug Manufacturers"

# Add Coca-Cola
python manage.py full KO "The Coca-Cola Company" --sector "Consumer Defensive" --industry "Beverages"
```

## What This Does Automatically:

✅ **Company Database Entry**: Adds company to your PostgreSQL database  
✅ **Stock Price Data**: Downloads 2 years of historical OHLCV data  
✅ **Financial KPIs**: Fetches real P/E, P/S, P/B ratios, EPS, market cap  
✅ **Earnings Calls**: Downloads recent earnings transcripts and AI summaries
✅ **Website Integration**: Makes company appear in your frontend immediately  
✅ **API Endpoints**: All endpoints (`/api/companies`, `/api/stock-prices/<symbol>`, `/api/financial-metrics/<symbol>`, `/api/earnings/<symbol>`) work instantly  

## Alternative Commands

Check ticker status:
```bash
python manage.py status <SYMBOL>
```

List all companies or tables:
```bash
python manage.py list --companies
python manage.py list --tables  
```

Company info only:
```bash
python manage.py company <SYMBOL> "<COMPANY_NAME>" "<SECTOR>" "<INDUSTRY>" "<MARKET_CAP>"
```

## Individual Component Commands (Advanced)

The manage.py script provides individual commands for specific tasks:

### Stock Data Only
```bash
python manage.py stock <SYMBOL>
```

### Financial Metrics Only  
```bash
python manage.py financial <SYMBOL>
```

### Earnings Calls Only
```bash 
python manage.py earnings <SYMBOL> --years <YEARS>
```

## Verification

After adding a new ticker, test it:

```bash
# Check status and data completeness
python manage.py status SYMBOL

# Test web endpoints
curl http://localhost:8000/api/companies | grep SYMBOL
curl http://localhost:8000/api/financial-metrics/SYMBOL | python3 -m json.tool
curl http://localhost:8000/api/stock-prices/SYMBOL | python3 -m json.tool | head -20
curl http://localhost:8000/api/earnings/SYMBOL | python3 -m json.tool
```

## Common Sectors & Industries

**Technology:**
- Software - Application
- Software - Infrastructure  
- Semiconductors
- Information Technology Services
- Electronic Components

**Healthcare:**
- Drug Manufacturers - Major
- Medical Devices
- Biotechnology
- Healthcare Plans

**Financial Services:**
- Banks - Regional
- Credit Services
- Asset Management
- Insurance - Life

**Consumer:**
- Consumer Cyclical: Retail, Auto, Travel
- Consumer Defensive: Food, Beverages, Household

## Troubleshooting

**"No data available"**: Some tickers may not have complete data in defeat-beta API  
**Database errors**: Ensure PostgreSQL is running and connection settings are correct  
**API timeouts**: defeat-beta API can be slow, this is normal  
**Missing earnings calls**: Not all companies have earnings transcripts in the system

## Data Sources

- **Stock Prices**: defeat-beta API (Hugging Face datasets)
- **Financial Ratios**: defeat-beta API (P/E, P/S, P/B, EPS)  
- **Market Cap**: defeat-beta API (real-time calculation)
- **Company Info**: Manual entry (you provide sector/industry)

Your platform now supports **any publicly traded stock** with real financial data!