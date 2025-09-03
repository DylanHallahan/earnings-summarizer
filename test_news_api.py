#!/usr/bin/env python3

from defeatbeta_api.data.ticker import Ticker
import pandas as pd

def test_news_api():
    """Test the defeat-beta news API to understand data structure"""
    
    # Test with Apple
    ticker = Ticker('AAPL')
    news_obj = ticker.news()
    
    # Get news list
    news_list = news_obj.get_news_list()
    print("News list columns:", list(news_list.columns))
    print("Number of news articles:", len(news_list))
    
    if len(news_list) > 0:
        print("\nFirst article info:")
        first_row = news_list.iloc[0]
        print("UUID:", first_row['uuid'])
        print("Title:", first_row['title'])
        print("Publisher:", first_row['publisher'])
        print("Date:", first_row['report_date'])
        print("Link:", first_row['link'])
        
        # Get detailed news content
        try:
            first_news_content = news_obj.get_news(first_row['uuid'])
            print("\nDetailed content type:", type(first_news_content))
            if hasattr(first_news_content, 'columns'):
                print("Content columns:", list(first_news_content.columns))
                if len(first_news_content) > 0:
                    print("First paragraph:", first_news_content.iloc[0])
        except Exception as e:
            print("Error getting detailed content:", e)
    
    return news_list

if __name__ == "__main__":
    test_news_api()