import json
import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer
import finnhub
from dotenv import load_dotenv

load_dotenv()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key=os.getenv('FINNHUB_API_KEY'))

TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
TOPIC = 'stock_prices'

# Track base prices for mock data fallback
mock_prices = {
    'AAPL': 195.00,
    'GOOGL': 175.00,
    'MSFT': 420.00,
    'AMZN': 185.00,
    'TSLA': 170.00
}

def generate_mock_data(ticker):
    """Fallback when API is unavailable"""
    base_price = mock_prices[ticker]
    change_pct = random.uniform(-0.02, 0.02)
    price = round(base_price * (1 + change_pct), 2)

    message = {
        'ticker': ticker,
        'price': price,
        'high': round(price * (1 + random.uniform(0, 0.01)), 2),
        'low': round(price * (1 - random.uniform(0, 0.01)), 2),
        'open': base_price,
        'volume': random.randint(100000, 5000000),
        'timestamp': datetime.now().isoformat(),
        'source': 'mock'
    }
    mock_prices[ticker] = price
    return message

def fetch_real_data(ticker):
    """Fetch real-time quote from Finnhub"""
    quote = finnhub_client.quote(ticker)

    if quote['c'] == 0:
        return None

    return {
        'ticker': ticker,
        'price': quote['c'],      # current price
        'high': quote['h'],        # high of day
        'low': quote['l'],         # low of day
        'open': quote['o'],        # open price
        'volume': 0,               # quote endpoint doesn't provide volume
        'timestamp': datetime.now().isoformat(),
        'source': 'finnhub'
    }

def fetch_and_send():
    for ticker in TICKERS:
        try:
            # Try real data first
            message = fetch_real_data(ticker)

            if message is None:
                print(f"  {ticker}: API returned no data, using mock")
                message = generate_mock_data(ticker)

            producer.send(TOPIC, key=ticker, value=message)
            print(f"  Sent: {ticker} @ ${message['price']} [{message['source']}]")

        except Exception as e:
            # If API fails, fall back to mock
            print(f"  {ticker}: API error ({e}), using mock")
            message = generate_mock_data(ticker)
            producer.send(TOPIC, key=ticker, value=message)
            print(f"  Sent: {ticker} @ ${message['price']} [mock-fallback]")

    producer.flush()

if __name__ == '__main__':
    print("Starting stock producer...")
    print(f"API Key: {'configured' if os.getenv('FINNHUB_API_KEY') else 'MISSING'}")
    print(f"Tickers: {TICKERS}")
    print(f"Topic: {TOPIC}")
    print("-" * 50)

    while True:
        fetch_and_send()
        print(f"--- Batch sent at {datetime.now().strftime('%H:%M:%S')} ---")
        time.sleep(15)  # Every 15 seconds (stays within 60 calls/min limit)