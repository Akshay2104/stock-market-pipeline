import json
import time
import random
import os
import logging
from datetime import datetime
from kafka import KafkaProducer
import finnhub
from dotenv import load_dotenv

load_dotenv()

# ============ LOGGING ============
# Set up logging so errors are recorded with timestamps
# This is better than just print() — logs can be shipped to CloudWatch in production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ KAFKA PRODUCER ============
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ============ FINNHUB CLIENT ============
finnhub_client = finnhub.Client(api_key=os.getenv('FINNHUB_API_KEY'))

TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
TOPIC = 'stock_prices'
DEAD_LETTER_TOPIC = 'stock_prices_dead_letter'
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds — doubles with each retry (exponential backoff)

# Track last known good prices for stale data serving
last_known_prices = {
    'AAPL': None,
    'GOOGL': None,
    'MSFT': None,
    'AMZN': None,
    'TSLA': None
}

# Track base prices for mock fallback
mock_prices = {
    'AAPL': 195.00,
    'GOOGL': 175.00,
    'MSFT': 420.00,
    'AMZN': 185.00,
    'TSLA': 170.00
}

def generate_mock_data(ticker):
    """Mock data generator — only used during development, never in production"""
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

def fetch_with_retry(ticker):
    """
    Fetch real stock data from Finnhub with exponential backoff retry logic.
    Returns message dict on success, None on all retries exhausted.
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            quote = finnhub_client.quote(ticker)

            # Finnhub returns 0 for all fields when market is closed or data unavailable
            if quote['c'] == 0:
                logger.warning(f"{ticker}: API returned zero price (market closed or unavailable)")
                return None

            message = {
                'ticker': ticker,
                'price': quote['c'],
                'high': quote['h'],
                'low': quote['l'],
                'open': quote['o'],
                'volume': 0,
                'timestamp': datetime.now().isoformat(),
                'source': 'finnhub'
            }

            # Update last known good price on success
            last_known_prices[ticker] = message
            logger.info(f"  Sent: {ticker} @ ${message['price']} [finnhub]")
            return message

        except Exception as e:
            last_error = str(e)
            # Calculate wait time — doubles with each retry
            # Attempt 1 fails → wait 2s
            # Attempt 2 fails → wait 4s
            # Attempt 3 fails → wait 8s
            wait_time = RETRY_BASE_DELAY ** attempt

            if attempt < MAX_RETRIES:
                logger.warning(f"  {ticker}: Attempt {attempt} failed ({e}). Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"  {ticker}: All {MAX_RETRIES} attempts failed. Last error: {e}")

    return None  # all retries exhausted

def send_to_dead_letter(ticker, error, retry_count):
    """
    Send failed fetch record to dead letter topic.
    Captures full context for later investigation and reprocessing.
    """
    dead_letter_record = {
        'ticker': ticker,
        'timestamp': datetime.now().isoformat(),
        'error': type(error).__name__ if error else 'ZeroPrice',
        'error_message': str(error) if error else 'API returned zero price',
        'retry_count': retry_count,
        'source': 'finnhub_producer',
        # Store last known good price so we know what data is missing
        'last_known_price': last_known_prices[ticker]
    }

    producer.send(
        DEAD_LETTER_TOPIC,
        key=ticker,
        value=dead_letter_record
    )
    logger.error(f"  {ticker}: Sent to dead letter topic — {dead_letter_record['error']}")

def fetch_and_send():
    for ticker in TICKERS:
        # Try to fetch real data with retry logic
        message = fetch_with_retry(ticker)

        if message is not None:
            # Success — send to main topic
            producer.send(TOPIC, key=ticker, value=message)

        else:
            # All retries exhausted — send to dead letter topic
            # DO NOT send mock data — a gap is better than fake data
            send_to_dead_letter(ticker, None, MAX_RETRIES)
            logger.error(f"  {ticker}: Data gap recorded. No data sent to main topic.")

    producer.flush()

if __name__ == '__main__':
    logger.info("Starting stock producer...")
    logger.info(f"API Key: {'configured' if os.getenv('FINNHUB_API_KEY') else 'MISSING'}")
    logger.info(f"Tickers: {TICKERS}")
    logger.info(f"Main topic: {TOPIC}")
    logger.info(f"Dead letter topic: {DEAD_LETTER_TOPIC}")
    logger.info(f"Max retries: {MAX_RETRIES}")
    logger.info("-" * 50)

    while True:
        fetch_and_send()
        logger.info(f"--- Batch complete at {datetime.now().strftime('%H:%M:%S')} ---")
        time.sleep(15)