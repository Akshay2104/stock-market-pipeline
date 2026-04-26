import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'stock_prices'

# Base prices - we'll simulate movement from these
STOCKS = {
    'AAPL': 195.00,
    'GOOGL': 175.00,
    'MSFT': 420.00,
    'AMZN': 185.00,
    'TSLA': 170.00
}

def generate_stock_data(ticker, base_price):
    # Simulate realistic price movement (±2%)
    change_pct = random.uniform(-0.02, 0.02)
    price = round(base_price * (1 + change_pct), 2)
    high = round(price * (1 + random.uniform(0, 0.01)), 2)
    low = round(price * (1 - random.uniform(0, 0.01)), 2)
    volume = random.randint(100000, 5000000)

    return {
        'ticker': ticker,
        'price': price,
        'high': high,
        'low': low,
        'open': base_price,
        'volume': volume,
        'timestamp': datetime.now().isoformat()
    }

def fetch_and_send():
    for ticker, base_price in STOCKS.items():
        message = generate_stock_data(ticker, base_price)

        producer.send(
            TOPIC,
            key=ticker,
            value=message
        )
        print(f"Sent: {ticker} @ ${message['price']}")

        # Update base price for next round to simulate trend
        STOCKS[ticker] = message['price']

    producer.flush()

if __name__ == '__main__':
    print("Starting stock producer (mock data)...")
    while True:
        fetch_and_send()
        print(f"--- Batch sent at {datetime.now().strftime('%H:%M:%S')} ---")
        time.sleep(10)  # Every 10 seconds for faster testing