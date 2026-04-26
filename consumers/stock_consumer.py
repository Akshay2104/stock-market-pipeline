import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='stock-verify',
    key_deserializer=lambda k: k.decode('utf-8'),
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for stock prices...")
for message in consumer:
    print(f"Partition: {message.partition} | Key: {message.key} | Price: ${message.value['price']} | Time: {message.value['timestamp']}")