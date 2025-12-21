from confluent_kafka import Producer
import json
import time
import random

# 1. Read configuration from client.properties
def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            # Skip empty lines, comments, or lines without '='
            if len(line) != 0 and line[0] != "#" and "=" in line:
                parameter, value = line.strip().split('=', 1)

                if parameter.strip() == "google.api.key":
                    continue

                config[parameter.strip()] = value.strip()
    return config

# 2. Create Producer
config = read_config()
producer = Producer(config)

TOPIC_NAME = "raw-data"

# Mock data generation
CITIES = ["New York", "Nw York", "NY", "New-York", "San Francisco", "San Fran", "S. Francisco"]
PRODUCTS = ["Laptop", "Lptop", "Phone", "Iphone 15", "Galaxy S24", "Headphones"]

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"📦 Produced to {msg.topic()}: {msg.value().decode('utf-8')}")

print("🚀 Starting dirty data generator... (Press Ctrl+C to stop)")

try:
    while True:
        # Generate random transaction with "dirty" data
        data = {
            "transaction_id": random.randint(1000, 9999),
            "user_location": random.choice(CITIES),  # Intentional typos
            "product_name": random.choice(PRODUCTS), # Intentional typos
            "amount": round(random.uniform(100.0, 2000.0), 2),
            "timestamp": time.time()
        }

        # Serialize to JSON and send
        producer.produce(
            TOPIC_NAME,
            key=str(data["transaction_id"]),
            value=json.dumps(data),
            on_delivery=delivery_report
        )
        
        # Flush to ensure delivery
        producer.flush()
        
        # Sleep to simulate real-time stream
        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Generator stopped.")