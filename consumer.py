from confluent_kafka import Consumer, Producer
import json
import time
import google.generativeai as genai

# 1. Read configuration
def read_config():
    config = {}
    google_key = None
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            # Skip comments and empty lines
            if len(line) != 0 and line[0] != "#" and "=" in line:
                parameter, value = line.strip().split('=', 1)
                if parameter.strip() == "google.api.key":
                    google_key = value.strip()
                else:
                    config[parameter.strip()] = value.strip()
    
    # Consumer specific settings
    consumer_config = config.copy()
    consumer_config["group.id"] = "stream-refinery-group"
    consumer_config["auto.offset.reset"] = "earliest"
    
    # Producer settings
    producer_config = config.copy()
    
    return consumer_config, producer_config, google_key

# 2. Setup Google AI and Kafka
consumer_config, producer_config, google_api_key = read_config()

if not google_api_key:
    print("❌ Error: google.api.key not found")
    exit(1)

genai.configure(api_key=google_api_key)

try:
    model = genai.GenerativeModel('gemini-2.5-flash')
    print("✅ Gemini 2.5 Flash connected.")
except Exception as e:
    print(f"⚠️ Model connection error (using fallback): {e}")
    model = genai.GenerativeModel('gemini-1.5-flash')

# 3. Connect to Confluent Consumer
consumer = Consumer(consumer_config)
consumer.subscribe(["raw-data"])
producer = Producer(producer_config)

# Callback for delivery confirmation
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"📤 Saved to clean-data: {msg.topic()} [{msg.partition()}]")

print("👀 Stream Refinery Active... (Ctrl+C to exit)")

try:
    while True:
        # Poll for message (1.0s timeout)
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue

        # Decode raw data
        raw_json = msg.value().decode('utf-8')
        print(f"\n📥 Input: {raw_json}")

        # 4. AI Processing
        try:
            prompt = f"""
            Act as a Data Engineer. Processing a raw data stream.
            
            Rules:
            1. PRESERVE all original fields (transaction_id, amount, timestamp).
            2. FIX typos in 'user_location' (e.g., "San Fran" -> "San Francisco").
            3. FIX typos in 'product_name' (e.g., "Lptop" -> "Laptop").
            4. ADD field "status": "enriched".
            5. Output strictly VALID JSON. No duplicates.

            Input JSON: {raw_json}
            """
            
            response = model.generate_content(prompt)
            clean_json_str = response.text.replace("```json", "").replace("```", "").strip()
            
            # Validate JSON before sending
            json.loads(clean_json_str) 
            
            print(f"✨ AI Cleaned: {clean_json_str}")

            # 5. Produce to 'clean-data'
            producer.produce(
                "clean-data",
                value=clean_json_str,
                callback=delivery_report
            )
            producer.poll(0) # Trigger callback

        except Exception as e:
            print(f"⚠️ Processing Error: {e}")

        # Sleep to respect Free Tier rate limits (approx 15 RPM)
        time.sleep(5)

except KeyboardInterrupt:
    print("Stopping...")
finally:
    consumer.close()
    producer.flush()