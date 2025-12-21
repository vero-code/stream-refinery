from confluent_kafka import Consumer
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
    config["group.id"] = "stream-refinery-group"
    config["auto.offset.reset"] = "earliest"
    return config, google_key

# 2. Configure Gemini 2.5 Flash
config_kafka, google_api_key = read_config()

if not google_api_key:
    print("❌ Error: google.api.key not found in client.properties")
    exit(1)

genai.configure(api_key=google_api_key)

try:
    model = genai.GenerativeModel('gemini-2.5-flash')
    print("✅ Gemini 2.5 Flash connected successfully.")
except Exception as e:
    print(f"⚠️ Model connection error (using fallback): {e}")
    model = genai.GenerativeModel('gemini-1.5-flash')

# 3. Connect to Confluent Consumer
consumer = Consumer(config_kafka)
consumer.subscribe(["raw-data"])

print("👀 Listening for data stream... (Ctrl+C to exit)")

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

        # 4. Send to AI for cleaning
        try:
            prompt = f"""
            Act as a Data Engineer. Fix this JSON data.
            Task:
            1. Correct typos in 'user_location' (e.g., "Nw York" -> "New York").
            2. Standardize 'product_name' (e.g., "Lptop" -> "Laptop").
            3. Add a field "ai_model": "gemini-2.5-flash".
            4. Return ONLY the valid JSON string. No markdown formatting.

            Input JSON: {raw_json}
            """
            
            response = model.generate_content(prompt)
            
            # Clean response from markdown tags if present
            clean_json = response.text.replace("```json", "").replace("```", "").strip()
            
            print(f"✨ Output (Gemini 2.5): {clean_json}")

        except Exception as e:
            print(f"⚠️ AI Error: {e}")

        # Sleep to respect Free Tier rate limits (approx 15 RPM)
        time.sleep(5)

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")
finally:
    consumer.close()