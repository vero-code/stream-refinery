import streamlit as st
from confluent_kafka import Consumer
import json
import time
import os

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Stream Refinery",
    page_icon="🌊",
    layout="wide"
)

# Title and Description
st.title("🌊 Stream Refinery")
st.markdown("**Real-time AI Data Cleaning Pipeline** powered by Confluent & Gemini 2.5")
st.markdown("---")

# Layout: Split screen into two columns
col1, col2 = st.columns(2)

with col1:
    st.subheader("📥 Raw Data Stream (Dirty)")
    st.caption("Listening to topic: raw-data")
    raw_placeholder = st.empty()

with col2:
    st.subheader("✨ AI Cleaned Data (Enriched)")
    st.caption("Listening to topic: clean-data")
    clean_placeholder = st.empty()

# --- CONFIGURATION SETUP ---
def read_config():
    config = {}
    # Check if local file exists
    if os.path.exists("client.properties"):
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#" and "=" in line:
                    parameter, value = line.strip().split('=', 1)
                    if parameter.strip() != "google.api.key": 
                        config[parameter.strip()] = value.strip()
    return config

# --- KAFKA CONSUMER SETUP ---
try:
    config = read_config()
    # Unique group ID for the frontend to read independently
    config["group.id"] = "streamlit-viewer-group-v2" 
    config["auto.offset.reset"] = "latest" # Read only new messages to look "live"
    
    consumer = Consumer(config)
    
    # Subscribe to BOTH topics to visualize the pipeline
    consumer.subscribe(["raw-data", "clean-data"])

    # Visual indicator of connection
    st.success("✅ Connected to Confluent Cloud. Waiting for live data...")

    # --- MAIN LOOP (AUTO-START) ---
    while True:
        msg = consumer.poll(0.5)
        
        if msg is None: 
            continue
        if msg.error():
            continue

        # Determine topic and decode value
        topic = msg.topic()
        value = msg.value().decode('utf-8')
        
        # Update UI based on topic
        if topic == "raw-data":
            with raw_placeholder.container():
                # Display Raw Data as Code Block for "tech" feel
                try:
                    json_data = json.loads(value)
                    st.code(json.dumps(json_data, indent=2), language="json")
                except:
                    st.text(value)
        
        elif topic == "clean-data":
            with clean_placeholder.container():
                # Display Clean Data as interactive JSON
                try:
                    json_data = json.loads(value)
                    st.json(json_data)
                except:
                    st.text(value)
        
        # Slight delay to prevent UI flickering
        time.sleep(0.2)

except Exception as e:
    st.error(f"Connection Error: {e}")
    st.info("Tip: Ensure client.properties is present and correct.")