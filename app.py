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

# Custom CSS to make JSON compact
st.markdown("""
<style>
    .element-container {margin-bottom: -1rem;}
    div.stCode {margin-bottom: 1rem;}
</style>
""", unsafe_allow_html=True)

# Title
st.title("🌊 Stream Refinery")
st.markdown("**Real-time AI Data Cleaning Pipeline** powered by Confluent & Gemini 2.5")
st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("📥 Raw Data Stream (Dirty)")
    st.caption("Listening to topic: raw-data")
    # Placeholder for the raw list
    raw_container = st.container()

with col2:
    st.subheader("✨ AI Cleaned Data (Enriched)")
    st.caption("Listening to topic: clean-data")
    # Placeholder for the clean list
    clean_container = st.container()

# --- INITIALIZE SESSION STATE (HISTORY) ---
if 'raw_history' not in st.session_state:
    st.session_state.raw_history = []
if 'clean_history' not in st.session_state:
    st.session_state.clean_history = []

# --- CONFIGURATION ---
def read_config():
    config = {}
    if os.path.exists("client.properties"):
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#" and "=" in line:
                    parameter, value = line.strip().split('=', 1)
                    if parameter.strip() != "google.api.key": 
                        config[parameter.strip()] = value.strip()
    return config

# --- MAIN APP ---
try:
    config = read_config()
    config["group.id"] = "streamlit-viewer-history-v3"
    config["auto.offset.reset"] = "latest"
    
    consumer = Consumer(config)
    consumer.subscribe(["raw-data", "clean-data"])

    # Status Notification
    st.toast("✅ Connected to Confluent Cloud. Waiting for stream...", icon="🟢")

    # --- LIVE LOOP ---
    while True:
        msg = consumer.poll(0.2)
        
        if msg is None: continue
        if msg.error(): continue

        topic = msg.topic()
        value = msg.value().decode('utf-8')
        
        # 1. RAW DATA LOGIC
        if topic == "raw-data":
            try:
                data = json.loads(value)
                st.session_state.raw_history.insert(0, data)
                st.session_state.raw_history = st.session_state.raw_history[:4]
                
                # Render the list
                with raw_container:
                    raw_container.empty() 
                    for item in st.session_state.raw_history:
                        st.code(json.dumps(item, indent=2), language="json")
            except:
                pass
        
        # 2. CLEAN DATA LOGIC
        elif topic == "clean-data":
            try:
                data = json.loads(value)
                st.session_state.clean_history.insert(0, data)
                st.session_state.clean_history = st.session_state.clean_history[:4]
                
                with clean_container:
                    clean_container.empty()
                    for item in st.session_state.clean_history:
                        st.json(item)
                        st.markdown("---")
            except:
                pass
        
        time.sleep(0.1)

except Exception as e:
    st.error(f"Connection Error: {e}")