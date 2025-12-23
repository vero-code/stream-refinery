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

# Custom CSS
st.markdown("""
<style>
    .stApp { background-color: #0E1117; color: #FAFAFA; }
    .metric-card { background-color: #262730; padding: 15px; border-radius: 10px; border: 1px solid #41424C; }
    .json-box { background-color: #1E1E1E; padding: 10px; border-radius: 5px; font-family: 'Courier New', monospace; font-size: 12px; }
    .status-badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; }
    .status-clean { background-color: #1B5E20; color: #81C784; }
    .status-dirty { background-color: #B71C1C; color: #EF9A9A; }
</style>
""", unsafe_allow_html=True)

# --- SESSION STATE INITIALIZATION ---
if 'raw_history' not in st.session_state:
    st.session_state.raw_history = []
if 'clean_history' not in st.session_state:
    st.session_state.clean_history = []
if 'is_running' not in st.session_state:
    st.session_state.is_running = False

# --- SIDEBAR CONTROLS ---
st.sidebar.image("https://img.icons8.com/fluency/96/data-configuration.png", width=80)
st.sidebar.title("Configuration")

run_mode = st.sidebar.radio(
    "Data Source Mode:",
    ("🟢 Live Demo (Simulation)", "🔴 Real Kafka Stream"),
    index=0,
    help="Use Simulation for judging/testing if Kafka is offline."
)

st.sidebar.divider()
st.sidebar.info("Stream Refinery processes raw data streams using Google Gemini 2.5 to fix typos and standardize formats in real-time.")

# --- TITLE ---
c1, c2 = st.columns([1, 4])
with c1:
    st.image("https://img.icons8.com/color/144/google-cloud-platform.png", width=90)
with c2:
    st.title("Stream Refinery")
    st.caption("Real-time AI Data Cleaning Pipeline powered by Confluent & Google Cloud")
st.markdown("---")

if not st.session_state.is_running:
    if st.sidebar.button("▶️ Start Stream", type="primary"):
        st.session_state.is_running = True
        st.rerun()
else:
    if st.sidebar.button("⏹️ Stop Stream", type="secondary"):
        st.session_state.is_running = False
        st.rerun()

if st.session_state.is_running:
    st.sidebar.success("Status: 🟢 Running")
else:
    st.sidebar.warning("Status: 🔴 Stopped")

status_message = st.empty()

col1, col2 = st.columns(2)

with col1:
    st.subheader("📥 Raw Data Stream (Dirty)")
    st.caption("Listening to topic: raw-data")
    raw_container = st.container()

with col2:
    st.subheader("✨ AI Cleaned Data (Enriched)")
    st.caption("Listening to topic: clean-data")
    clean_container = st.container()

# --- CONFIGURATION ---
def read_config():
    config = {}
    
    # 1. Try loading from Streamlit Secrets (Cloud)
    try:
        if "kafka" in st.secrets:
            config.update(dict(st.secrets["kafka"]))
        if "google" in st.secrets:
            os.environ["GOOGLE_API_KEY"] = st.secrets["google"]["api_key"]
    except (FileNotFoundError, KeyError):
        pass

    # 2. If config is still empty, read local file (Local)
    if not config and os.path.exists("client.properties"):
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#" and "=" in line:
                    parameter, value = line.strip().split('=', 1)
                    if parameter.strip() == "google.api.key":
                        os.environ["GOOGLE_API_KEY"] = value.strip()
                    else:
                        config[parameter.strip()] = value.strip()
    return config

# --- MAIN APP ---
if not st.session_state.is_running:
    status_message.info("👋 The stream is currently stopped. Click 'Start Stream' in the sidebar to connect.")
    
    with raw_container:
        for item in st.session_state.raw_history:
            st.code(json.dumps(item, indent=2), language="json")
    with clean_container:
        for item in st.session_state.clean_history:
            st.json(item)
            st.markdown("---")

else:
    status_message.empty()

    config = read_config()

    if "group.id" not in config: config["group.id"] = "streamlit-viewer-local-dev"
    if "auto.offset.reset" not in config: config["auto.offset.reset"] = "latest"

    if "bootstrap.servers" in config:
        try:
            consumer = Consumer(config)
            consumer.subscribe(["raw-data", "clean-data"])
            st.toast("✅ Connected to Confluent Cloud. Waiting for stream...", icon="🟢")

            # --- LIVE LOOP ---
            while st.session_state.is_running:
                msg = consumer.poll(0.2)
                
                if msg is None: continue
                if msg.error(): 
                    st.error(f"Kafka Error: {msg.error()}")
                    continue

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
            
            consumer.close()

        except Exception as e:
            st.error(f"Connection Error: {e}")
            st.session_state.is_running = False
    else:
        st.error("❌ Configuration missing.")
        st.session_state.is_running = False