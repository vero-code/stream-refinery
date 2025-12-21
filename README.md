# Stream Refinery 🌊🧠

**"Garbage In, Gold Out"** — Real-time data cleaning and enrichment using Confluent and Google Vertex AI.

## Project Goal
Standardize and fix "dirty" data streams (typos in locations, product names) on the fly using LLMs, transforming a chaotic raw stream into a high-quality analytical dataset without manual intervention.

## Architecture (The Onion)
1.  **Ingestion:** Python Producer generates mock transactions with intentional errors.
2.  **Transport:** Confluent Cloud (Kafka) streams the raw data.
3.  **Intelligence:** Google Vertex AI (Gemini) processes and cleans the JSON.
4.  **Output:** Clean data is pushed to a destination topic / displayed.

## Setup

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  Configure Environment:
    Create a `client.properties` file in the root directory. You need Confluent credentials and a Google AI Studio key:
    ```properties
    # Confluent Settings
    bootstrap.servers=YOUR_BOOTSTRAP_SERVER
    security.protocol=SASL_SSL
    sasl.mechanisms=PLAIN
    sasl.username=YOUR_KAFKA_API_KEY
    sasl.password=YOUR_KAFKA_API_SECRET

    # Google AI Settings
    google.api.key=YOUR_GOOGLE_AI_KEY
    ```

3.  Run the pipeline:
    
    **Terminal 1 (Data Source):**
    ```bash
    python producer.py
    ```

    **Terminal 2 (AI Processor):**
    ```bash
    python consumer.py
    ```