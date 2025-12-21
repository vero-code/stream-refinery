# Stream Refinery 🌊🧠

**"Garbage In, Gold Out"** — Real-time data cleaning and enrichment using Confluent and Google Vertex AI.

## Project Goal
Standardize and fix "dirty" data streams (typos in locations, product names) on the fly using LLMs, transforming a chaotic raw stream into a high-quality analytical dataset without manual intervention.

## Architecture (The Onion)
1.  **Ingestion:** Python Producer generates mock transactions with intentional errors.
2.  **Transport:** Confluent Cloud (Kafka) streams the raw data.
3.  **Intelligence:** Google Vertex AI (Gemini) processes and cleans the JSON.
4.  **Output:** Clean data is pushed to a destination topic.

## Setup

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  Configure Confluent:
    Create a `client.properties` file in the root directory:
    ```properties
    bootstrap.servers=YOUR_BOOTSTRAP_SERVER
    security.protocol=SASL_SSL
    sasl.mechanisms=PLAIN
    sasl.username=YOUR_API_KEY
    sasl.password=YOUR_API_SECRET
    ```

3.  Run the producer:
    ```bash
    python producer.py
    ```