# Real-Time Financial Market Data Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://python.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black.svg)](https://kafka.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00ADD8.svg)](https://delta.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A cloud-native, production-grade streaming data platform that ingests, processes, and serves high-volume cryptocurrency market data using modern data engineering best practices.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION LAYER                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Binance    │  │  CoinGecko   │  │ Fear & Greed │  │  RSS/News    │    │
│  │  WebSocket   │  │     API      │  │    Index     │  │    Feeds     │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│         │                 │                 │                 │            │
│         └────────────┬────┴────────┬────────┴────────┬────────┘            │
│                      │             │                 │                      │
│                      ▼             ▼                 ▼                      │
│              ┌─────────────────────────────────────────┐                    │
│              │           Apache Kafka                  │                    │
│              │    (crypto-prices, market-data, etc.)   │                    │
│              └────────────────────┬────────────────────┘                    │
└───────────────────────────────────┼─────────────────────────────────────────┘
                                    │
┌───────────────────────────────────┼─────────────────────────────────────────┐
│                         PROCESSING LAYER                                    │
├───────────────────────────────────┼─────────────────────────────────────────┤
│                                   ▼                                         │
│              ┌─────────────────────────────────────────┐                    │
│              │     Spark Structured Streaming          │                    │
│              │  • Windowed Aggregations (OHLC, VWAP)   │                    │
│              │  • Late Event Handling (Watermarks)     │                    │
│              │  • Exactly-Once Semantics               │                    │
│              └────────────────────┬────────────────────┘                    │
│                                   │                                         │
│         ┌─────────────────────────┼─────────────────────────┐              │
│         ▼                         ▼                         ▼              │
│  ┌──────────────┐         ┌──────────────┐         ┌──────────────┐        │
│  │    BRONZE    │         │    SILVER    │         │     GOLD     │        │
│  │  Raw Events  │  ────►  │    OHLC      │  ────►  │  Analytics   │        │
│  │              │         │   Candles    │         │   Signals    │        │
│  └──────────────┘         └──────────────┘         └──────────────┘        │
│         │                         │                         │              │
│         └─────────────────────────┼─────────────────────────┘              │
│                                   ▼                                         │
│              ┌─────────────────────────────────────────┐                    │
│              │            Delta Lake (S3)              │                    │
│              │     Partitioned, Time-Travel Enabled    │                    │
│              └─────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────┼─────────────────────────────────────────┐
│                      TRANSFORMATION LAYER                                   │
├───────────────────────────────────┼─────────────────────────────────────────┤
│                                   ▼                                         │
│              ┌─────────────────────────────────────────┐                    │
│              │               dbt Core                  │                    │
│              │  • Staging Models (Data Cleaning)       │                    │
│              │  • Fact & Dimension Tables              │                    │
│              │  • Hourly Aggregations                  │                    │
│              └─────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
┌───────────────────────────────────┼─────────────────────────────────────────┐
│                         SERVING LAYER                                       │
├───────────────────────────────────┼─────────────────────────────────────────┤
│         ┌─────────────────────────┼─────────────────────────┐              │
│         ▼                         ▼                         ▼              │
│  ┌──────────────┐         ┌──────────────┐         ┌──────────────┐        │
│  │   FastAPI    │         │   Grafana    │         │   Airflow    │        │
│  │   REST API   │         │  Dashboards  │         │     DAGs     │        │
│  └──────────────┘         └──────────────┘         └──────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

### Data Ingestion
- **Real-time WebSocket streaming** from Binance for BTC, ETH, BNB
- **REST API polling** from CoinGecko for market data
- **Sentiment data** from Fear & Greed Index
- **News aggregation** from multiple RSS feeds and NewsAPI
- **Schema validation** and exactly-once delivery guarantees

### Stream Processing
- **Spark Structured Streaming** with checkpointing
- **OHLC candlestick aggregation** (1-min, 5-min, 15-min, 1-hour)
- **VWAP (Volume Weighted Average Price)** calculation
- **Late event handling** with 5-minute watermarks
- **Volume spike detection** using rolling baselines

### Data Lakehouse
- **Delta Lake** for ACID transactions and time travel
- **Medallion Architecture**: Bronze → Silver → Gold layers
- **Partitioned storage** for efficient querying
- **Automatic compaction** and vacuum jobs

### Data Quality
- **Great Expectations** for data validation
- **Pydantic models** for schema enforcement
- **Anomaly detection** for price and volume outliers
- **Freshness monitoring** and alerting

### Orchestration
- **Apache Airflow** DAGs for pipeline scheduling
- **Streaming DAG** for continuous processing
- **Batch DAG** for hourly analytics
- **Quality monitoring DAG** every 15 minutes

### Serving Layer
- **FastAPI REST API** for real-time and historical data
- **Prometheus metrics** for monitoring
- **Grafana dashboards** for visualization

## Tech Stack

| Layer | Technology |
|-------|------------|
| **Ingestion** | Kafka, WebSocket, REST APIs |
| **Processing** | Spark Structured Streaming |
| **Storage** | Delta Lake, DuckDB |
| **Transformation** | dbt Core |
| **Quality** | Great Expectations, Pydantic |
| **Orchestration** | Apache Airflow |
| **API** | FastAPI, Uvicorn |
| **Monitoring** | Prometheus, Grafana |
| **Infrastructure** | Docker, Docker Compose |

## Project Structure

```
crypto-stream-analytics/
├── src/
│   ├── producers/           # Kafka producers for data ingestion
│   │   ├── binance_producer.py
│   │   ├── coingecko_producer.py
│   │   ├── fear_greed_producer.py
│   │   └── rss_producer.py
│   ├── streaming/           # Spark streaming processors
│   │   └── spark_stream_processor.py
│   ├── quality/             # Data quality validation
│   │   └── data_validator.py
│   ├── api/                 # FastAPI REST API
│   │   └── main.py
│   └── processors/          # Legacy Python processors
├── dbt/
│   ├── models/
│   │   ├── staging/         # Staging models
│   │   └── marts/           # Business-level models
│   ├── dbt_project.yml
│   └── profiles.yml
├── airflow/
│   └── dags/                # Airflow DAG definitions
├── config/
│   ├── settings.py          # Centralized configuration
│   └── prometheus.yml       # Prometheus config
├── data/
│   ├── delta/               # Delta Lake tables
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── checkpoints/         # Spark checkpoints
├── docker-compose.yml       # Full infrastructure stack
├── Dockerfile.api           # API container
├── requirements.txt         # Python dependencies
├── .env.example             # Environment template
└── README.md
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- 8GB+ RAM recommended

### 1. Clone and Setup

```bash
git clone https://github.com/yourusername/crypto-stream-analytics.git
cd crypto-stream-analytics

# Copy environment template
cp .env.example .env

# Edit .env with your API keys
# COINGECKO_API_KEY=your_key
# NEWSAPI_KEY=your_key
```

### 2. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps
```

### 3. Start Data Producers

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Start producers (in separate terminals)
python src/producers/binance_producer.py
python src/producers/coingecko_producer.py
python src/producers/fear_greed_producer.py
```

### 4. Start Spark Streaming

```bash
python src/streaming/spark_stream_processor.py
```

### 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | - |
| Spark UI | http://localhost:8082 | - |
| Airflow | http://localhost:8083 | admin/admin |
| API Docs | http://localhost:8000/docs | - |
| Grafana | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | - |

## API Endpoints

```bash
# Get latest prices
GET /prices/latest

# Get price history for symbol
GET /prices/BTCUSDT?limit=100&hours=24

# Get OHLC candles
GET /ohlc/BTCUSDT?interval=1m&limit=100

# Get market analytics
GET /analytics/BTCUSDT

# Health check
GET /health

# Prometheus metrics
GET /metrics
```

## Running dbt Transformations

```bash
cd dbt

# Install dbt
pip install dbt-duckdb

# Run models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Generate docs
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## Resume-Ready Description

> **Data Engineering Project – Real-Time Market Data Platform**
>
> - Designed and implemented a cloud-native streaming data platform processing real-time financial market data at scale
> - Built Kafka-based ingestion and Spark Structured Streaming pipelines with exactly-once semantics and watermark-based late event handling
> - Implemented medallion architecture (Bronze/Silver/Gold) using Delta Lake for ACID transactions and time-travel queries
> - Modeled analytics data using dbt with dimensional modeling patterns and optimized warehouse queries for BI workloads
> - Implemented data quality checks using Great Expectations and Pydantic with automated anomaly detection
> - Built REST API serving layer with FastAPI for real-time and historical data access
> - Deployed infrastructure using Docker Compose with Prometheus/Grafana monitoring and Airflow orchestration

## Key Metrics

- **Ingestion Rate**: ~100 messages/second from Binance WebSocket
- **Processing Latency**: <1 second for Bronze layer
- **OHLC Aggregation**: 1-minute candles with VWAP
- **Data Retention**: 7 days in Kafka, unlimited in Delta Lake
- **API Response Time**: <100ms for latest prices

## Future Improvements

- [ ] Deploy to AWS/GCP with Terraform
- [ ] Add Kubernetes deployment manifests
- [ ] Implement CDC with Debezium
- [ ] Add ML-based price prediction
- [ ] Expand to more trading pairs
- [ ] Add alerting with PagerDuty/Slack

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

Built as a portfolio project demonstrating production data engineering practices.
