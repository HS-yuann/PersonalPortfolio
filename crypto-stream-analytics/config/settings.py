"""
Centralized configuration management using Pydantic and environment variables.
"""
import os
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_schema_registry_url: str = "http://localhost:8081"

    # --- API Keys ---
    coingecko_api_key: Optional[str] = None
    newsapi_key: Optional[str] = None

    # --- Storage Paths ---
    delta_lake_path: str = "./data/delta"
    checkpoint_path: str = "./data/checkpoints"
    duckdb_path: str = "./data/analytics.duckdb"

    # --- Spark ---
    spark_master: str = "local[*]"
    spark_app_name: str = "crypto-stream-analytics"

    # --- API Server ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # --- Monitoring ---
    prometheus_port: int = 9090
    log_level: str = "INFO"

    # --- Kafka Topics ---
    topic_crypto_prices: str = "crypto-prices"
    topic_market_data: str = "crypto-market-data"
    topic_fear_greed: str = "crypto-fear-greed"
    topic_news: str = "crypto-news"

    # --- Processing Thresholds ---
    volume_spike_multiplier: float = 1.5
    price_change_threshold: float = 0.1
    fear_threshold: int = 30
    greed_threshold: int = 70

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()


# Ensure data directories exist
def ensure_directories():
    """Create required data directories."""
    dirs = [
        settings.delta_lake_path,
        settings.checkpoint_path,
        Path(settings.delta_lake_path) / "bronze",
        Path(settings.delta_lake_path) / "silver",
        Path(settings.delta_lake_path) / "gold",
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)
