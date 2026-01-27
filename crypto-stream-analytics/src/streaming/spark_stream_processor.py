"""
Spark Structured Streaming Processor for Real-Time Crypto Data.

This module implements production-grade stream processing with:
- Exactly-once semantics via checkpointing
- Windowed aggregations (OHLC, VWAP)
- Late event handling with watermarks
- Delta Lake sink for lakehouse storage

Architecture:
    Kafka -> Spark Structured Streaming -> Delta Lake (Bronze/Silver/Gold)
"""
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    first, last, max as spark_max, min as spark_min,
    sum as spark_sum, avg, count, lit, current_timestamp,
    expr, when, lag, stddev
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType
)
from delta import configure_spark_with_delta_pip

from config import settings, ensure_directories


class SparkStreamProcessor:
    """
    Production Spark Structured Streaming processor for crypto market data.

    Implements the medallion architecture:
    - Bronze: Raw ingested data
    - Silver: Cleaned, validated data with OHLC aggregations
    - Gold: Business-level aggregates and analytics
    """

    # Schema for incoming Kafka messages (crypto-prices topic)
    PRICE_SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("price_change_percent", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("timestamp", LongType(), True),
        StructField("human_time", StringType(), True),
    ])

    # Schema for Fear & Greed Index
    SENTIMENT_SCHEMA = StructType([
        StructField("type", StringType(), True),
        StructField("value", LongType(), True),
        StructField("classification", StringType(), True),
        StructField("timestamp", LongType(), True),
    ])

    def __init__(self):
        """Initialize Spark session with Delta Lake support."""
        ensure_directories()

        # Configure Spark with Delta Lake
        builder = (
            SparkSession.builder
            .appName(settings.spark_app_name)
            .master(settings.spark_master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.checkpointLocation", settings.checkpoint_path)
            # Kafka configs
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "io.delta:delta-spark_2.12:3.0.0")
            # Performance tuning
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.streaming.backpressure.enabled", "true")
        )

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        print(f"[OK] Spark session initialized: {settings.spark_app_name}")
        print(f"[OK] Delta Lake path: {settings.delta_lake_path}")

    def read_kafka_stream(self, topic: str):
        """
        Read streaming data from Kafka topic.

        Args:
            topic: Kafka topic name

        Returns:
            DataFrame: Spark streaming DataFrame
        """
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def process_bronze_layer(self):
        """
        Bronze Layer: Raw data ingestion with minimal transformation.

        - Preserves original data
        - Adds ingestion timestamp
        - Partitions by date for efficient querying
        """
        print("\n[BRONZE] Starting raw data ingestion...")

        # Read from Kafka
        raw_stream = self.read_kafka_stream(settings.topic_crypto_prices)

        # Parse JSON and add metadata
        bronze_df = (
            raw_stream
            .selectExpr("CAST(key AS STRING) as symbol_key",
                       "CAST(value AS STRING) as raw_json",
                       "timestamp as kafka_timestamp",
                       "partition", "offset")
            .withColumn("data", from_json(col("raw_json"), self.PRICE_SCHEMA))
            .select(
                col("symbol_key"),
                col("data.*"),
                col("kafka_timestamp"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                current_timestamp().alias("ingested_at")
            )
            .withColumn("event_time",
                       to_timestamp(col("timestamp") / 1000))
            # Add watermark for late data handling (5 minutes)
            .withWatermark("event_time", "5 minutes")
        )

        # Write to Delta Lake Bronze layer
        bronze_path = f"{settings.delta_lake_path}/bronze/crypto_prices"
        checkpoint_path = f"{settings.checkpoint_path}/bronze_prices"

        query = (
            bronze_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("symbol")
            .start(bronze_path)
        )

        print(f"[BRONZE] Writing to: {bronze_path}")
        return query

    def process_silver_layer(self):
        """
        Silver Layer: Cleaned data with OHLC candlestick aggregations.

        Computes 1-minute OHLC (Open, High, Low, Close) candles with:
        - Volume Weighted Average Price (VWAP)
        - Trade count
        - Volatility metrics
        """
        print("\n[SILVER] Starting OHLC aggregation...")

        # Read from Bronze layer
        bronze_path = f"{settings.delta_lake_path}/bronze/crypto_prices"

        # Read as streaming from Delta
        bronze_stream = (
            self.spark.readStream
            .format("delta")
            .load(bronze_path)
            .withWatermark("event_time", "5 minutes")
        )

        # Compute 1-minute OHLC candles
        ohlc_df = (
            bronze_stream
            .groupBy(
                col("symbol"),
                window(col("event_time"), "1 minute")
            )
            .agg(
                first("price").alias("open"),
                spark_max("price").alias("high"),
                spark_min("price").alias("low"),
                last("price").alias("close"),
                spark_sum("volume").alias("volume"),
                count("*").alias("trade_count"),
                avg("price").alias("avg_price"),
                # VWAP = sum(price * volume) / sum(volume)
                (spark_sum(col("price") * col("volume")) / spark_sum("volume")).alias("vwap"),
                stddev("price").alias("price_volatility"),
                spark_max("price_change_percent").alias("max_change_pct"),
                spark_min("price_change_percent").alias("min_change_pct")
            )
            .select(
                col("symbol"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("open"), col("high"), col("low"), col("close"),
                col("volume"), col("trade_count"), col("vwap"),
                col("price_volatility"),
                ((col("close") - col("open")) / col("open") * 100).alias("candle_change_pct"),
                current_timestamp().alias("processed_at")
            )
        )

        # Write to Silver layer
        silver_path = f"{settings.delta_lake_path}/silver/ohlc_1min"
        checkpoint_path = f"{settings.checkpoint_path}/silver_ohlc"

        query = (
            ohlc_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("symbol")
            .start(silver_path)
        )

        print(f"[SILVER] Writing OHLC candles to: {silver_path}")
        return query

    def process_gold_layer(self):
        """
        Gold Layer: Business-level analytics and signals.

        Computes:
        - Volume spike detection
        - Price movement alerts
        - Rolling statistics (15-min, 1-hour windows)
        """
        print("\n[GOLD] Starting analytics aggregation...")

        # Read from Silver OHLC
        silver_path = f"{settings.delta_lake_path}/silver/ohlc_1min"

        silver_stream = (
            self.spark.readStream
            .format("delta")
            .load(silver_path)
            .withWatermark("window_end", "10 minutes")
        )

        # 15-minute rolling analytics
        analytics_df = (
            silver_stream
            .groupBy(
                col("symbol"),
                window(col("window_end"), "15 minutes", "5 minutes")  # 15-min window, 5-min slide
            )
            .agg(
                first("open").alias("period_open"),
                spark_max("high").alias("period_high"),
                spark_min("low").alias("period_low"),
                last("close").alias("period_close"),
                spark_sum("volume").alias("period_volume"),
                avg("vwap").alias("period_vwap"),
                avg("price_volatility").alias("avg_volatility"),
                spark_sum("trade_count").alias("total_trades")
            )
            .withColumn("price_change_pct",
                       (col("period_close") - col("period_open")) / col("period_open") * 100)
            .withColumn("price_range_pct",
                       (col("period_high") - col("period_low")) / col("period_low") * 100)
            # Generate signals
            .withColumn("signal",
                when(col("price_change_pct") > 2.0, "STRONG_BULLISH")
                .when(col("price_change_pct") > 0.5, "BULLISH")
                .when(col("price_change_pct") < -2.0, "STRONG_BEARISH")
                .when(col("price_change_pct") < -0.5, "BEARISH")
                .otherwise("NEUTRAL")
            )
            .withColumn("volume_signal",
                when(col("period_volume") > 1000000, "HIGH_VOLUME")
                .otherwise("NORMAL_VOLUME")
            )
            .select(
                col("symbol"),
                col("window.start").alias("period_start"),
                col("window.end").alias("period_end"),
                col("period_open"), col("period_high"),
                col("period_low"), col("period_close"),
                col("period_volume"), col("period_vwap"),
                col("avg_volatility"), col("total_trades"),
                col("price_change_pct"), col("price_range_pct"),
                col("signal"), col("volume_signal"),
                current_timestamp().alias("computed_at")
            )
        )

        # Write to Gold layer
        gold_path = f"{settings.delta_lake_path}/gold/market_analytics"
        checkpoint_path = f"{settings.checkpoint_path}/gold_analytics"

        query = (
            analytics_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("symbol")
            .start(gold_path)
        )

        print(f"[GOLD] Writing analytics to: {gold_path}")
        return query

    def process_console_output(self):
        """Debug stream: Write to console for monitoring."""
        raw_stream = self.read_kafka_stream(settings.topic_crypto_prices)

        parsed = (
            raw_stream
            .selectExpr("CAST(value AS STRING) as json_value")
            .select(from_json(col("json_value"), self.PRICE_SCHEMA).alias("data"))
            .select("data.*")
        )

        query = (
            parsed.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start()
        )

        return query

    def start_all_layers(self):
        """Start all processing layers (Bronze -> Silver -> Gold)."""
        print("=" * 60)
        print("SPARK STRUCTURED STREAMING - MEDALLION ARCHITECTURE")
        print("=" * 60)
        print(f"\nKafka: {settings.kafka_bootstrap_servers}")
        print(f"Delta Lake: {settings.delta_lake_path}")
        print(f"Checkpoints: {settings.checkpoint_path}")
        print("=" * 60)

        queries = []

        # Start Bronze layer (raw ingestion)
        queries.append(self.process_bronze_layer())

        # Note: Silver and Gold layers read from Delta tables
        # They need Bronze data to exist first
        # In production, these would be separate jobs or have proper dependencies

        print("\n[OK] All streaming queries started")
        print("[INFO] Press Ctrl+C to stop\n")

        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            print("\n[STOPPING] Graceful shutdown initiated...")
            for q in queries:
                q.stop()
            self.spark.stop()
            print("[OK] All streams stopped")


def main():
    """Entry point for Spark streaming processor."""
    processor = SparkStreamProcessor()

    # For development: just run Bronze layer
    # In production: use Airflow to orchestrate layers
    processor.start_all_layers()


if __name__ == "__main__":
    main()
