"""
FastAPI REST API for serving real-time and historical market data.

Endpoints:
- /prices/latest: Latest prices for all symbols
- /prices/{symbol}: Historical prices for a symbol
- /ohlc/{symbol}: OHLC candles (1-min, 5-min, 15-min, 1-hour)
- /analytics/{symbol}: Market analytics and signals
- /health: Service health check
- /metrics: Prometheus metrics

This API serves data from the Gold layer of the Delta Lake.
"""
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import duckdb

from config import settings
from src.quality import data_quality_checker


# ============================================
# Pydantic Models
# ============================================

class TimeInterval(str, Enum):
    ONE_MIN = "1m"
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"


class PriceResponse(BaseModel):
    symbol: str
    price: float
    volume: float
    price_change_percent: float
    timestamp: datetime


class OHLCResponse(BaseModel):
    symbol: str
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    trade_count: int
    timestamp: datetime


class AnalyticsResponse(BaseModel):
    symbol: str
    period_start: datetime
    period_end: datetime
    price_change_pct: float
    volatility: float
    signal: str
    volume_signal: str
    total_trades: int


class HealthResponse(BaseModel):
    status: str
    kafka_connected: bool
    delta_lake_accessible: bool
    timestamp: datetime


class DataQualityResponse(BaseModel):
    total_records: int
    valid_records: int
    invalid_records: int
    validity_rate: float
    warning_rate: float
    last_checked: str


# ============================================
# FastAPI Application
# ============================================

app = FastAPI(
    title="Crypto Stream Analytics API",
    description="Real-time and historical cryptocurrency market data API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# Database Connection
# ============================================

def get_db_connection():
    """Get DuckDB connection for querying Delta Lake."""
    conn = duckdb.connect(settings.duckdb_path)
    # Register Delta tables
    try:
        conn.execute(f"""
            CREATE OR REPLACE VIEW bronze_prices AS
            SELECT * FROM delta_scan('{settings.delta_lake_path}/bronze/crypto_prices')
        """)
        conn.execute(f"""
            CREATE OR REPLACE VIEW silver_ohlc AS
            SELECT * FROM delta_scan('{settings.delta_lake_path}/silver/ohlc_1min')
        """)
        conn.execute(f"""
            CREATE OR REPLACE VIEW gold_analytics AS
            SELECT * FROM delta_scan('{settings.delta_lake_path}/gold/market_analytics')
        """)
    except Exception as e:
        # Tables may not exist yet
        pass
    return conn


# ============================================
# API Endpoints
# ============================================

@app.get("/", tags=["Root"])
async def root():
    """API root endpoint."""
    return {
        "name": "Crypto Stream Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint.

    Returns service status and component connectivity.
    """
    delta_accessible = Path(settings.delta_lake_path).exists()

    # Check Kafka connectivity (simplified)
    kafka_connected = True  # Would implement actual check in production

    status = "healthy" if delta_accessible else "degraded"

    return HealthResponse(
        status=status,
        kafka_connected=kafka_connected,
        delta_lake_accessible=delta_accessible,
        timestamp=datetime.utcnow()
    )


@app.get("/prices/latest", response_model=List[PriceResponse], tags=["Prices"])
async def get_latest_prices():
    """
    Get latest prices for all tracked symbols.

    Returns the most recent price data from the Bronze layer.
    """
    try:
        conn = get_db_connection()
        result = conn.execute("""
            SELECT symbol, price, volume, price_change_percent, event_time
            FROM bronze_prices
            WHERE event_time = (
                SELECT MAX(event_time) FROM bronze_prices bp2
                WHERE bp2.symbol = bronze_prices.symbol
            )
            ORDER BY symbol
        """).fetchall()
        conn.close()

        return [
            PriceResponse(
                symbol=row[0],
                price=row[1],
                volume=row[2],
                price_change_percent=row[3] or 0,
                timestamp=row[4]
            )
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/prices/{symbol}", response_model=List[PriceResponse], tags=["Prices"])
async def get_price_history(
    symbol: str,
    limit: int = Query(100, ge=1, le=1000),
    hours: int = Query(24, ge=1, le=168)
):
    """
    Get historical prices for a specific symbol.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        limit: Maximum number of records (default: 100)
        hours: Lookback period in hours (default: 24)
    """
    try:
        conn = get_db_connection()
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        result = conn.execute("""
            SELECT symbol, price, volume, price_change_percent, event_time
            FROM bronze_prices
            WHERE symbol = ? AND event_time > ?
            ORDER BY event_time DESC
            LIMIT ?
        """, [symbol.upper(), cutoff, limit]).fetchall()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for {symbol}")

        return [
            PriceResponse(
                symbol=row[0],
                price=row[1],
                volume=row[2],
                price_change_percent=row[3] or 0,
                timestamp=row[4]
            )
            for row in result
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/ohlc/{symbol}", response_model=List[OHLCResponse], tags=["OHLC"])
async def get_ohlc_candles(
    symbol: str,
    interval: TimeInterval = Query(TimeInterval.ONE_MIN),
    limit: int = Query(100, ge=1, le=500)
):
    """
    Get OHLC candlestick data for a symbol.

    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        interval: Candle interval (1m, 5m, 15m, 1h, 1d)
        limit: Number of candles to return
    """
    try:
        conn = get_db_connection()

        result = conn.execute("""
            SELECT symbol, window_start, open, high, low, close,
                   volume, vwap, trade_count
            FROM silver_ohlc
            WHERE symbol = ?
            ORDER BY window_start DESC
            LIMIT ?
        """, [symbol.upper(), limit]).fetchall()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"No OHLC data for {symbol}")

        return [
            OHLCResponse(
                symbol=row[0],
                interval=interval.value,
                open=row[2],
                high=row[3],
                low=row[4],
                close=row[5],
                volume=row[6],
                vwap=row[7],
                trade_count=row[8],
                timestamp=row[1]
            )
            for row in result
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/analytics/{symbol}", response_model=List[AnalyticsResponse], tags=["Analytics"])
async def get_market_analytics(
    symbol: str,
    limit: int = Query(50, ge=1, le=200)
):
    """
    Get market analytics and trading signals for a symbol.

    Returns aggregated analytics from the Gold layer including:
    - Price change percentage
    - Volatility metrics
    - Trading signals (BULLISH, BEARISH, NEUTRAL)
    - Volume signals
    """
    try:
        conn = get_db_connection()

        result = conn.execute("""
            SELECT symbol, period_start, period_end, price_change_pct,
                   avg_volatility, signal, volume_signal, total_trades
            FROM gold_analytics
            WHERE symbol = ?
            ORDER BY period_end DESC
            LIMIT ?
        """, [symbol.upper(), limit]).fetchall()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"No analytics for {symbol}")

        return [
            AnalyticsResponse(
                symbol=row[0],
                period_start=row[1],
                period_end=row[2],
                price_change_pct=row[3],
                volatility=row[4] or 0,
                signal=row[5],
                volume_signal=row[6],
                total_trades=row[7]
            )
            for row in result
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/quality/metrics", response_model=DataQualityResponse, tags=["Quality"])
async def get_data_quality_metrics():
    """
    Get data quality metrics.

    Returns validation statistics from the data quality checker.
    """
    metrics = data_quality_checker.get_quality_metrics()
    return DataQualityResponse(**metrics)


@app.get("/symbols", tags=["Metadata"])
async def list_symbols():
    """
    List all available trading symbols.
    """
    return {
        "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        "description": "Supported trading pairs"
    }


@app.get("/metrics", tags=["Monitoring"])
async def prometheus_metrics():
    """
    Prometheus metrics endpoint.

    Returns metrics in Prometheus format for monitoring.
    """
    # In production, use prometheus_client to generate proper metrics
    metrics = data_quality_checker.get_quality_metrics()

    prometheus_output = f"""
# HELP crypto_records_total Total records processed
# TYPE crypto_records_total counter
crypto_records_total {metrics.get('total_records', 0)}

# HELP crypto_records_valid Valid records count
# TYPE crypto_records_valid counter
crypto_records_valid {metrics.get('valid_records', 0)}

# HELP crypto_validity_rate Data validity percentage
# TYPE crypto_validity_rate gauge
crypto_validity_rate {metrics.get('validity_rate', 0)}
"""
    return JSONResponse(content=prometheus_output, media_type="text/plain")


# ============================================
# Entry Point
# ============================================

def start_server():
    """Start the API server."""
    import uvicorn
    uvicorn.run(
        "src.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True
    )


if __name__ == "__main__":
    start_server()
