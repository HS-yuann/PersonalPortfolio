"""
FastAPI REST API for serving real-time and historical market data.

Endpoints:
- GET /                     - API info
- GET /health               - Health check
- GET /prices/latest        - Latest prices
- GET /prices/{symbol}      - Price history for symbol
- GET /ohlc/{symbol}        - OHLC candles
- GET /analytics/{symbol}   - Market analytics & signals
- GET /quality/metrics      - Data quality metrics
- GET /metrics              - Prometheus metrics
"""
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse
from pydantic import BaseModel
import duckdb
from dotenv import load_dotenv

load_dotenv()

# Database path
DB_PATH = os.getenv('DUCKDB_PATH', './data/analytics.duckdb')


# ============================================
# Pydantic Models
# ============================================

class PriceResponse(BaseModel):
    symbol: str
    price: float
    volume: float
    price_change_percent: float
    event_time: datetime


class OHLCResponse(BaseModel):
    symbol: str
    window_start: datetime
    window_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int
    vwap: Optional[float]
    candle_change_pct: Optional[float]


class AnalyticsResponse(BaseModel):
    symbol: str
    period_start: datetime
    period_end: datetime
    price_change_pct: float
    avg_volatility: Optional[float]
    signal: str
    volume_signal: str
    total_trades: int


class SymbolInfo(BaseModel):
    symbol: str
    base_currency: str
    quote_currency: str
    currency_name: str
    market_tier: str


class QualityMetric(BaseModel):
    check_time: datetime
    table_name: str
    metric_name: str
    metric_value: float
    threshold: float
    status: str


class HealthResponse(BaseModel):
    status: str
    database_connected: bool
    tables_count: int
    total_records: int
    timestamp: datetime


# ============================================
# FastAPI App
# ============================================

app = FastAPI(
    title="Crypto Analytics API",
    description="Real-time cryptocurrency market data and analytics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    """Get database connection."""
    return duckdb.connect(DB_PATH, read_only=True)


# ============================================
# Endpoints
# ============================================

@app.get("/", response_class=HTMLResponse)
async def root():
    """API root with links."""
    return """
    <html>
    <head><title>Crypto Analytics API</title></head>
    <body style="font-family: monospace; padding: 20px;">
        <h1>Crypto Analytics API</h1>
        <h2>Endpoints</h2>
        <ul>
            <li><a href="/docs">/docs</a> - Swagger UI</li>
            <li><a href="/health">/health</a> - Health check</li>
            <li><a href="/prices/latest">/prices/latest</a> - Latest prices</li>
            <li><a href="/ohlc/BTCUSDT">/ohlc/BTCUSDT</a> - OHLC candles</li>
            <li><a href="/analytics/BTCUSDT">/analytics/BTCUSDT</a> - Analytics</li>
            <li><a href="/symbols">/symbols</a> - Available symbols</li>
            <li><a href="/quality/metrics">/quality/metrics</a> - Data quality</li>
            <li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
        </ul>
    </body>
    </html>
    """


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        conn = get_db()
        tables = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main'").fetchone()[0]
        records = conn.execute("SELECT COUNT(*) FROM bronze_prices").fetchone()[0]
        conn.close()

        return HealthResponse(
            status="healthy",
            database_connected=True,
            tables_count=tables,
            total_records=records,
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            database_connected=False,
            tables_count=0,
            total_records=0,
            timestamp=datetime.utcnow()
        )


@app.get("/prices/latest", response_model=List[PriceResponse])
async def get_latest_prices():
    """Get latest prices for all symbols."""
    try:
        conn = get_db()
        results = conn.execute("""
            SELECT symbol, price, volume, price_change_percent, event_time
            FROM bronze_prices
            WHERE (symbol, event_time) IN (
                SELECT symbol, MAX(event_time)
                FROM bronze_prices
                GROUP BY symbol
            )
            ORDER BY symbol
        """).fetchall()
        conn.close()

        return [
            PriceResponse(
                symbol=r[0], price=r[1], volume=r[2],
                price_change_percent=r[3], event_time=r[4]
            ) for r in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices/{symbol}", response_model=List[PriceResponse])
async def get_price_history(symbol: str, limit: int = Query(50, ge=1, le=500)):
    """Get price history for a symbol."""
    try:
        conn = get_db()
        results = conn.execute("""
            SELECT symbol, price, volume, price_change_percent, event_time
            FROM bronze_prices
            WHERE symbol = ?
            ORDER BY event_time DESC
            LIMIT ?
        """, [symbol.upper(), limit]).fetchall()
        conn.close()

        if not results:
            raise HTTPException(status_code=404, detail=f"No data for {symbol}")

        return [
            PriceResponse(
                symbol=r[0], price=r[1], volume=r[2],
                price_change_percent=r[3], event_time=r[4]
            ) for r in results
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ohlc/{symbol}", response_model=List[OHLCResponse])
async def get_ohlc(symbol: str, limit: int = Query(50, ge=1, le=200)):
    """Get OHLC candlestick data."""
    try:
        conn = get_db()
        results = conn.execute("""
            SELECT symbol, window_start, window_end, open, high, low, close,
                   volume, trade_count, vwap, candle_change_pct
            FROM silver_ohlc_1min
            WHERE symbol = ?
            ORDER BY window_start DESC
            LIMIT ?
        """, [symbol.upper(), limit]).fetchall()
        conn.close()

        if not results:
            raise HTTPException(status_code=404, detail=f"No OHLC data for {symbol}")

        return [
            OHLCResponse(
                symbol=r[0], window_start=r[1], window_end=r[2],
                open=r[3], high=r[4], low=r[5], close=r[6],
                volume=r[7], trade_count=r[8], vwap=r[9],
                candle_change_pct=r[10]
            ) for r in results
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/{symbol}", response_model=List[AnalyticsResponse])
async def get_analytics(symbol: str):
    """Get market analytics and trading signals."""
    try:
        conn = get_db()
        results = conn.execute("""
            SELECT symbol, period_start, period_end, price_change_pct,
                   avg_volatility, signal, volume_signal, total_trades
            FROM gold_market_analytics
            WHERE symbol = ?
            ORDER BY period_end DESC
        """, [symbol.upper()]).fetchall()
        conn.close()

        if not results:
            raise HTTPException(status_code=404, detail=f"No analytics for {symbol}")

        return [
            AnalyticsResponse(
                symbol=r[0], period_start=r[1], period_end=r[2],
                price_change_pct=r[3], avg_volatility=r[4],
                signal=r[5], volume_signal=r[6], total_trades=r[7]
            ) for r in results
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/symbols", response_model=List[SymbolInfo])
async def list_symbols():
    """List all available symbols."""
    try:
        conn = get_db()
        results = conn.execute("""
            SELECT symbol, base_currency, quote_currency, currency_name, market_tier
            FROM dim_symbols
            WHERE is_active = TRUE
            ORDER BY market_tier, symbol
        """).fetchall()
        conn.close()

        return [
            SymbolInfo(
                symbol=r[0], base_currency=r[1], quote_currency=r[2],
                currency_name=r[3], market_tier=r[4]
            ) for r in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/quality/metrics", response_model=List[QualityMetric])
async def get_quality_metrics():
    """Get data quality metrics."""
    try:
        conn = get_db()
        results = conn.execute("""
            SELECT check_time, table_name, metric_name, metric_value, threshold, status
            FROM data_quality_metrics
            ORDER BY check_time DESC
            LIMIT 20
        """).fetchall()
        conn.close()

        return [
            QualityMetric(
                check_time=r[0], table_name=r[1], metric_name=r[2],
                metric_value=r[3], threshold=r[4], status=r[5]
            ) for r in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics():
    """Prometheus metrics endpoint."""
    try:
        conn = get_db()

        # Get counts
        bronze_count = conn.execute("SELECT COUNT(*) FROM bronze_prices").fetchone()[0]
        silver_count = conn.execute("SELECT COUNT(*) FROM silver_ohlc_1min").fetchone()[0]
        gold_count = conn.execute("SELECT COUNT(*) FROM gold_market_analytics").fetchone()[0]

        # Get quality pass rate
        quality = conn.execute("""
            SELECT
                SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            FROM data_quality_metrics
        """).fetchone()[0] or 0

        conn.close()

        return f"""# HELP crypto_bronze_records_total Total records in bronze layer
# TYPE crypto_bronze_records_total gauge
crypto_bronze_records_total {bronze_count}

# HELP crypto_silver_records_total Total records in silver layer
# TYPE crypto_silver_records_total gauge
crypto_silver_records_total {silver_count}

# HELP crypto_gold_records_total Total records in gold layer
# TYPE crypto_gold_records_total gauge
crypto_gold_records_total {gold_count}

# HELP crypto_data_quality_pass_rate Data quality pass rate percentage
# TYPE crypto_data_quality_pass_rate gauge
crypto_data_quality_pass_rate {quality:.2f}
"""
    except Exception as e:
        return f"# Error: {str(e)}"


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Simple HTML dashboard."""
    try:
        conn = get_db()

        # Get latest prices
        prices = conn.execute("""
            SELECT symbol, price, price_change_percent, event_time
            FROM bronze_prices
            WHERE (symbol, event_time) IN (
                SELECT symbol, MAX(event_time)
                FROM bronze_prices
                GROUP BY symbol
            )
            ORDER BY symbol
        """).fetchall()

        # Get analytics
        analytics = conn.execute("""
            SELECT symbol, price_change_pct, signal, volume_signal, total_trades
            FROM gold_market_analytics
            ORDER BY symbol
        """).fetchall()

        # Get quality metrics
        quality = conn.execute("""
            SELECT metric_name, metric_value, status
            FROM data_quality_metrics
            ORDER BY check_time DESC
            LIMIT 5
        """).fetchall()

        conn.close()

        # Build HTML
        prices_html = ""
        for p in prices:
            color = "green" if p[2] > 0 else "red" if p[2] < 0 else "gray"
            prices_html += f"""
            <div style="border:1px solid #ddd; padding:15px; margin:10px; border-radius:5px;">
                <h3>{p[0]}</h3>
                <p style="font-size:24px; font-weight:bold;">${p[1]:,.2f}</p>
                <p style="color:{color};">{p[2]:+.2f}%</p>
                <small>{p[3]}</small>
            </div>
            """

        analytics_html = ""
        for a in analytics:
            signal_color = "green" if "BULLISH" in a[2] else "red" if "BEARISH" in a[2] else "gray"
            analytics_html += f"""
            <tr>
                <td>{a[0]}</td>
                <td>{a[1]:+.3f}%</td>
                <td style="color:{signal_color}; font-weight:bold;">{a[2]}</td>
                <td>{a[3]}</td>
                <td>{a[4]}</td>
            </tr>
            """

        quality_html = ""
        for q in quality:
            status_color = "green" if q[2] == "PASS" else "orange" if q[2] == "WARN" else "red"
            quality_html += f"""
            <tr>
                <td>{q[0]}</td>
                <td>{q[1]}</td>
                <td style="color:{status_color}; font-weight:bold;">{q[2]}</td>
            </tr>
            """

        return f"""
        <html>
        <head>
            <title>Crypto Analytics Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                h1 {{ color: #333; }}
                .section {{ background: white; padding: 20px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
                .prices {{ display: flex; flex-wrap: wrap; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background: #f0f0f0; }}
            </style>
            <meta http-equiv="refresh" content="30">
        </head>
        <body>
            <h1>Crypto Analytics Dashboard</h1>

            <div class="section">
                <h2>Latest Prices</h2>
                <div class="prices">{prices_html}</div>
            </div>

            <div class="section">
                <h2>Market Analytics</h2>
                <table>
                    <tr><th>Symbol</th><th>Price Change</th><th>Signal</th><th>Volume</th><th>Trades</th></tr>
                    {analytics_html}
                </table>
            </div>

            <div class="section">
                <h2>Data Quality</h2>
                <table>
                    <tr><th>Metric</th><th>Value</th><th>Status</th></tr>
                    {quality_html}
                </table>
            </div>

            <p><small>Auto-refreshes every 30 seconds. <a href="/docs">API Docs</a></small></p>
        </body>
        </html>
        """
    except Exception as e:
        return f"<html><body><h1>Error</h1><p>{str(e)}</p></body></html>"


# ============================================
# Entry Point
# ============================================

if __name__ == "__main__":
    import uvicorn
    print(f"Starting API server...")
    print(f"Database: {DB_PATH}")
    print(f"Docs: http://localhost:8000/docs")
    print(f"Dashboard: http://localhost:8000/dashboard")
    uvicorn.run(app, host="0.0.0.0", port=8000)
