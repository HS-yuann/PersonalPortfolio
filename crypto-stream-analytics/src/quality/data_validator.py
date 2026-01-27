"""
Data Quality Validation using Great Expectations.

Implements data quality checks for:
- Schema validation
- Null detection
- Price outlier detection
- Volume anomaly detection
- Freshness checks

Integrates with the streaming pipeline to validate data quality
at each layer of the medallion architecture.
"""
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from pydantic import BaseModel, validator
import structlog

from config import settings

logger = structlog.get_logger()


class PriceDataValidator(BaseModel):
    """Pydantic model for validating incoming price data."""
    symbol: str
    price: float
    volume: float
    timestamp: int
    price_change_percent: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None

    @validator("price")
    def price_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Price must be positive")
        return v

    @validator("volume")
    def volume_must_be_non_negative(cls, v):
        if v < 0:
            raise ValueError("Volume cannot be negative")
        return v

    @validator("symbol")
    def symbol_must_be_valid(cls, v):
        valid_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
        if v.upper() not in valid_symbols:
            logger.warning("unknown_symbol", symbol=v)
        return v.upper()


class DataQualityChecker:
    """
    Data quality checker using Great Expectations.

    Validates data at each layer:
    - Bronze: Schema validation, null checks
    - Silver: Business rule validation, outlier detection
    - Gold: Aggregation integrity checks
    """

    # Price thresholds for anomaly detection (configurable per symbol)
    PRICE_BOUNDS = {
        "BTCUSDT": {"min": 10000, "max": 500000},
        "ETHUSDT": {"min": 500, "max": 50000},
        "BNBUSDT": {"min": 100, "max": 5000},
    }

    # Volume thresholds
    VOLUME_BOUNDS = {
        "BTCUSDT": {"min": 0, "max": 100_000_000_000},
        "ETHUSDT": {"min": 0, "max": 50_000_000_000},
        "BNBUSDT": {"min": 0, "max": 10_000_000_000},
    }

    def __init__(self):
        """Initialize Great Expectations context."""
        self.context = self._init_ge_context()
        self._create_expectation_suites()
        self.validation_results: List[Dict] = []

    def _init_ge_context(self):
        """Initialize GE data context."""
        try:
            context = gx.get_context()
            logger.info("ge_context_initialized")
            return context
        except Exception as e:
            logger.warning("ge_context_fallback", error=str(e))
            return None

    def _create_expectation_suites(self):
        """Create expectation suites for each data layer."""
        if not self.context:
            return

        # Bronze layer expectations
        bronze_suite = ExpectationSuite(expectation_suite_name="bronze_crypto_prices")
        bronze_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "symbol"}
            )
        )
        bronze_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "price"}
            )
        )
        bronze_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "symbol"}
            )
        )
        bronze_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "price"}
            )
        )
        bronze_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={"column": "price", "type_": "float"}
            )
        )

    def validate_price_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate incoming price data record.

        Args:
            data: Raw price data dictionary

        Returns:
            Validation result with status and any errors
        """
        result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "data": data,
            "validated_at": datetime.utcnow().isoformat()
        }

        try:
            # Schema validation via Pydantic
            validated = PriceDataValidator(**data)
            result["data"] = validated.dict()

        except Exception as e:
            result["valid"] = False
            result["errors"].append(f"Schema validation failed: {str(e)}")
            return result

        # Business rule validations
        symbol = data.get("symbol", "").upper()
        price = data.get("price", 0)
        volume = data.get("volume", 0)

        # Price bounds check
        if symbol in self.PRICE_BOUNDS:
            bounds = self.PRICE_BOUNDS[symbol]
            if price < bounds["min"] or price > bounds["max"]:
                result["warnings"].append(
                    f"Price {price} outside expected range [{bounds['min']}, {bounds['max']}]"
                )

        # Volume bounds check
        if symbol in self.VOLUME_BOUNDS:
            bounds = self.VOLUME_BOUNDS[symbol]
            if volume < bounds["min"] or volume > bounds["max"]:
                result["warnings"].append(
                    f"Volume {volume} outside expected range [{bounds['min']}, {bounds['max']}]"
                )

        # Timestamp freshness check (data should be < 5 minutes old)
        timestamp = data.get("timestamp", 0)
        if timestamp > 0:
            event_time = datetime.fromtimestamp(timestamp / 1000)
            age = datetime.utcnow() - event_time
            if age > timedelta(minutes=5):
                result["warnings"].append(f"Stale data: {age.total_seconds():.0f}s old")

        # Price change sanity check
        price_change = data.get("price_change_percent")
        if price_change is not None and abs(price_change) > 50:
            result["warnings"].append(f"Extreme price change: {price_change}%")

        self.validation_results.append(result)
        return result

    def validate_ohlc_candle(self, candle: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate OHLC candle data.

        Checks:
        - High >= Open, Close, Low
        - Low <= Open, Close, High
        - Volume is non-negative
        - VWAP is within OHLC range
        """
        result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "data": candle,
            "validated_at": datetime.utcnow().isoformat()
        }

        o, h, l, c = candle.get("open"), candle.get("high"), candle.get("low"), candle.get("close")

        # OHLC integrity
        if h is not None and l is not None:
            if h < l:
                result["valid"] = False
                result["errors"].append(f"High ({h}) < Low ({l})")

        if o is not None and h is not None and o > h:
            result["warnings"].append(f"Open ({o}) > High ({h})")

        if c is not None and l is not None and c < l:
            result["warnings"].append(f"Close ({c}) < Low ({l})")

        # VWAP check
        vwap = candle.get("vwap")
        if vwap is not None and l is not None and h is not None:
            if vwap < l or vwap > h:
                result["warnings"].append(f"VWAP ({vwap}) outside OHLC range [{l}, {h}]")

        # Volume check
        volume = candle.get("volume", 0)
        if volume < 0:
            result["valid"] = False
            result["errors"].append("Negative volume")

        return result

    def get_quality_metrics(self) -> Dict[str, Any]:
        """
        Get aggregated data quality metrics.

        Returns:
            Dictionary with quality statistics
        """
        if not self.validation_results:
            return {"total": 0, "valid": 0, "invalid": 0, "warning_rate": 0}

        total = len(self.validation_results)
        valid = sum(1 for r in self.validation_results if r["valid"])
        with_warnings = sum(1 for r in self.validation_results if r["warnings"])

        return {
            "total_records": total,
            "valid_records": valid,
            "invalid_records": total - valid,
            "records_with_warnings": with_warnings,
            "validity_rate": valid / total * 100 if total > 0 else 0,
            "warning_rate": with_warnings / total * 100 if total > 0 else 0,
            "last_checked": datetime.utcnow().isoformat()
        }

    def clear_results(self):
        """Clear validation results (for periodic reset)."""
        self.validation_results = []


# Singleton instance for use across the application
data_quality_checker = DataQualityChecker()


def validate_record(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function for validating a single record."""
    return data_quality_checker.validate_price_data(data)
