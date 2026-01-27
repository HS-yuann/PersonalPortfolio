"""
Airflow DAG for Crypto Data Pipeline Orchestration.

This DAG orchestrates the complete data pipeline:
1. Start Kafka producers (data ingestion)
2. Run Spark streaming jobs (Bronze -> Silver -> Gold)
3. Run data quality checks
4. Run dbt transformations
5. Alert on failures

Schedule: Continuous (streaming) with periodic batch jobs
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ============================================
# Default Arguments
# ============================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["alerts@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ============================================
# DAG Definition: Main Pipeline
# ============================================

with DAG(
    dag_id="crypto_streaming_pipeline",
    default_args=default_args,
    description="Real-time crypto data streaming pipeline",
    schedule_interval=None,  # Triggered manually or by external system
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "streaming", "production"],
) as streaming_dag:

    start = EmptyOperator(task_id="start")

    # Check Kafka connectivity
    check_kafka = BashOperator(
        task_id="check_kafka_connectivity",
        bash_command="""
            echo "Checking Kafka connectivity..."
            nc -zv localhost 9092 || exit 1
            echo "Kafka is accessible"
        """,
    )

    # Start Bronze layer ingestion (Spark Streaming)
    start_bronze_streaming = SparkSubmitOperator(
        task_id="start_bronze_streaming",
        application="{{ var.value.project_root }}/src/streaming/spark_stream_processor.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0",
    )

    # Data quality check
    run_quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=lambda: print("Running data quality checks..."),
        # In production: call data_quality_checker.validate_batch()
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task dependencies
    start >> check_kafka >> start_bronze_streaming >> run_quality_checks >> end


# ============================================
# DAG Definition: Batch Analytics
# ============================================

with DAG(
    dag_id="crypto_batch_analytics",
    default_args=default_args,
    description="Hourly batch analytics and dbt transformations",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "batch", "analytics"],
) as batch_dag:

    start_batch = EmptyOperator(task_id="start")

    # Run dbt transformations
    run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="""
            cd {{ var.value.project_root }}/dbt
            dbt run --profiles-dir . --target prod
        """,
    )

    # Run dbt tests
    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command="""
            cd {{ var.value.project_root }}/dbt
            dbt test --profiles-dir . --target prod
        """,
    )

    # Generate dbt docs
    generate_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command="""
            cd {{ var.value.project_root }}/dbt
            dbt docs generate --profiles-dir . --target prod
        """,
    )

    # Compact Delta tables (maintenance)
    compact_delta_tables = BashOperator(
        task_id="compact_delta_tables",
        bash_command="""
            echo "Running Delta Lake OPTIMIZE and VACUUM..."
            # In production: spark-submit delta-maintenance.py
        """,
    )

    # Data quality report
    generate_quality_report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=lambda: print("Generating data quality report..."),
    )

    end_batch = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task dependencies
    start_batch >> run_dbt >> test_dbt >> generate_docs
    start_batch >> compact_delta_tables
    [test_dbt, compact_delta_tables] >> generate_quality_report >> end_batch



with DAG(
    dag_id="crypto_data_quality_monitor",
    default_args=default_args,
    description="Continuous data quality monitoring",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "quality", "monitoring"],
) as quality_dag:

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=lambda: print("Checking data freshness..."),
        # In production: check if latest data < 5 minutes old
    )

    check_completeness = PythonOperator(
        task_id="check_data_completeness",
        python_callable=lambda: print("Checking data completeness..."),
        # In production: check all symbols have recent data
    )

    check_validity = PythonOperator(
        task_id="check_data_validity",
        python_callable=lambda: print("Checking data validity..."),
        # In production: run Great Expectations suite
    )

    alert_on_issues = PythonOperator(
        task_id="alert_on_quality_issues",
        python_callable=lambda: print("Alerting if issues found..."),
        trigger_rule=TriggerRule.ALL_DONE
    )

    [check_freshness, check_completeness, check_validity] >> alert_on_issues
