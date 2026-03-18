"""
GenAI Orchestrator DAG - Main Airflow DAG for LLM-powered pipeline orchestration.
Deployed on GCP Cloud Composer 2 (Airflow 2.7+).

This DAG demonstrates:
- LLM-powered failure triage via Vertex AI Gemini
- Intelligent retry with adaptive backoff
- Anomaly detection on pipeline metrics
- Self-healing task execution
- BigQuery audit logging

Author: Jagadeesh Thiruveedula
Project: GenAI-DataPipeline-Orchestrator
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level constants (override via Airflow Variables)
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="my-gcp-project")
BQ_DATASET = Variable.get("bq_monitoring_dataset", default_var="pipeline_monitoring")
LLM_MODEL = Variable.get("llm_model", default_var="gemini-1.5-pro")
SENSITIVITY = Variable.get("anomaly_sensitivity", default_var="medium")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # Retries managed by LLM orchestrator
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "tags": ["genai", "llm", "self-healing", "gcp"],
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def extract_pipeline_data(**context: Any) -> dict:
    """
    Extract data from source systems into GCS staging area.
    Simulates a real ETL extract step with metrics tracking.
    """
    import time
    logger.info("Starting data extraction for execution_date=%s", context["ds"])
    start_time = time.time()

    # Simulate extraction work
    row_count = 150_000  # Would be actual extraction result
    duration = time.time() - start_time

    metrics = {
        "row_count": row_count,
        "duration_seconds": duration,
        "source": "gcs://raw-data-bucket/events/",
        "execution_date": context["ds"],
    }
    context["ti"].xcom_push(key="extract_metrics", value=metrics)
    logger.info("Extraction complete: %s rows in %.1fs", row_count, duration)
    return metrics


def run_anomaly_detection(**context: Any) -> str:
    """
    Run anomaly detection on pipeline metrics.
    Returns branch: 'anomaly_detected' or 'proceed_to_transform'
    """
    from src.anomaly_detector import AnomalyDetector

    metrics = context["ti"].xcom_pull(key="extract_metrics", task_ids="extract_data")
    if not metrics:
        logger.warning("No metrics found, skipping anomaly check")
        return "proceed_to_transform"

    detector = AnomalyDetector(
        project_id=GCP_PROJECT_ID,
        bq_dataset=BQ_DATASET,
        sensitivity=SENSITIVITY,
    )

    anomalies = detector.check_row_count(
        dag_id=context["dag"].dag_id,
        current_row_count=metrics["row_count"],
    )
    anomalies += detector.check_dag_duration(
        dag_id=context["dag"].dag_id,
        current_duration_seconds=int(metrics["duration_seconds"]),
    )

    if anomalies:
        severity_levels = [a.severity.value for a in anomalies]
        logger.warning("Anomalies detected: %s", severity_levels)
        context["ti"].xcom_push(key="anomalies", value=[str(a) for a in anomalies])

        # Critical anomalies halt pipeline; others log and continue
        if any(s in ("CRITICAL", "HIGH") for s in severity_levels):
            return "handle_anomaly"

    return "proceed_to_transform"


def handle_anomaly_task(**context: Any) -> None:
    """Handle detected anomalies - alert and optionally halt."""
    anomalies = context["ti"].xcom_pull(key="anomalies", task_ids="check_anomalies")
    logger.error("ANOMALY HANDLER: Pipeline halted due to anomalies: %s", anomalies)
    # In production: send PagerDuty/Slack alert
    raise RuntimeError(f"Pipeline halted: anomalies detected - {anomalies}")


def transform_data(**context: Any) -> dict:
    """Transform extracted data using BigQuery SQL."""
    logger.info("Starting transformation for %s", context["ds"])
    # In production: execute BigQuery transformation SQL
    result = {"transformed_rows": 148_500, "quality_score": 0.99}
    context["ti"].xcom_push(key="transform_result", value=result)
    return result


def load_to_bigquery(**context: Any) -> None:
    """Load transformed data to BigQuery target tables."""
    transform_result = context["ti"].xcom_pull(
        key="transform_result", task_ids="transform_data"
    )
    logger.info("Loading %s rows to BigQuery", transform_result.get("transformed_rows"))
    # In production: BigQueryHook().run_query() or use BigQueryInsertJobOperator


def llm_triage_on_failure(context: dict) -> None:
    """
    Airflow on_failure_callback that invokes LLM triage.
    Attached to all tasks for automatic failure analysis.
    """
    from src.llm_orchestrator import LLMOrchestrator, TriageContext

    ti = context["task_instance"]
    exception = context.get("exception", "Unknown error")

    logger.info("LLM triage triggered for dag=%s task=%s", ti.dag_id, ti.task_id)

    orchestrator = LLMOrchestrator(
        project_id=GCP_PROJECT_ID,
        model=LLM_MODEL,
    )
    triage_ctx = TriageContext(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        error_message=str(exception),
        execution_date=str(context["execution_date"]),
        retry_count=ti.try_number - 1,
    )
    plan = orchestrator.triage_failure(triage_ctx)
    logger.info(
        "LLM recommendation: action=%s delay=%dm explanation=%s",
        plan.action, plan.delay_minutes, plan.explanation
    )
    # Store plan in XCom for potential use by downstream self-healing tasks
    ti.xcom_push(key="llm_remediation_plan", value={
        "action": plan.action,
        "delay_minutes": plan.delay_minutes,
        "explanation": plan.explanation,
        "confidence": plan.confidence_score,
    })


def audit_log(**context: Any) -> None:
    """Write execution audit record to BigQuery."""
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)

    record = {
        "execution_id": context["run_id"],
        "dag_id": context["dag"].dag_id,
        "task_id": "pipeline_complete",
        "execution_date": context["ds"],
        "status": "success",
        "created_at": datetime.utcnow().isoformat(),
    }
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dag_execution_audit"
    errors = client.insert_rows_json(table_id, [record])
    if errors:
        logger.error("BigQuery audit insert errors: %s", errors)
    else:
        logger.info("Audit record written to %s", table_id)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="genai_orchestrator_pipeline",
    default_args=DEFAULT_ARGS,
    description="LLM-powered ETL pipeline with anomaly detection and self-healing",
    schedule_interval="0 2 * * *",  # Daily at 2am UTC
    catchup=False,
    max_active_runs=1,
    tags=["genai", "llm", "self-healing", "production"],
    doc_md="""
    # GenAI Orchestrator Pipeline

    Production ETL pipeline with:
    - **LLM-powered failure triage** via Vertex AI Gemini
    - **Anomaly detection** on row counts and durations
    - **Self-healing** capabilities for common failure patterns
    - **BigQuery audit logging** for full observability

    Owner: Jagadeesh Thiruveedula | Team: Data Engineering
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_pipeline_data,
        on_failure_callback=llm_triage_on_failure,
        doc_md="Extract raw data from GCS source buckets",
    )

    check_anomalies = BranchPythonOperator(
        task_id="check_anomalies",
        python_callable=run_anomaly_detection,
        doc_md="Run multi-layer anomaly detection on extraction metrics",
    )

    handle_anomaly = PythonOperator(
        task_id="handle_anomaly",
        python_callable=handle_anomaly_task,
        doc_md="Alert and halt on critical anomalies",
    )

    proceed = EmptyOperator(
        task_id="proceed_to_transform",
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        on_failure_callback=llm_triage_on_failure,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="BigQuery SQL transformation layer",
    )

    load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        on_failure_callback=llm_triage_on_failure,
        doc_md="Load transformed data to BigQuery target tables",
    )

    audit = PythonOperator(
        task_id="audit_log",
        python_callable=audit_log,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Write execution audit record to BigQuery monitoring table",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # DAG dependencies
    start >> extract >> check_anomalies
    check_anomalies >> [handle_anomaly, proceed]
    proceed >> transform >> load >> audit >> end
    handle_anomaly >> end

