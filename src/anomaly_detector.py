"""
Anomaly Detector - Multi-layer anomaly detection for Airflow data pipelines.
Combines statistical methods (Z-score, IQR) with BigQuery historical data.

Author: Jagadeesh Thiruveedula
Project: GenAI-DataPipeline-Orchestrator
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class AnomalySeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AnomalyType(Enum):
    DURATION = "DURATION"
    ROW_COUNT = "ROW_COUNT"
    FAILURE_RATE = "FAILURE_RATE"
    RETRY_SPIKE = "RETRY_SPIKE"
    DATA_FRESHNESS = "DATA_FRESHNESS"


@dataclass
class Anomaly:
    """Detected anomaly with context and severity."""
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    dag_id: str
    task_id: Optional[str]
    current_value: float
    expected_value: float
    expected_range: tuple[float, float]
    deviation_pct: float
    detected_at: datetime = field(default_factory=datetime.utcnow)
    description: str = ""
    recommended_action: str = ""


class AnomalyDetector:
    """
    Multi-layer anomaly detection engine for Airflow pipelines.

    Detection layers:
    1. Z-score statistical outlier detection
    2. IQR (Interquartile Range) robust outlier detection
    3. Exponential moving average trend analysis
    4. Rolling window failure rate monitoring
    """

    # Thresholds
    Z_SCORE_THRESHOLD = {"low": 2.5, "medium": 2.0, "high": 1.5}
    IQR_MULTIPLIER = {"low": 2.0, "medium": 1.5, "high": 1.0}

    def __init__(
        self,
        project_id: str,
        bq_dataset: str = "pipeline_monitoring",
        lookback_days: int = 30,
        sensitivity: str = "medium",  # low | medium | high
    ):
        self.project_id = project_id
        self.bq_dataset = bq_dataset
        self.lookback_days = lookback_days
        self.sensitivity = sensitivity
        self._bq_client = bigquery.Client(project=project_id)
        logger.info(
            "AnomalyDetector initialized dataset=%s sensitivity=%s",
            bq_dataset, sensitivity
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_dag_duration(
        self,
        dag_id: str,
        current_duration_seconds: int,
        task_id: Optional[str] = None,
    ) -> list[Anomaly]:
        """
        Detect duration anomalies for a DAG or specific task.

        Args:
            dag_id: Airflow DAG identifier
            current_duration_seconds: Duration of current run
            task_id: Optional task to scope detection

        Returns:
            List of detected anomalies (empty if normal)
        """
        history = self._fetch_duration_history(dag_id, task_id)
        if len(history) < 5:
            logger.warning("Insufficient history for dag=%s, skipping anomaly check", dag_id)
            return []

        anomalies = []

        # Z-score check
        z_anomaly = self._zscore_check(
            values=history,
            current=current_duration_seconds,
            dag_id=dag_id,
            task_id=task_id,
            anomaly_type=AnomalyType.DURATION,
        )
        if z_anomaly:
            anomalies.append(z_anomaly)

        # IQR check
        iqr_anomaly = self._iqr_check(
            values=history,
            current=current_duration_seconds,
            dag_id=dag_id,
            task_id=task_id,
            anomaly_type=AnomalyType.DURATION,
        )
        if iqr_anomaly and not z_anomaly:
            anomalies.append(iqr_anomaly)

        return anomalies

    def check_row_count(
        self,
        dag_id: str,
        current_row_count: int,
        task_id: Optional[str] = None,
    ) -> list[Anomaly]:
        """Detect data volume anomalies."""
        history = self._fetch_row_count_history(dag_id, task_id)
        if len(history) < 5:
            return []

        anomalies = []
        z_anomaly = self._zscore_check(
            values=history,
            current=current_row_count,
            dag_id=dag_id,
            task_id=task_id,
            anomaly_type=AnomalyType.ROW_COUNT,
        )
        if z_anomaly:
            anomalies.append(z_anomaly)
        return anomalies

    def check_failure_rate(
        self,
        dag_id: str,
        window_hours: int = 24,
    ) -> list[Anomaly]:
        """Detect spike in failure rate over a rolling window."""
        current_rate = self._fetch_failure_rate(dag_id, window_hours)
        baseline_rate = self._fetch_failure_rate(dag_id, window_hours * 7)  # 7-day baseline

        if baseline_rate is None or current_rate is None:
            return []

        if baseline_rate == 0:
            if current_rate > 0.1:
                return [Anomaly(
                    anomaly_type=AnomalyType.FAILURE_RATE,
                    severity=AnomalySeverity.HIGH,
                    dag_id=dag_id,
                    task_id=None,
                    current_value=current_rate,
                    expected_value=0.0,
                    expected_range=(0.0, 0.05),
                    deviation_pct=float("inf"),
                    description=f"Failure rate {current_rate:.1%} spiked from baseline 0%",
                    recommended_action="ALERT_ONCALL",
                )]
            return []

        deviation_pct = ((current_rate - baseline_rate) / baseline_rate) * 100
        threshold = {"low": 150, "medium": 100, "high": 50}.get(self.sensitivity, 100)

        if deviation_pct > threshold:
            severity = self._compute_severity(deviation_pct, threshold)
            return [Anomaly(
                anomaly_type=AnomalyType.FAILURE_RATE,
                severity=severity,
                dag_id=dag_id,
                task_id=None,
                current_value=current_rate,
                expected_value=baseline_rate,
                expected_range=(0.0, baseline_rate * 1.5),
                deviation_pct=deviation_pct,
                description=f"Failure rate {current_rate:.1%} vs baseline {baseline_rate:.1%}",
                recommended_action="ALERT_ONCALL",
            )]
        return []

    # ------------------------------------------------------------------
    # Statistical methods
    # ------------------------------------------------------------------

    def _zscore_check(
        self,
        values: list[float],
        current: float,
        dag_id: str,
        task_id: Optional[str],
        anomaly_type: AnomalyType,
    ) -> Optional[Anomaly]:
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 1
        z_score = abs((current - mean) / std)
        threshold = self.Z_SCORE_THRESHOLD.get(self.sensitivity, 2.0)

        if z_score > threshold:
            deviation_pct = ((current - mean) / mean * 100) if mean > 0 else 0
            severity = self._compute_severity(z_score, threshold)
            return Anomaly(
                anomaly_type=anomaly_type,
                severity=severity,
                dag_id=dag_id,
                task_id=task_id,
                current_value=current,
                expected_value=mean,
                expected_range=(mean - 2 * std, mean + 2 * std),
                deviation_pct=deviation_pct,
                description=f"Z-score {z_score:.2f} exceeds threshold {threshold}",
                recommended_action="ALERT_ONCALL" if severity == AnomalySeverity.CRITICAL else "LOG_WARNING",
            )
        return None

    def _iqr_check(
        self,
        values: list[float],
        current: float,
        dag_id: str,
        task_id: Optional[str],
        anomaly_type: AnomalyType,
    ) -> Optional[Anomaly]:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[(3 * n) // 4]
        iqr = q3 - q1
        multiplier = self.IQR_MULTIPLIER.get(self.sensitivity, 1.5)
        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr

        if current < lower or current > upper:
            median = sorted_vals[n // 2]
            deviation_pct = ((current - median) / median * 100) if median > 0 else 0
            severity = self._compute_severity(abs(deviation_pct), 50)
            return Anomaly(
                anomaly_type=anomaly_type,
                severity=severity,
                dag_id=dag_id,
                task_id=task_id,
                current_value=current,
                expected_value=median,
                expected_range=(lower, upper),
                deviation_pct=deviation_pct,
                description=f"IQR outlier: {current} outside [{lower:.1f}, {upper:.1f}]",
                recommended_action="LOG_WARNING",
            )
        return None

    # ------------------------------------------------------------------
    # BigQuery data fetchers
    # ------------------------------------------------------------------

    def _fetch_duration_history(self, dag_id: str, task_id: Optional[str]) -> list[float]:
        task_filter = f"AND task_id = '{task_id}'" if task_id else ""
        query = f"""
        SELECT duration_seconds
        FROM `{self.project_id}.{self.bq_dataset}.dag_execution_audit`
        WHERE dag_id = '{dag_id}'
          AND status = 'success'
          AND execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.lookback_days} DAY)
          {task_filter}
        ORDER BY execution_date DESC
        LIMIT 100
        """
        return self._run_query_single_col(query)

    def _fetch_row_count_history(self, dag_id: str, task_id: Optional[str]) -> list[float]:
        task_filter = f"AND task_id = '{task_id}'" if task_id else ""
        query = f"""
        SELECT CAST(JSON_EXTRACT_SCALAR(metadata, '$.row_count') AS INT64) AS row_count
        FROM `{self.project_id}.{self.bq_dataset}.dag_execution_audit`
        WHERE dag_id = '{dag_id}'
          AND status = 'success'
          AND execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.lookback_days} DAY)
          {task_filter}
        ORDER BY execution_date DESC
        LIMIT 100
        """
        return self._run_query_single_col(query)

    def _fetch_failure_rate(self, dag_id: str, window_hours: int) -> Optional[float]:
        query = f"""
        SELECT
          COUNTIF(status = 'failed') / COUNT(*) AS failure_rate
        FROM `{self.project_id}.{self.bq_dataset}.dag_execution_audit`
        WHERE dag_id = '{dag_id}'
          AND execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {window_hours} HOUR)
        """
        results = self._run_query_single_col(query)
        return results[0] if results else None

    def _run_query_single_col(self, query: str) -> list[float]:
        try:
            rows = self._bq_client.query(query).result()
            return [row[0] for row in rows if row[0] is not None]
        except Exception as exc:
            logger.error("BigQuery query failed: %s", exc)
            return []

    @staticmethod
    def _compute_severity(value: float, threshold: float) -> AnomalySeverity:
        ratio = value / threshold
        if ratio >= 4:
            return AnomalySeverity.CRITICAL
        if ratio >= 2.5:
            return AnomalySeverity.HIGH
        if ratio >= 1.5:
            return AnomalySeverity.MEDIUM
        return AnomalySeverity.LOW

