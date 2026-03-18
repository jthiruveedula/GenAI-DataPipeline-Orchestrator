"""
LLM Orchestrator - Core intelligence engine for GenAI-DataPipeline-Orchestrator
Uses Vertex AI Gemini to analyze Airflow DAG failures and generate remediation strategies.

Author: Jagadeesh Thiruveedula
Project: GenAI-DataPipeline-Orchestrator
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import vertexai
from google.cloud import secretmanager
from vertexai.generative_models import GenerativeModel, GenerationConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class RemediationPlan:
    """Structured remediation plan returned by the LLM triage engine."""
    action: str                        # e.g. RETRY_WITH_BACKOFF, SKIP_TASK, ALERT_ONCALL
    delay_minutes: int = 5
    max_retries: int = 3
    explanation: str = ""
    confidence_score: float = 0.0
    tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class TriageContext:
    """Context payload sent to the LLM for failure analysis."""
    dag_id: str
    task_id: str
    error_message: str
    execution_date: str
    retry_count: int = 0
    task_duration_seconds: Optional[int] = None
    upstream_failed_tasks: list[str] = field(default_factory=list)
    recent_dag_history: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# LLM Orchestrator
# ---------------------------------------------------------------------------

class LLMOrchestrator:
    """
    Core LLM-powered orchestration engine.

    Leverages Vertex AI Gemini to:
    - Classify failure root causes
    - Generate context-aware remediation plans
    - Recommend retry strategies and delays
    - Escalate unresolvable issues to on-call engineers
    """

    SYSTEM_PROMPT = """
    You are an expert Apache Airflow and GCP data pipeline reliability engineer.
    Analyze the provided task failure context and return a JSON remediation plan.

    Possible actions:
    - RETRY_WITH_BACKOFF: Retry after a calculated delay
    - RETRY_IMMEDIATELY: Retry without delay (transient errors)
    - SKIP_TASK: Skip and continue downstream
    - TRIGGER_UPSTREAM: Re-run upstream dependencies first
    - ALERT_ONCALL: Escalate to on-call engineer
    - SCALE_RESOURCES: Request additional compute resources
    - REFRESH_CREDENTIALS: Rotate and refresh API credentials

    Return ONLY valid JSON with fields: action, delay_minutes, max_retries,
    explanation, confidence_score (0.0-1.0), tags (list).
    """

    def __init__(
        self,
        project_id: str,
        location: str = "us-central1",
        model: str = "gemini-1.5-pro",
        temperature: float = 0.1,
        secret_name: Optional[str] = None,
    ):
        self.project_id = project_id
        self.location = location
        self.model_name = model
        self.temperature = temperature

        vertexai.init(project=project_id, location=location)
        self._model = GenerativeModel(
            model_name=model,
            system_instruction=self.SYSTEM_PROMPT,
        )
        self._generation_config = GenerationConfig(
            temperature=temperature,
            max_output_tokens=1024,
            response_mime_type="application/json",
        )
        logger.info("LLMOrchestrator initialized with model=%s project=%s", model, project_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def triage_failure(self, ctx: TriageContext) -> RemediationPlan:
        """
        Analyze a task failure and return a structured remediation plan.

        Args:
            ctx: TriageContext containing failure details

        Returns:
            RemediationPlan with recommended action and parameters
        """
        prompt = self._build_prompt(ctx)
        try:
            response = self._model.generate_content(
                prompt,
                generation_config=self._generation_config,
            )
            plan = self._parse_response(response.text)
            logger.info(
                "LLM triage complete dag=%s task=%s action=%s confidence=%.2f",
                ctx.dag_id, ctx.task_id, plan.action, plan.confidence_score
            )
            return plan
        except Exception as exc:
            logger.error("LLM triage failed: %s. Falling back to default retry.", exc)
            return self._default_plan(ctx)

    def classify_error(self, error_message: str) -> str:
        """
        Quickly classify error type without full triage context.

        Returns one of: QUOTA_ERROR, TRANSIENT_ERROR, DATA_ERROR,
        AUTH_ERROR, INFRA_ERROR, UNKNOWN
        """
        error_lower = error_message.lower()
        if any(k in error_lower for k in ["quota", "rate limit", "resource exhausted"]):
            return "QUOTA_ERROR"
        if any(k in error_lower for k in ["timeout", "deadline exceeded", "connection reset"]):
            return "TRANSIENT_ERROR"
        if any(k in error_lower for k in ["not found", "schema", "parse error", "invalid"]):
            return "DATA_ERROR"
        if any(k in error_lower for k in ["permission denied", "unauthorized", "unauthenticated"]):
            return "AUTH_ERROR"
        if any(k in error_lower for k in ["oom", "memory", "cpu", "disk"]):
            return "INFRA_ERROR"
        return "UNKNOWN"

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_prompt(self, ctx: TriageContext) -> str:
        return f"""
        Analyze the following Airflow task failure and provide a remediation plan.

        DAG ID: {ctx.dag_id}
        Task ID: {ctx.task_id}
        Execution Date: {ctx.execution_date}
        Retry Count: {ctx.retry_count}
        Task Duration: {ctx.task_duration_seconds}s
        Error Message: {ctx.error_message}
        Upstream Failed Tasks: {', '.join(ctx.upstream_failed_tasks) or 'None'}
        Recent History (last 5 runs): {json.dumps(ctx.recent_dag_history, indent=2)}

        Return a JSON remediation plan.
        """

    def _parse_response(self, response_text: str) -> RemediationPlan:
        data = json.loads(response_text)
        return RemediationPlan(
            action=data.get("action", "RETRY_WITH_BACKOFF"),
            delay_minutes=int(data.get("delay_minutes", 5)),
            max_retries=int(data.get("max_retries", 3)),
            explanation=data.get("explanation", ""),
            confidence_score=float(data.get("confidence_score", 0.5)),
            tags=data.get("tags", []),
        )

    def _default_plan(self, ctx: TriageContext) -> RemediationPlan:
        """Fallback remediation when LLM call fails."""
        error_class = self.classify_error(ctx.error_message)
        delay_map = {
            "QUOTA_ERROR": 15,
            "TRANSIENT_ERROR": 2,
            "DATA_ERROR": 0,
            "AUTH_ERROR": 1,
            "INFRA_ERROR": 10,
            "UNKNOWN": 5,
        }
        action_map = {
            "QUOTA_ERROR": "RETRY_WITH_BACKOFF",
            "TRANSIENT_ERROR": "RETRY_IMMEDIATELY",
            "DATA_ERROR": "ALERT_ONCALL",
            "AUTH_ERROR": "REFRESH_CREDENTIALS",
            "INFRA_ERROR": "SCALE_RESOURCES",
            "UNKNOWN": "RETRY_WITH_BACKOFF",
        }
        return RemediationPlan(
            action=action_map[error_class],
            delay_minutes=delay_map[error_class],
            max_retries=3,
            explanation=f"Fallback plan for {error_class} (LLM unavailable)",
            confidence_score=0.6,
            tags=[error_class, "FALLBACK"],
        )

