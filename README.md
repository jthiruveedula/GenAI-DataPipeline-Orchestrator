# 🤖 GenAI-DataPipeline-Orchestrator

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![GCP Cloud Composer](https://img.shields.io/badge/GCP-Cloud%20Composer-4285F4?logo=google-cloud)](https://cloud.google.com/composer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GenAI Powered](https://img.shields.io/badge/GenAI-Powered-ff6b35)](https://cloud.google.com/vertex-ai)

> **LLM-powered Airflow DAG orchestration** with intelligent retry logic, anomaly detection, and self-healing pipelines for GCP Cloud Composer. Built for production-grade data engineering at scale.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    GCP Cloud Composer (Airflow 2.7)              │
│                                                                   │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐    │
│  │  DAG Runner │───▶│ LLM Orchestr │───▶│ Self-Healing Eng │    │
│  │  (Trigger)  │    │  (Vertex AI) │    │  (Auto-Remediate)│    │
│  └─────────────┘    └──────────────┘    └──────────────────┘    │
│          │                  │                      │              │
│          ▼                  ▼                      ▼              │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐    │
│  │  Anomaly    │    │  Retry Logic │    │  Alert Manager   │    │
│  │  Detector   │    │  (Adaptive)  │    │  (PagerDuty/Slack│    │
│  └─────────────┘    └──────────────┘    └──────────────────┘    │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              BigQuery │ GCS │ Pub/Sub │ Dataflow           │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

| Feature | Description |
|--------|-------------|
| 🧠 **LLM-Powered Triage** | Vertex AI Gemini analyzes task failures and recommends remediation strategies |
| 🔄 **Intelligent Retry** | Adaptive backoff with context-aware retry windows based on failure classification |
| 📊 **Anomaly Detection** | Statistical + ML-based detection of pipeline duration and data volume anomalies |
| 🩺 **Self-Healing DAGs** | Automated task re-routing, dependency skipping, and upstream repair triggers |
| 📡 **Real-time Monitoring** | Cloud Monitoring dashboards with custom metrics and SLA breach alerts |
| 🔐 **Secret Management** | Google Secret Manager integration for credentials and API keys |
| 📝 **Audit Logging** | Full lineage tracking via BigQuery audit tables with task-level metadata |

---

## 🗂️ Project Structure

```
GenAI-DataPipeline-Orchestrator/
├── dags/
│   ├── genai_orchestrator_dag.py        # Main orchestration DAG
│   ├── self_healing_dag.py              # Self-healing pipeline DAG
│   └── anomaly_detection_dag.py        # Scheduled anomaly checks
├── plugins/
│   ├── operators/
│   │   ├── llm_triage_operator.py       # Custom Airflow operator for LLM triage
│   │   └── self_healing_operator.py     # Self-healing execution operator
│   └── hooks/
│       └── vertex_ai_hook.py            # Vertex AI custom hook
├── src/
│   ├── llm_orchestrator.py             # Core LLM orchestration engine
│   ├── anomaly_detector.py             # Anomaly detection module
│   ├── self_healer.py                  # Self-healing logic
│   ├── retry_strategy.py               # Adaptive retry strategies
│   └── alert_manager.py               # Alert routing and notifications
├── config/
│   ├── composer_config.yaml            # Cloud Composer environment config
│   ├── retry_policies.yaml             # Retry policy definitions
│   └── anomaly_thresholds.yaml        # Anomaly detection thresholds
├── tests/
│   ├── unit/
│   └── integration/
├── .github/
│   └── workflows/
│       └── ci_cd.yml                   # GitHub Actions CI/CD pipeline
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- GCP Project with Cloud Composer 2.x environment
- Python 3.10+
- Vertex AI API enabled
- BigQuery dataset for audit logging

### Installation

```bash
# Clone the repository
git clone https://github.com/jthiruveedula/GenAI-DataPipeline-Orchestrator.git
cd GenAI-DataPipeline-Orchestrator

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GCP_PROJECT_ID="your-gcp-project-id"
export COMPOSER_ENVIRONMENT="your-composer-env"
export COMPOSER_LOCATION="us-central1"
```

### Deploy to Cloud Composer

```bash
# Upload DAGs to Composer GCS bucket
gcloud composer environments storage dags import \
  --environment $COMPOSER_ENVIRONMENT \
  --location $COMPOSER_LOCATION \
  --source dags/

# Upload plugins
gcloud composer environments storage plugins import \
  --environment $COMPOSER_ENVIRONMENT \
  --location $COMPOSER_LOCATION \
  --source plugins/

# Set Airflow variables
gcloud composer environments run $COMPOSER_ENVIRONMENT \
  --location $COMPOSER_LOCATION \
  variables set -- \
  llm_model_endpoint "projects/YOUR_PROJECT/locations/us-central1/publishers/google/models/gemini-pro"
```

---

## 🧠 LLM Orchestration Engine

The core intelligence layer uses **Vertex AI Gemini Pro** to analyze pipeline failures and generate remediation plans:

```python
from src.llm_orchestrator import LLMOrchestrator

orchestrator = LLMOrchestrator(
    project_id="your-gcp-project",
    model="gemini-1.5-pro",
    temperature=0.1
)

# Analyze a task failure and get remediation strategy
remedy = orchestrator.triage_failure(
    dag_id="etl_pipeline_prod",
    task_id="bq_load_task",
    error_message="Quota exceeded for BigQuery load jobs",
    execution_date="2024-01-15"
)

print(remedy.action)        # "RETRY_WITH_BACKOFF"
print(remedy.delay_minutes) # 15
print(remedy.explanation)   # "BigQuery quota typically resets within 10-15 min..."
```

---

## 📊 Anomaly Detection

Multi-layer anomaly detection combining statistical methods with ML:

```python
from src.anomaly_detector import AnomalyDetector

detector = AnomalyDetector(
    bq_dataset="pipeline_monitoring",
    lookback_days=30,
    sensitivity="medium"  # low | medium | high
)

# Check for duration anomalies
anomalies = detector.check_dag_duration(
    dag_id="etl_pipeline_prod",
    current_duration_seconds=3600
)

for anomaly in anomalies:
    print(f"Type: {anomaly.type}")
    print(f"Severity: {anomaly.severity}")
    print(f"Expected range: {anomaly.expected_range}")
    print(f"Deviation: {anomaly.deviation_pct:.1f}%")
```

**Detection Methods:**
- Z-score based statistical outlier detection
- IQR (Interquartile Range) for robust outlier handling
- Exponential moving average for trend analysis
- DBSCAN clustering for multi-dimensional anomaly patterns

---

## 🩺 Self-Healing Capabilities

| Failure Type | Auto-Remediation Action |
|-------------|-------------------------|
| `QuotaExceeded` | Exponential backoff + quota check |
| `DeadlineExceeded` | Task timeout increase + retry |
| `ResourcesExhausted` | Scale-up trigger + requeue |
| `DataNotFound` | Upstream DAG trigger + wait |
| `SchemaChange` | Schema evolution handler + alert |
| `NetworkTimeout` | Region failover + retry |
| `AuthenticationError` | Secret refresh + retry |

---

## ⚙️ Configuration

### retry_policies.yaml

```yaml
retry_policies:
  default:
    max_retries: 3
    initial_delay: 300  # 5 minutes
    backoff_multiplier: 2.0
    max_delay: 3600     # 1 hour

  quota_exceeded:
    max_retries: 5
    initial_delay: 600
    backoff_multiplier: 2.5
    llm_triage: true

  data_quality:
    max_retries: 2
    initial_delay: 60
    alert_on_first_failure: true
    self_healing: true
```

---

## 📈 Monitoring & Observability

### Cloud Monitoring Metrics

```python
# Custom metrics published to Cloud Monitoring
metrics = [
    "custom.googleapis.com/airflow/dag_success_rate",
    "custom.googleapis.com/airflow/task_retry_count",
    "custom.googleapis.com/airflow/llm_triage_latency",
    "custom.googleapis.com/airflow/self_healing_actions",
    "custom.googleapis.com/airflow/anomaly_detected_count"
]
```

### BigQuery Audit Table Schema

```sql
CREATE TABLE pipeline_monitoring.dag_execution_audit (
  execution_id       STRING NOT NULL,
  dag_id             STRING NOT NULL,
  task_id            STRING NOT NULL,
  execution_date     TIMESTAMP NOT NULL,
  status             STRING NOT NULL,
  duration_seconds   INT64,
  retry_count        INT64,
  failure_reason     STRING,
  llm_triage_result  JSON,
  self_healing_action STRING,
  resolved           BOOL,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PARTITION BY DATE(execution_date)
CLUSTER BY dag_id, status;
```

---

## 🧪 Testing

```bash
# Run unit tests
pytest tests/unit/ -v --cov=src --cov-report=html

# Run integration tests (requires GCP access)
pytest tests/integration/ -v -m integration

# Run DAG integrity checks
python -m pytest tests/test_dag_integrity.py
```

---

## 🔧 Tech Stack

| Layer | Technology |
|-------|------------|
| Orchestration | Apache Airflow 2.7 on GCP Cloud Composer 2 |
| AI/LLM | Vertex AI Gemini 1.5 Pro |
| Data Warehouse | BigQuery |
| Streaming | Pub/Sub, Dataflow |
| Storage | Google Cloud Storage |
| Monitoring | Cloud Monitoring, Cloud Logging |
| Secrets | Google Secret Manager |
| CI/CD | GitHub Actions + Cloud Build |
| Language | Python 3.10+ |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Jagadeesh Thiruveedula**
- GitHub: [@jthiruveedula](https://github.com/jthiruveedula)
- Expertise: GCP Data Engineering, LLM/GenAI, Apache Airflow, BigQuery

---

*Built with ❤️ for production-grade GenAI data engineering on Google Cloud Platform*
