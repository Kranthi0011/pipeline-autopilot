# 🚀 Pipeline Autopilot

**MLOps CI/CD Pipeline Failure Prediction System**

Predict pipeline failures BEFORE they happen using Machine Learning, and explain root causes using RAG.

---

## 📋 Project Overview

Pipeline Autopilot is an MLOps system that:
1. **Predicts pipeline failures** BEFORE they happen using ML (XGBoost/LightGBM)
2. **Explains root causes** and suggests fixes using RAG (Vertex AI Gemini)
3. **Built on Google Cloud Platform** with Airflow orchestration

### Problem We're Solving
- Data pipelines fail unexpectedly → engineers waste hours debugging
- Our solution: Don't run a pipeline if it's likely to fail. Warn first, suggest fixes.

---

## 👥 Team Members

| Member | Role | Responsibilities |
|--------|------|------------------|
| Member 1 | Pipeline Architect | Folder structure, config.py, Airflow DAG, Docker setup |
| Member 2 | Data Engineer | Data acquisition, preprocessing scripts |
| Member 3 | Quality Engineer | Schema validation, bias detection |
| Member 4 | ML Engineer | Anomaly detection, model training |
| Member 5 | MLOps Engineer | DVC versioning, experiment tracking |
| Member 6 | Frontend Engineer | Streamlit dashboard, deployment |

---

## 📁 Project Structure

```
Data-Pipeline-Autopilot/
├── dags/
│   └── pipeline_dag.py          # Main Airflow DAG
├── scripts/
│   ├── config.py                # Central configuration ✅
│   ├── data_acquisition.py      # Load raw data
│   ├── data_preprocessing.py    # Clean & transform data
│   ├── schema_validation.py     # Validate data schema
│   ├── bias_detection.py        # Detect data bias
│   ├── anomaly_detection.py     # Find outliers
│   └── dvc_versioning.py        # Version data with DVC
├── data/
│   ├── raw/                     # Raw dataset (150K rows)
│   ├── processed/               # Cleaned dataset
│   ├── schema/                  # Schema definitions
│   └── reports/                 # Generated reports
├── tests/                       # Unit tests
├── logs/                        # Airflow logs
├── docker-compose.yaml          # Airflow infrastructure ✅
├── requirements.txt             # Python dependencies ✅
├── dvc.yaml                     # DVC pipeline definition
├── .env                         # Environment variables
├── .gitignore                   # Git ignore rules ✅
└── README.md                    # This file ✅
```

---

## 📊 Dataset

**Total: 150K rows, 26 features**

| Category | Columns |
|----------|---------|
| **ID** | run_id |
| **Datetime** | trigger_time |
| **Temporal** | day_of_week, hour, is_weekend |
| **Performance** | duration_seconds, avg_duration_7_runs, duration_deviation |
| **Historical** | prev_run_status, failures_last_7_runs, workflow_failure_rate, hours_since_last_run |
| **Complexity** | total_jobs, failed_jobs, retry_count, concurrent_runs |
| **Risk** | head_branch, is_main_branch, is_first_run, is_bot_triggered, trigger_type |
| **Categorical** | pipeline_name, repo, failure_type, error_message |
| **Target** | failed (0 or 1) |

---

## 🔄 Pipeline Flow (DAG)

```
data_acquisition 
    → data_preprocessing 
        → [schema_validation || bias_detection]  (parallel)
            → anomaly_detection 
                → dvc_versioning 
                    → pipeline_complete
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+
- Git

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/pipeline-autopilot.git
cd pipeline-autopilot
```

### 2. Add Your Dataset
Place your `final_dataset.csv` in `data/raw/` folder.

### 3. Start Airflow
```bash
docker-compose up -d
```

### 4. Access Airflow UI
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 5. Run the Pipeline
1. Enable the DAG (toggle switch)
2. Click "Trigger DAG" (play button)
3. Watch tasks complete in Graph view

### 6. Stop Airflow
```bash
docker-compose down
```

---

## ⚙️ Configuration

All settings are centralized in `scripts/config.py`:
- File paths
- Column definitions
- Validation rules
- Anomaly detection thresholds
- Bias detection settings
- Airflow DAG configuration

---

## 📦 Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow |
| Containerization | Docker |
| Data Processing | Pandas, NumPy |
| ML Models | XGBoost, LightGBM |
| RAG | Vertex AI Gemini |
| Data Versioning | DVC |
| Cloud | Google Cloud Platform |
| Dashboard | Streamlit |
| Database | PostgreSQL (Airflow metadata) |

---

## 📅 Timeline

| Deadline | Deliverable |
|----------|-------------|
| Feb 24, 2026 | Data Pipeline Submission |
| TBD | Full Project Submission |

---

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Test config
python scripts/config.py
```

---

## 📝 License

This project is for educational purposes (MLOps Course Project).

---

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit changes: `git commit -m "Add your feature"`
3. Push to branch: `git push origin feature/your-feature`
4. Open a Pull Request

---

**Built with ❤️ by the Pipeline Autopilot Team**