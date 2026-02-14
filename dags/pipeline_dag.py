"""
pipeline_dag.py
===============
Main Airflow DAG for Pipeline Autopilot Data Pipeline.
Orchestrates: data acquisition → preprocessing → validation → anomaly detection → versioning

Author: Member 1 (Pipeline Architect)
Date: February 2026
Project: Pipeline Autopilot - CI/CD Failure Prediction System

DAG Flow:
    data_acquisition 
        → data_preprocessing 
            → [schema_validation || bias_detection]  (parallel)
                → anomaly_detection 
                    → dvc_versioning 
                        → pipeline_complete
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging
import sys
import os

# Add scripts directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# =============================================================================
# IMPORT TASK FUNCTIONS FROM SCRIPTS
# =============================================================================

from config import DAG_CONFIG, LOGGING_CONFIG

# Import task functions (these will be created by other team members)
# For now, we create wrapper functions that will call the actual implementations

# =============================================================================
# LOGGING SETUP
# =============================================================================

logger = logging.getLogger(__name__)
logger.setLevel(LOGGING_CONFIG.get("log_level", "INFO"))

# =============================================================================
# CALLBACK FUNCTIONS
# =============================================================================

def on_task_failure(context):
    """
    Callback function triggered when a task fails.
    Logs error details and can send alerts.
    
    Args:
        context: Airflow context dictionary containing task instance info
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    error_message = f"""
    ❌ TASK FAILURE ALERT
    =====================
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {execution_date}
    Error: {str(exception)}
    """
    
    logger.error(error_message)
    print(error_message)
    
    # TODO: Add Slack/Email notification here
    # Example: send_slack_alert(error_message)


def on_task_success(context):
    """
    Callback function triggered when a task succeeds.
    
    Args:
        context: Airflow context dictionary containing task instance info
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    
    logger.info(f"✅ Task '{task_id}' completed successfully")


def on_task_retry(context):
    """
    Callback function triggered when a task retries.
    
    Args:
        context: Airflow context dictionary containing task instance info
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    try_number = task_instance.try_number
    
    logger.warning(f"🔄 Task '{task_id}' retrying (attempt {try_number})")

# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def run_data_acquisition(**context):
    """
    Task 1: Load and validate raw data.
    Reads the raw dataset and performs initial checks.
    """
    logger.info("=" * 60)
    logger.info("TASK: DATA ACQUISITION - Starting")
    logger.info("=" * 60)
    
    try:
        # Import here to avoid circular imports
        from data_acquisition import acquire_data
        
        result = acquire_data()
        
        # Push result to XCom for downstream tasks
        context['ti'].xcom_push(key='acquisition_status', value=result)
        
        logger.info("✅ Data acquisition completed successfully")
        return result
        
    except ImportError:
        # Fallback if script not yet implemented
        logger.warning("⚠️ data_acquisition.py not implemented yet, using placeholder")
        
        from config import RAW_DATASET_PATH
        import pandas as pd
        
        # Basic acquisition: just verify file exists and is readable
        if not RAW_DATASET_PATH.exists():
            raise FileNotFoundError(f"Raw dataset not found: {RAW_DATASET_PATH}")
        
        df = pd.read_csv(RAW_DATASET_PATH)
        row_count = len(df)
        col_count = len(df.columns)
        
        result = {
            "status": "success",
            "rows": row_count,
            "columns": col_count,
            "file_path": str(RAW_DATASET_PATH)
        }
        
        logger.info(f"📊 Loaded {row_count:,} rows, {col_count} columns")
        context['ti'].xcom_push(key='acquisition_status', value=result)
        
        return result


def run_data_preprocessing(**context):
    """
    Task 2: Clean and preprocess the data.
    Handles missing values, encoding, feature engineering.
    """
    logger.info("=" * 60)
    logger.info("TASK: DATA PREPROCESSING - Starting")
    logger.info("=" * 60)
    
    try:
        from data_preprocessing import preprocess_data
        
        result = preprocess_data()
        context['ti'].xcom_push(key='preprocessing_status', value=result)
        
        logger.info("✅ Data preprocessing completed successfully")
        return result
        
    except ImportError:
        logger.warning("⚠️ data_preprocessing.py not implemented yet, using placeholder")
        
        from config import RAW_DATASET_PATH, PROCESSED_DATASET_PATH
        import pandas as pd
        
        # Basic preprocessing placeholder
        df = pd.read_csv(RAW_DATASET_PATH)
        
        # Save to processed (placeholder - no actual preprocessing)
        PROCESSED_DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(PROCESSED_DATASET_PATH, index=False)
        
        result = {
            "status": "success",
            "input_rows": len(df),
            "output_rows": len(df),
            "output_path": str(PROCESSED_DATASET_PATH)
        }
        
        logger.info(f"📊 Processed {len(df):,} rows")
        context['ti'].xcom_push(key='preprocessing_status', value=result)
        
        return result


def run_schema_validation(**context):
    """
    Task 3a: Validate data schema (runs in parallel with bias detection).
    Checks column types, constraints, and data quality rules.
    """
    logger.info("=" * 60)
    logger.info("TASK: SCHEMA VALIDATION - Starting")
    logger.info("=" * 60)
    
    try:
        from schema_validation import validate_schema
        
        result = validate_schema()
        context['ti'].xcom_push(key='schema_validation_status', value=result)
        
        logger.info("✅ Schema validation completed successfully")
        return result
        
    except ImportError:
        logger.warning("⚠️ schema_validation.py not implemented yet, using placeholder")
        
        from config import PROCESSED_DATASET_PATH, ALL_COLUMNS
        import pandas as pd
        
        df = pd.read_csv(PROCESSED_DATASET_PATH)
        
        # Basic schema check
        missing_cols = set(ALL_COLUMNS) - set(df.columns)
        extra_cols = set(df.columns) - set(ALL_COLUMNS)
        
        result = {
            "status": "success" if not missing_cols else "warning",
            "total_columns": len(df.columns),
            "missing_columns": list(missing_cols),
            "extra_columns": list(extra_cols),
            "validation_passed": len(missing_cols) == 0
        }
        
        logger.info(f"📋 Schema check: {len(df.columns)} columns found")
        if missing_cols:
            logger.warning(f"⚠️ Missing columns: {missing_cols}")
        
        context['ti'].xcom_push(key='schema_validation_status', value=result)
        return result


def run_bias_detection(**context):
    """
    Task 3b: Detect bias in the dataset (runs in parallel with schema validation).
    Checks for class imbalance and representation issues.
    """
    logger.info("=" * 60)
    logger.info("TASK: BIAS DETECTION - Starting")
    logger.info("=" * 60)
    
    try:
        from bias_detection import detect_bias
        
        result = detect_bias()
        context['ti'].xcom_push(key='bias_detection_status', value=result)
        
        logger.info("✅ Bias detection completed successfully")
        return result
        
    except ImportError:
        logger.warning("⚠️ bias_detection.py not implemented yet, using placeholder")
        
        from config import PROCESSED_DATASET_PATH, TARGET_COLUMN, BIAS_SETTINGS
        import pandas as pd
        
        df = pd.read_csv(PROCESSED_DATASET_PATH)
        
        # Basic class imbalance check
        if TARGET_COLUMN in df.columns:
            class_counts = df[TARGET_COLUMN].value_counts()
            imbalance_ratio = class_counts.max() / class_counts.min()
        else:
            imbalance_ratio = None
        
        result = {
            "status": "success",
            "target_column": TARGET_COLUMN,
            "class_distribution": class_counts.to_dict() if TARGET_COLUMN in df.columns else {},
            "imbalance_ratio": round(imbalance_ratio, 2) if imbalance_ratio else None,
            "bias_detected": imbalance_ratio > BIAS_SETTINGS["max_imbalance_ratio"] if imbalance_ratio else False
        }
        
        logger.info(f"⚖️ Class imbalance ratio: {imbalance_ratio:.2f}" if imbalance_ratio else "⚠️ Target column not found")
        context['ti'].xcom_push(key='bias_detection_status', value=result)
        
        return result


def run_anomaly_detection(**context):
    """
    Task 4: Detect anomalies in the dataset.
    Uses statistical methods to identify outliers.
    """
    logger.info("=" * 60)
    logger.info("TASK: ANOMALY DETECTION - Starting")
    logger.info("=" * 60)
    
    try:
        from anomaly_detection import detect_anomalies
        
        result = detect_anomalies()
        context['ti'].xcom_push(key='anomaly_detection_status', value=result)
        
        logger.info("✅ Anomaly detection completed successfully")
        return result
        
    except ImportError:
        logger.warning("⚠️ anomaly_detection.py not implemented yet, using placeholder")
        
        from config import PROCESSED_DATASET_PATH, ANOMALY_SETTINGS
        import pandas as pd
        import numpy as np
        
        df = pd.read_csv(PROCESSED_DATASET_PATH)
        
        # Basic anomaly detection using Z-score
        anomaly_summary = {}
        columns_to_check = ANOMALY_SETTINGS["columns_to_check"]
        threshold = ANOMALY_SETTINGS["zscore_threshold"]
        
        for col in columns_to_check:
            if col in df.columns and df[col].dtype in ['int64', 'float64']:
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    z_scores = np.abs((df[col] - mean) / std)
                    anomaly_count = (z_scores > threshold).sum()
                    anomaly_summary[col] = {
                        "anomaly_count": int(anomaly_count),
                        "anomaly_percent": round(anomaly_count / len(df) * 100, 2)
                    }
        
        total_anomalies = sum(v["anomaly_count"] for v in anomaly_summary.values())
        
        result = {
            "status": "success",
            "total_rows": len(df),
            "columns_checked": len(anomaly_summary),
            "anomaly_summary": anomaly_summary,
            "total_anomalies_found": total_anomalies
        }
        
        logger.info(f"🔍 Found {total_anomalies} anomalies across {len(anomaly_summary)} columns")
        context['ti'].xcom_push(key='anomaly_detection_status', value=result)
        
        return result


def run_dvc_versioning(**context):
    """
    Task 5: Version the processed data using DVC.
    Tracks data changes and pushes to remote storage.
    """
    logger.info("=" * 60)
    logger.info("TASK: DVC VERSIONING - Starting")
    logger.info("=" * 60)
    
    try:
        from dvc_versioning import version_data
        
        result = version_data()
        context['ti'].xcom_push(key='dvc_versioning_status', value=result)
        
        logger.info("✅ DVC versioning completed successfully")
        return result
        
    except ImportError:
        logger.warning("⚠️ dvc_versioning.py not implemented yet, using placeholder")
        
        from config import PROCESSED_DATASET_PATH
        from datetime import datetime
        
        # Placeholder - just log version info
        result = {
            "status": "success",
            "versioned_file": str(PROCESSED_DATASET_PATH),
            "version_timestamp": datetime.now().isoformat(),
            "dvc_commit": "placeholder_commit_hash"
        }
        
        logger.info(f"📦 Data versioned at {result['version_timestamp']}")
        context['ti'].xcom_push(key='dvc_versioning_status', value=result)
        
        return result


def run_pipeline_complete(**context):
    """
    Task 6: Final task - summarize pipeline execution.
    Collects results from all tasks and generates report.
    """
    logger.info("=" * 60)
    logger.info("TASK: PIPELINE COMPLETE - Generating Summary")
    logger.info("=" * 60)
    
    ti = context['ti']
    
    # Collect results from all tasks via XCom
    acquisition = ti.xcom_pull(key='acquisition_status', task_ids='data_acquisition')
    preprocessing = ti.xcom_pull(key='preprocessing_status', task_ids='data_preprocessing')
    schema = ti.xcom_pull(key='schema_validation_status', task_ids='schema_validation')
    bias = ti.xcom_pull(key='bias_detection_status', task_ids='bias_detection')
    anomaly = ti.xcom_pull(key='anomaly_detection_status', task_ids='anomaly_detection')
    versioning = ti.xcom_pull(key='dvc_versioning_status', task_ids='dvc_versioning')
    
    # Generate summary report
    summary = f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║           PIPELINE AUTOPILOT - EXECUTION SUMMARY             ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                       ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  📥 DATA ACQUISITION                                         ║
    ║     Status: {acquisition.get('status', 'N/A') if acquisition else 'N/A'}                                           ║
    ║     Rows: {acquisition.get('rows', 'N/A') if acquisition else 'N/A'}                                            ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  🔧 PREPROCESSING                                            ║
    ║     Status: {preprocessing.get('status', 'N/A') if preprocessing else 'N/A'}                                           ║
    ║     Output Rows: {preprocessing.get('output_rows', 'N/A') if preprocessing else 'N/A'}                                      ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  📋 SCHEMA VALIDATION                                        ║
    ║     Status: {schema.get('status', 'N/A') if schema else 'N/A'}                                           ║
    ║     Passed: {schema.get('validation_passed', 'N/A') if schema else 'N/A'}                                           ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  ⚖️  BIAS DETECTION                                          ║
    ║     Status: {bias.get('status', 'N/A') if bias else 'N/A'}                                           ║
    ║     Imbalance Ratio: {bias.get('imbalance_ratio', 'N/A') if bias else 'N/A'}                                  ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  🔍 ANOMALY DETECTION                                        ║
    ║     Status: {anomaly.get('status', 'N/A') if anomaly else 'N/A'}                                           ║
    ║     Anomalies Found: {anomaly.get('total_anomalies_found', 'N/A') if anomaly else 'N/A'}                                 ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  📦 DVC VERSIONING                                           ║
    ║     Status: {versioning.get('status', 'N/A') if versioning else 'N/A'}                                           ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                    ✅ PIPELINE COMPLETE!                      ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    
    print(summary)
    logger.info(summary)
    
    return {"status": "success", "message": "Pipeline completed successfully"}

# =============================================================================
# DAG DEFINITION
# =============================================================================

# Default arguments for all tasks
default_args = {
    "owner": DAG_CONFIG["default_args"]["owner"],
    "email": DAG_CONFIG["default_args"]["email"],
    "email_on_failure": DAG_CONFIG["default_args"]["email_on_failure"],
    "email_on_retry": DAG_CONFIG["default_args"]["email_on_retry"],
    "retries": DAG_CONFIG["default_args"]["retries"],
    "retry_delay": DAG_CONFIG["default_args"]["retry_delay"],
    "execution_timeout": DAG_CONFIG["default_args"]["execution_timeout"],
    "on_failure_callback": on_task_failure,
    "on_success_callback": on_task_success,
    "on_retry_callback": on_task_retry,
}

# Create the DAG
with DAG(
    dag_id=DAG_CONFIG["dag_id"],
    description=DAG_CONFIG["description"],
    schedule_interval=DAG_CONFIG["schedule_interval"],
    start_date=datetime(2026, 2, 1),
    catchup=DAG_CONFIG["catchup"],
    max_active_runs=DAG_CONFIG["max_active_runs"],
    tags=DAG_CONFIG["tags"],
    default_args=default_args,
) as dag:
    
    # =========================================================================
    # TASK DEFINITIONS
    # =========================================================================
    
    # Task 1: Data Acquisition
    task_data_acquisition = PythonOperator(
        task_id="data_acquisition",
        python_callable=run_data_acquisition,
        provide_context=True,
        doc_md="""
        ### Data Acquisition
        Loads raw data from source and performs initial validation.
        - Input: Raw CSV file
        - Output: Verified data ready for preprocessing
        """,
    )
    
    # Task 2: Data Preprocessing
    task_data_preprocessing = PythonOperator(
        task_id="data_preprocessing",
        python_callable=run_data_preprocessing,
        provide_context=True,
        doc_md="""
        ### Data Preprocessing
        Cleans and transforms raw data.
        - Handles missing values
        - Encodes categorical variables
        - Feature engineering
        """,
    )
    
    # Task 3a: Schema Validation (parallel)
    task_schema_validation = PythonOperator(
        task_id="schema_validation",
        python_callable=run_schema_validation,
        provide_context=True,
        doc_md="""
        ### Schema Validation
        Validates data structure and types.
        - Checks column names and types
        - Validates constraints
        - Runs in parallel with bias detection
        """,
    )
    
    # Task 3b: Bias Detection (parallel)
    task_bias_detection = PythonOperator(
        task_id="bias_detection",
        python_callable=run_bias_detection,
        provide_context=True,
        doc_md="""
        ### Bias Detection
        Identifies potential biases in the dataset.
        - Checks class imbalance
        - Analyzes feature distributions
        - Runs in parallel with schema validation
        """,
    )
    
    # Task 4: Anomaly Detection
    task_anomaly_detection = PythonOperator(
        task_id="anomaly_detection",
        python_callable=run_anomaly_detection,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Only runs if both parallel tasks succeed
        doc_md="""
        ### Anomaly Detection
        Identifies outliers and anomalies.
        - Uses Z-score method
        - IQR-based detection
        - Flags suspicious data points
        """,
    )
    
    # Task 5: DVC Versioning
    task_dvc_versioning = PythonOperator(
        task_id="dvc_versioning",
        python_callable=run_dvc_versioning,
        provide_context=True,
        doc_md="""
        ### DVC Versioning
        Versions processed data using DVC.
        - Tracks data changes
        - Creates version hash
        - Pushes to remote storage
        """,
    )
    
    # Task 6: Pipeline Complete
    task_pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=run_pipeline_complete,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,  # Runs regardless of upstream status
        doc_md="""
        ### Pipeline Complete
        Final summary task.
        - Collects all task results
        - Generates execution report
        - Logs pipeline status
        """,
    )
    
    # =========================================================================
    # TASK DEPENDENCIES (DAG Flow)
    # =========================================================================
    
    # Sequential: acquisition → preprocessing
    task_data_acquisition >> task_data_preprocessing
    
    # Parallel: preprocessing → [schema_validation, bias_detection]
    task_data_preprocessing >> [task_schema_validation, task_bias_detection]
    
    # Join: [schema_validation, bias_detection] → anomaly_detection
    [task_schema_validation, task_bias_detection] >> task_anomaly_detection
    
    # Sequential: anomaly_detection → dvc_versioning → pipeline_complete
    task_anomaly_detection >> task_dvc_versioning >> task_pipeline_complete