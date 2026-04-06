"""
model_training.py
-----------------
Trains the MLP (Neural Network) pipeline failure prediction model.

Architecture: 17 → 128 → 64 → 32 → 1 (ReLU, Adam optimizer)

Author  : Member 3 (Data Scientist)
Project : Pipeline Autopilot — CI/CD Failure Prediction System
Date    : April 2026
"""

import json
import logging
import warnings
from pathlib import Path
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from sklearn.neural_network  import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing   import StandardScaler
from sklearn.metrics         import (
    roc_auc_score, f1_score, precision_score,
    recall_score, accuracy_score, average_precision_score,
    classification_report
)

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent.parent
DATA_PATH  = BASE_DIR / "scripts" / "final_dataset_processed.csv"
MODELS_DIR = BASE_DIR / "models" / "trained"
MODEL_PATH = MODELS_DIR / "best_model.joblib"
SCALER_PATH = MODELS_DIR / "scaler.joblib"

for d in [MODELS_DIR, BASE_DIR / "models" / "registry", BASE_DIR / "models" / "sensitivity"]:
    try:
        d.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_COL  = "failed"
RANDOM_SEED = 42

DROP_COLS = [
    "run_id",                 # identifier, not a feature
    "trigger_time",           # already extracted into day_of_week, hour, is_weekend
    "failure_type",           # post-run metadata — not available at prediction time
    "error_message",          # post-run metadata — not available at prediction time
    "failure_type_encoded",   # encoded version of failure_type — same leakage
    "error_message_encoded",  # encoded version of error_message — same leakage
    "pipeline_name",          # high cardinality identifier
    "repo",                   # high cardinality identifier
]

# ---------------------------------------------------------------------------
# 1. Load Data
# ---------------------------------------------------------------------------
def load_data(path=DATA_PATH):
    logger.info(f"Loading data from: {path}")
    if not Path(path).exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    df = pd.read_csv(path)
    logger.info(f"Dataset shape: {df.shape}")
    if TARGET_COL not in df.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found.")
    cols_to_drop = [c for c in DROP_COLS if c in df.columns]
    df = df.drop(columns=cols_to_drop)
    bool_cols = df.select_dtypes(include="bool").columns.tolist()
    df[bool_cols] = df[bool_cols].astype(int)
    obj_cols = df.select_dtypes(include="object").columns.tolist()
    if obj_cols:
        df = df.drop(columns=obj_cols)
    df = df.fillna(df.median(numeric_only=True))
    X = df.drop(columns=[TARGET_COL])
    y = df[TARGET_COL].astype(int)
    logger.info(f"Features: {X.shape[1]}  Samples: {len(y)}  Failure rate: {y.mean():.4f}")
    return X, y

# ---------------------------------------------------------------------------
# 2. Split Data
# ---------------------------------------------------------------------------
def split_data(X, y):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.15, stratify=y, random_state=RANDOM_SEED
    )
    logger.info(f"Split -> Train: {len(y_train)} | Test: {len(y_test)}")
    return X_train, X_test, y_train, y_test

# ---------------------------------------------------------------------------
# 3. Scale Features
# ---------------------------------------------------------------------------
def scale_features(X_train, X_test):
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)
    joblib.dump(scaler, SCALER_PATH)
    logger.info(f"Scaler saved to: {SCALER_PATH}")
    return X_train_scaled, X_test_scaled, scaler

# ---------------------------------------------------------------------------
# 4. Train MLP
# ---------------------------------------------------------------------------
def train_mlp(X_train, y_train) -> MLPClassifier:
    logger.info("Training MLP Neural Network...")
    logger.info("Architecture: input → 128 → 64 → 32 → 1")
    model = MLPClassifier(
        hidden_layer_sizes=(128, 64, 32),
        activation="relu",
        solver="adam",
        alpha=0.001,
        batch_size=256,
        learning_rate="adaptive",
        learning_rate_init=0.001,
        max_iter=100,
        random_state=RANDOM_SEED,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=10,
        verbose=False,
    )
    model.fit(X_train, y_train)
    logger.info(f"MLP training complete. Iterations: {model.n_iter_}")
    return model

# ---------------------------------------------------------------------------
# 5. Evaluate
# ---------------------------------------------------------------------------
def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    metrics = {
        "model"     : "MLP Neural Network",
        "accuracy"  : round(float(accuracy_score(y_test, y_pred)), 4),
        "precision" : round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
        "recall"    : round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
        "f1"        : round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        "auc_roc"   : round(float(roc_auc_score(y_test, y_prob)), 4),
        "auc_pr"    : round(float(average_precision_score(y_test, y_prob)), 4),
    }
    logger.info("── Test Set Results ──")
    for k, v in metrics.items():
        if k != "model":
            logger.info(f"  {k}: {v}")
    logger.info("\n" + classification_report(y_test, y_pred, target_names=["pass", "fail"]))
    return metrics

# ---------------------------------------------------------------------------
# 6. Save Model & Metadata
# ---------------------------------------------------------------------------
def save_model(model, metrics):
    # Backup existing model
    if MODEL_PATH.exists():
        prev_path = MODELS_DIR / "previous_model.joblib"
        joblib.dump(joblib.load(MODEL_PATH), prev_path)
        logger.info(f"Previous model backed up to: {prev_path}")

    joblib.dump(model, MODEL_PATH)
    logger.info(f"Model saved to: {MODEL_PATH}")

    metadata = {
        "model_name"  : "MLP Neural Network",
        "architecture": "input → 128 → 64 → 32 → 1",
        "trained_at"  : datetime.now().isoformat(),
        "test_metrics": metrics,
        "model_path"  : str(MODEL_PATH),
        "scaler_path" : str(SCALER_PATH),
    }
    meta_path = MODELS_DIR / "model_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Metadata saved to: {meta_path}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Pipeline Autopilot — MLP Model Training")
    logger.info("=" * 60)

    X, y             = load_data()
    X_train, X_test, y_train, y_test = split_data(X, y)
    X_train_s, X_test_s, _ = scale_features(X_train, X_test)
    model            = train_mlp(X_train_s, y_train)
    metrics          = evaluate_model(model, X_test_s, y_test)
    save_model(model, metrics)

    logger.info("=" * 60)
    logger.info("Training complete.")
    logger.info(f"  AUC-ROC : {metrics['auc_roc']}")
    logger.info(f"  F1      : {metrics['f1']}")
    logger.info(f"  Saved to: {MODEL_PATH}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
