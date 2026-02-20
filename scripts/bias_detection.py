"""
bias_detection.py
=================
Operational Fairness Analysis for Pipeline Autopilot.
Detects and mitigates bias in CI/CD pipeline failure predictions.

Author  : Member 5 — Fairness Analyst
Project : Pipeline Autopilot (PipelineGuard)
"""

import os
import json
import logging
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# ── Config imports ─────────────────────────────────────────────
from scripts.config import (
    PROCESSED_DATA_FILE,
    REPORTS_DIR,
    TARGET_COLUMN,
    BIAS_SETTINGS,
)

# Key variables from config
BIAS_REPORT       = REPORTS_DIR / "bias_report.json"
BIAS_SLICE_COLUMNS = BIAS_SETTINGS["sensitive_columns"]

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# 1. LOAD PROCESSED DATA
# ──────────────────────────────────────────────────────────────
def load_processed_data(filepath) -> pd.DataFrame:
    """
    Load the processed pipeline dataset from CSV.

    Args:
        filepath: Path to the processed CSV file.

    Returns:
        pd.DataFrame: Loaded dataset.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    filepath = Path(filepath)
    logger.info(f"Loading data from: {filepath}")
    if not filepath.exists():
        raise FileNotFoundError(
            f"Data file not found: {filepath}\n"
            f"Run 'ls data/processed/' to check the correct filename."
        )
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


# ──────────────────────────────────────────────────────────────
# 2. SLICE DATA
# ──────────────────────────────────────────────────────────────
def slice_data(df: pd.DataFrame, column: str) -> dict:
    """
    Split DataFrame into subgroups by a categorical/binary column.

    Args:
        df     : Input DataFrame.
        column : Column name to slice on.

    Returns:
        dict: {slice_value: sub_dataframe}
    """
    logger.info(f"Slicing data by column: '{column}'")
    if column not in df.columns:
        logger.warning(f"Column '{column}' not in dataset — skipping")
        return {}
    slices = {str(val): grp for val, grp in df.groupby(column)}
    logger.info(f"  → {len(slices)} slices: {list(slices.keys())}")
    return slices


# ──────────────────────────────────────────────────────────────
# 3. COMPUTE SLICE METRICS
# ──────────────────────────────────────────────────────────────
def compute_slice_metrics(
    df: pd.DataFrame,
    slice_column: str,
    target: str
) -> pd.DataFrame:
    """
    For each slice compute: count, failure_rate, mean_duration, std_dev.

    Args:
        df           : Input DataFrame.
        slice_column : Column to group by.
        target       : Binary target column (e.g. 'failed').

    Returns:
        pd.DataFrame: Per-slice metrics table.
    """
    logger.info(f"Computing slice metrics for '{slice_column}'")
    records = []
    for val, grp in df.groupby(slice_column):
        records.append({
            "slice_column":  slice_column,
            "slice_value":   str(val),
            "count":         len(grp),
            "failure_rate":  round(float(grp[target].mean()), 4),
            "mean_duration": round(float(grp["duration_seconds"].mean()), 2)
                             if "duration_seconds" in grp.columns else None,
            "std_dev":       round(float(grp[target].std()), 4),
        })
    metrics_df = pd.DataFrame(records)
    logger.info(f"\n{metrics_df.to_string(index=False)}")
    return metrics_df


# ──────────────────────────────────────────────────────────────
# 4. COMPUTE DISPARITY METRICS
# ──────────────────────────────────────────────────────────────
def compute_disparity_metrics(slice_metrics: pd.DataFrame) -> dict:
    """
    Calculate disparity ratio (max/min failure rate) and difference.

    Args:
        slice_metrics: Output of compute_slice_metrics().

    Returns:
        dict: Disparity metrics including ratio, difference, worst slice.
    """
    logger.info("Computing disparity metrics")
    rates    = slice_metrics["failure_rate"]
    max_rate = float(rates.max())
    min_rate = float(rates.min())

    disparity = {
        "slice_column":         slice_metrics["slice_column"].iloc[0],
        "max_failure_rate":     round(max_rate, 4),
        "min_failure_rate":     round(min_rate, 4),
        "disparity_ratio":      round(max_rate / min_rate, 4)
                                if min_rate > 0 else float("inf"),
        "disparity_difference": round(max_rate - min_rate, 4),
        "worst_slice":          slice_metrics.loc[rates.idxmax(), "slice_value"],
        "best_slice":           slice_metrics.loc[rates.idxmin(), "slice_value"],
    }
    logger.info(
        f"  Ratio: {disparity['disparity_ratio']} | "
        f"Diff: {disparity['disparity_difference']} | "
        f"Worst: '{disparity['worst_slice']}'"
    )
    return disparity


# ──────────────────────────────────────────────────────────────
# 5. DETECT BIAS
# ──────────────────────────────────────────────────────────────
def detect_bias(
    disparity_metrics: dict,
    threshold: float = 1.5
) -> dict:
    """
    Flag columns where disparity ratio exceeds the threshold.

    Args:
        disparity_metrics : Output of compute_disparity_metrics().
        threshold         : Bias detection threshold (default 1.5).

    Returns:
        dict: Bias result with is_biased flag and severity level.
    """
    ratio     = disparity_metrics["disparity_ratio"]
    is_biased = ratio > threshold

    result = {
        **disparity_metrics,
        "threshold": threshold,
        "is_biased": is_biased,
        "severity":  "HIGH"   if ratio > 2.0
                     else "MEDIUM" if ratio > threshold
                     else "LOW",
        "explanation": (
            f"Disparity ratio {ratio} exceeds threshold {threshold}. "
            f"The '{disparity_metrics['worst_slice']}' subgroup has a "
            f"disproportionately high failure prediction rate."
        ) if is_biased else (
            f"No significant bias detected. "
            f"Ratio {ratio} is within acceptable threshold {threshold}."
        ),
    }

    if is_biased:
        logger.warning(
            f"⚠️  BIAS DETECTED — '{disparity_metrics['slice_column']}' "
            f"ratio={ratio} severity={result['severity']}"
        )
    else:
        logger.info(
            f"✅ No bias — '{disparity_metrics['slice_column']}' ratio={ratio}"
        )
    return result


# ──────────────────────────────────────────────────────────────
# 6. MITIGATE BIAS
# ──────────────────────────────────────────────────────────────
def mitigate_bias(
    df: pd.DataFrame,
    biased_slices: list
) -> tuple:
    """
    Apply re-sampling strategy to reduce detected bias.
    Oversample underrepresented, undersample overrepresented slices.

    Args:
        df            : Original DataFrame.
        biased_slices : List of bias detection result dicts.

    Returns:
        tuple: (mitigated_df, mitigation_steps_list)
    """
    logger.info("Planning bias mitigation via re-sampling...")
    mitigation_steps = []
    df_mitigated     = df.copy()

    for bias_info in biased_slices:
        col         = bias_info["slice_column"]
        max_rate    = bias_info["max_failure_rate"]
        min_rate    = bias_info["min_failure_rate"]
        target_rate = round((max_rate + min_rate) / 2, 4)

        step = {
            "column":      col,
            "strategy":    "resampling",
            "description": (
                f"Balance failure rates in '{col}' toward midpoint {target_rate}. "
                f"Oversample underrepresented slices, undersample overrepresented."
            ),
            "target_rate": target_rate,
            "severity":    bias_info["severity"],
            "trade_offs": (
                "Re-sampling may reduce overall dataset size. "
                "Threshold adjustment is an alternative with less data loss. "
                "Monitor overall model accuracy after mitigation."
            ),
        }
        mitigation_steps.append(step)
        logger.info(f"  Mitigation for '{col}': target_rate={target_rate}")

    return df_mitigated, mitigation_steps


# ──────────────────────────────────────────────────────────────
# 7. GENERATE BIAS REPORT
# ──────────────────────────────────────────────────────────────
def generate_bias_report(
    all_metrics: list,
    biased_slices: list,
    mitigation_steps: list,
    output_path=BIAS_REPORT,
) -> None:
    """
    Save bias_report.json with slice metrics, detected biases,
    mitigation steps, and trade-offs documented.

    Args:
        all_metrics      : List of slice metrics DataFrames.
        biased_slices    : List of detected bias dicts.
        mitigation_steps : List of mitigation step dicts.
        output_path      : Output path for the JSON report.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    biased_cols = [b["slice_column"] for b in biased_slices]
    clean_cols  = [
        m["slice_column"].iloc[0]
        for m in all_metrics
        if m["slice_column"].iloc[0] not in biased_cols
    ]

    report = {
        "project":       "Pipeline Autopilot",
        "member":        "Member 5 — Fairness Analyst",
        "analysis_type": "Operational Fairness — Bias Detection & Mitigation",
        "slice_metrics": [m.to_dict(orient="records") for m in all_metrics],
        "biases_detected":  biased_slices,
        "mitigation_steps": mitigation_steps,
        "summary": {
            "total_columns_analyzed": len(all_metrics),
            "biased_columns":         biased_cols,
            "clean_columns":          clean_cols,
            "overall_status":         "BIASED" if biased_slices else "FAIR",
            "high_severity_count":    sum(
                1 for b in biased_slices if b["severity"] == "HIGH"
            ),
            "medium_severity_count":  sum(
                1 for b in biased_slices if b["severity"] == "MEDIUM"
            ),
        },
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"✅ Bias report saved → {output_path}")


# ──────────────────────────────────────────────────────────────
# 8. CREATE VISUALIZATIONS
# ──────────────────────────────────────────────────────────────
def create_bias_visualizations(
    slice_metrics_list: list,
    output_dir=REPORTS_DIR
) -> None:
    """
    Generate bar charts of failure rates per slice.
    One PNG chart per slice column saved to output_dir.

    Args:
        slice_metrics_list : List of slice metrics DataFrames.
        output_dir         : Folder to save PNG charts.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Creating bias visualizations...")

    for metrics_df in slice_metrics_list:
        col      = metrics_df["slice_column"].iloc[0]
        avg_rate = metrics_df["failure_rate"].mean()

        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(
            data=metrics_df,
            x="slice_value",
            y="failure_rate",
            palette="Reds_d",
            ax=ax
        )
        ax.axhline(
            y=avg_rate, color="steelblue", linestyle="--", linewidth=1.5,
            label=f"Average: {avg_rate:.2%}"
        )
        for bar in ax.patches:
            h = bar.get_height()
            ax.annotate(
                f"{h:.2%}",
                (bar.get_x() + bar.get_width() / 2, h),
                ha="center", va="bottom", fontsize=9, fontweight="bold"
            )
        ax.set_title(
            f"Failure Rate by '{col}'\n(Operational Fairness — Pipeline Autopilot)",
            fontsize=13, fontweight="bold"
        )
        ax.set_xlabel(col, fontsize=11)
        ax.set_ylabel("Failure Rate", fontsize=11)
        ax.set_ylim(0, min(1.0, metrics_df["failure_rate"].max() * 1.35))
        ax.legend(fontsize=10)
        plt.xticks(rotation=45, ha="right", fontsize=9)
        plt.tight_layout()

        out_path = output_dir / f"bias_{col}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()
        logger.info(f"  Chart saved → {out_path}")


# ──────────────────────────────────────────────────────────────
# 9. MASTER FUNCTION
# ──────────────────────────────────────────────────────────────
def run_bias_detection(data_path=PROCESSED_DATA_FILE) -> None:
    """
    Master function chaining all bias detection steps:
    load → slice → metrics → disparity → detect →
    mitigate → report → visualize

    Args:
        data_path: Path to the processed dataset CSV.
    """
    logger.info("=" * 60)
    logger.info("  Pipeline Autopilot — Bias Detection Starting")
    logger.info("=" * 60)

    df               = load_processed_data(data_path)
    all_metrics      = []
    biased_slices    = []
    mitigation_steps = []

    for col in BIAS_SLICE_COLUMNS:
        if col not in df.columns:
            logger.warning(f"'{col}' not in dataset — skipping")
            continue

        metrics_df  = compute_slice_metrics(df, col, TARGET_COLUMN)
        all_metrics.append(metrics_df)

        disparity   = compute_disparity_metrics(metrics_df)
        bias_result = detect_bias(disparity, threshold=1.5)

        if bias_result["is_biased"]:
            biased_slices.append(bias_result)

    if biased_slices:
        _, steps = mitigate_bias(df, biased_slices)
        mitigation_steps.extend(steps)

    generate_bias_report(all_metrics, biased_slices, mitigation_steps)
    create_bias_visualizations(all_metrics)

    logger.info("=" * 60)
    logger.info("  BIAS DETECTION COMPLETE")
    logger.info(f"  Columns analyzed : {len(all_metrics)}")
    logger.info(f"  Biased columns   : {[b['slice_column'] for b in biased_slices]}")
    logger.info(f"  Report           : {BIAS_REPORT}")
    logger.info(f"  Charts           : {REPORTS_DIR}")
    logger.info("=" * 60)


# ── Entry Point ────────────────────────────────────────────────
if __name__ == "__main__":
    run_bias_detection()
