import json
import logging
from pathlib import Path
import pandas as pd
from airflow.exceptions import AirflowSkipException
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score

from pipeline.retrain.fit_catboost import prepare_features
from pipeline.settings import MODELS_DIR, SPLITS_DIR

logger = logging.getLogger(__name__)
REPORTS_DIR = Path("/opt/airflow/reports/comparison")


def calculate_metrics(model, features, target):
    frame = features.loc[:, model.feature_names_].copy()
    for index in model.get_cat_feature_indices():
        column = model.feature_names_[index]
        frame[column] = frame[column].astype(str)
    probability = model.predict_proba(frame)[:, 1]
    prediction = (probability > 0.5).astype(int)
    return {
        "roc_auc": float(roc_auc_score(target, probability)),
        "f1": float(f1_score(target, prediction, zero_division=0)),
    }


def check_model_quality(train_info):
    """Читает кандидата, активную модель и подготовленный проверочный месяц"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"comparison_{train_info['version']}.json"
    test = pd.read_parquet(SPLITS_DIR / "test_proc.parquet")
    features, target = prepare_features(test)

    report = {
        "candidate_version": train_info["version"],
        "test_month": train_info["test_month"],
        "test_rows": len(test),
        "rule": "ROC-AUC и F1 кандидата не ниже текущей модели",
    }

    if target.nunique() != 2:
        report.update(accepted=False, reason="В проверочном месяце нет двух классов")
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise AirflowSkipException(report["reason"])

    candidate = CatBoostClassifier()
    candidate.load_model(train_info["staging_model"])
    candidate_metrics = calculate_metrics(candidate, features, target)

    latest_path = MODELS_DIR / "latest.txt"
    current_version = latest_path.read_text(encoding="utf-8").strip() if latest_path.exists() else None
    current_metrics = None

    if current_version:
        current = CatBoostClassifier()
        current.load_model(str(MODELS_DIR / f"my_catboost_{current_version}.cbm"))
        current_metrics = calculate_metrics(current, features, target)

    accepted = current_metrics is None or all(
        candidate_metrics[name] >= current_metrics[name]
        for name in ("roc_auc", "f1")
    )

    report.update(
        current_version=current_version,
        candidate_metrics=candidate_metrics,
        current_metrics=current_metrics,
        accepted=accepted,
        reason="Проверка пройдена" if accepted else "Кандидат ухудшает метрики",
    )
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not accepted:
        raise AirflowSkipException("Модель отклонена")
    return {
        **train_info,
        "roc_auc": candidate_metrics["roc_auc"],
        "f1": candidate_metrics["f1"],
    }