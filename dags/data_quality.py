from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pipeline.data_quality import check_dataset_quality


@dag(
    dag_id="data_quality",
    description="Проверяет размеченную витрину и сохраняет HTML-отчёт",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0},
    tags=["mlops", "data-quality"],
)
def data_quality():
    task(check_dataset_quality, task_id="check_dataset_quality")()


data_quality()