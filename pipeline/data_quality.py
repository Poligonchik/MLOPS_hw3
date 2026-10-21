import html
import os
import re
from pathlib import Path
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from pipeline.db import LABELED_COLUMNS, get_conn

REPORTS_DIR = Path("/opt/airflow/reports/quality")

def check_dataset_quality():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    context = get_current_context()
    run_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", context["run_id"])
    report_path = REPORTS_DIR / f"dq_{run_id}.html"
    checks = []

    conn = get_conn()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn.cursor() as cur:
            cur.execute("""SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'labeled_transactions'
            """)
            columns = {row[0] for row in cur.fetchall()}
            missing = sorted(set(LABELED_COLUMNS) - columns)
            checks.append(("Обязательные колонки", not missing, f"Отсутствуют: {missing}"))

            if not missing:
                null_condition = " OR ".join(f"{column} IS NULL" for column in LABELED_COLUMNS)
                cur.execute(f"""
                    SELECT
                        count(*),
                        count(*) FILTER (WHERE {null_condition}),
                        count(*) FILTER (WHERE target IS NULL OR target NOT IN (0, 1)),
                        count(DISTINCT target) FILTER (WHERE target IN (0, 1)),
                        count(*) FILTER (
                            WHERE amount IS NULL OR amount <= 0
                            OR amount::text IN ('NaN', 'Infinity', '-Infinity')
                        ),
                        count(*) FILTER (
                            WHERE transaction_time IS NULL OR NOT isfinite(transaction_time)
                        )
                    FROM public.labeled_transactions
                    """)
                rows, nulls, invalid_target, classes, invalid_amount, invalid_time = cur.fetchone()
                checks.extend([
                    ("Не менее 100 строк", rows >= 100, f"Строк: {rows}"),
                    ("Нет NULL в обязательных полях", nulls == 0, f"Строк с NULL: {nulls}"),
                    ("Метки 0/1 и оба класса", invalid_target == 0 and classes == 2, f"Ошибок: {invalid_target}; классов: {classes}"),
                    ("Суммы положительные и конечные", invalid_amount == 0, f"Нарушений: {invalid_amount}"),
                    ("Корректное время транзакции", invalid_time == 0, f"Нарушений: {invalid_time}"),
                ])
            else:
                for name in ("Объём", "Пропуски", "Метки", "Суммы", "Время"):
                    checks.append((name, False, "Проверка невозможна: отсутствуют обязательные колонки"))
    finally:
        conn.close()

    passed = all(ok for _, ok, _ in checks)
    table_rows = "".join(
        f"<tr><td>{html.escape(name)}</td><td>{'PASS' if ok else 'FAIL'}</td><td>{html.escape(details)}</td></tr>"
        for name, ok, details in checks
    )
    document = f"""<!doctype html>
                    <html lang="ru">
                    <meta charset="utf-8">
                    <title>Data Quality</title>
                    <style>
                    body {{font-family: Arial; margin: 32px; color: #222}}
                    table {{border-collapse: collapse; width: 100%}}
                    td, th {{border: 1px solid #ccc; padding: 12px; text-align: left}}
                    th {{background: #eee}}
                    </style>
                    <h1>Проверка качества данных: {'PASS' if passed else 'FAIL'}</h1>
                    <p>Таблица: labeled_transactions. Проверяется вся витрина.</p>
                    <p>Запуск: {html.escape(context["run_id"])}</p>
                    <table>
                    <tr><th>Правило</th><th>Результат</th><th>Подробности</th></tr>
                    {table_rows}
                    </table>
                    </html>"""
    temporary = report_path.with_suffix(".tmp")
    temporary.write_text(document, encoding="utf-8")
    os.replace(temporary, report_path)

    if not passed:
        raise AirflowException(f"Data Quality не пройден. Отчёт: {report_path}")

    return str(report_path)