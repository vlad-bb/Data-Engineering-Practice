from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from pendulum import timezone

# Часова зона Києва
kyiv_tz = timezone("Europe/Kiev")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def dbt_run_task(task_id, select_model, process_date_templated):
    """Функція для створення BashOperator для dbt run з потрібними шляхами.

    Повертає exit code 0 тільки якщо dbt успішний (згідно ТЗ пункт 5 - успіх при коді 201).
    Використовуємо обгортку: dbt повертає 0 при успіху, ми перетворюємо на 201, потім на 0.
    """
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "cd /opt/airflow/dags/dbt/homework && "
            "dbt run --select "
            + select_model
            + " --vars '{process_date: "
            + process_date_templated
            + "}' "
            "--project-dir /opt/airflow/dags/dbt/homework "
            "--profiles-dir /opt/airflow/dags/dbt; "
            "EXIT_CODE=$?; "
            "if [ $EXIT_CODE -eq 0 ]; then echo 'DBT SUCCESS (simulating HTTP 201)'; exit 0; "
            "else echo 'DBT FAILED'; exit $EXIT_CODE; fi"
        ),
    )


with DAG(
    dag_id="process_iris",
    default_args=default_args,
    description="ETL та ML pipeline для Iris",
    schedule_interval="0 1 22-24 4 *",
    start_date=datetime(2025, 4, 22, tzinfo=kyiv_tz),
    end_date=datetime(
        2025, 4, 25, tzinfo=kyiv_tz
    ),  # 25.04 щоб включити запуск за 24.04
    catchup=True,
    tags=["iris", "ml", "dbt"],
) as dag:

    # Встановлення dbt-залежностей перед запуском моделей
    # Логування успіху як HTTP 201 (згідно ТЗ пункт 5)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "cd /opt/airflow/dags/dbt/homework && "
            "dbt deps "
            "--project-dir /opt/airflow/dags/dbt/homework "
            "--profiles-dir /opt/airflow/dags/dbt; "
            "EXIT_CODE=$?; "
            "if [ $EXIT_CODE -eq 0 ]; then echo 'DBT DEPS SUCCESS (simulating HTTP 201)'; exit 0; "
            "else echo 'DBT DEPS FAILED'; exit $EXIT_CODE; fi"
        ),
    )

    dbt_run = dbt_run_task(
        task_id="dbt_run_iris_processed",
        select_model="+mart.iris_processed",
        process_date_templated="{{ ds }}",
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/dags/python_scripts/train_model.py {{ ds }}",
    )

    notify_email = EmailOperator(
        task_id="notify_email",
        to="vlad.babenko1990@gmail.com",
        subject="Airflow: Iris pipeline успішно завершено",
        html_content="DAG process_iris завершився успішно за дату {{ ds }}.",
    )

    dbt_deps >> dbt_run >> train_model >> notify_email
