from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

GLUE_JOB_NAME = "data-platform-process-user-profiles"
CRAWLER_NAME = "data-platform-silver-crawler"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_user_profiles_etl',
    default_args=default_args,
    description='Pipeline to process user profiles data from Bronze to Silver',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['medallion', 'user_profiles'],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id='run_user_profiles_pyspark_job',
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True
    )

    publish_to_silver = GlueCrawlerOperator(
        task_id='register_silver_table',
        config={'Name': CRAWLER_NAME},
        wait_for_completion=True
    )

    run_glue_job >> publish_to_silver
