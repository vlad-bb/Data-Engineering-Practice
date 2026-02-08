from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


GLUE_JOB_NAME = "data-platform-process-sales"
CRAWLER_NAME = "data-platform-silver-crawler"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_sales_etl',
    default_args=default_args,
    description='Pipeline to process sales data from Bronze to Silver',
    schedule_interval=None, 
    start_date=days_ago(1),
    tags=['medallion', 'sales'],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id='run_sales_pyspark_job',
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True
    )

    publish_to_silver = GlueCrawlerOperator(
        task_id='register_silver_table',
        config={'Name': CRAWLER_NAME},
        wait_for_completion=True
    )

    run_glue_job >> publish_to_silver
