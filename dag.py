import pendulum
from datetime import timedelta, datetime
from airflow import DAG
from allecomposer.bigquery_data_sensor import BigQueryDataSensor
from dataengine.composer.operators import AnalyticsBigQueryOperator


ds = "{{ ds }}"
local_tz = pendulum.timezone("Europe/Warsaw")


default_args = {
    'owner': '<MSosinska>',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 15, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG(
    'stackoverflow_posts',
    default_args=default_args,
    schedule_interval="0 6 1 * *",
    concurrency=3,
    max_active_runs=2,
    )

stack_posts = BigQueryDataSensor(
    task_id='stack_posts',
    project_id='bigquery-public-data.',
    dataset_id ='stackoverflow',
    table_id ='posts_questions',
    where_clause="date(creation_date) = '{}'".format(ds),
    dag=dag)

stack_data = AnalyticsBigQueryOperator(
    task_id = 'stack_data',
    dag = dag,
    sql = 'de_allegro_112023.sql',
    destination_dataset_table = 'msosinska.de_allegro.stackoverflow_post_questions',
    write_disposition='WRITE_APPEND',
)

stack_posts >> stack_data