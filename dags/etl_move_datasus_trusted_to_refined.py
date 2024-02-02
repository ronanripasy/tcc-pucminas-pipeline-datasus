import os
import datetime as dt
from airflow import DAG, AirflowException
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.models import Variable

DAG_ID = os.path.realpath(__file__).split("/")[-1].replace(".py","")
path = os.getcwd()
PATH_FILES = f"{path}/local_transient_area/datasus"

# Connections
AWS_CONN_ID="AWS_connection"
WORKGROUP_NAME=Variable.get("workgroup_name")
REDSHIFT_DB=Variable.get("redshift_db")

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    template_searchpath="/home/ronanripasy/airflow_pysus",
    tags=["datasus", "puc", "ingestion", "etl"]
)

inicio = EmptyOperator(
    task_id="inicio",
    dag=dag
)

move_data_from_trusted_to_refined = RedshiftDataOperator(
    task_id="move_data_from_trusted_to_refined",
    database=REDSHIFT_DB,
    workgroup_name=WORKGROUP_NAME,
    sql="/scripts/move_trusted_to_refined.sql",
    aws_conn_id=AWS_CONN_ID,
    wait_for_completion=True,
    return_sql_result=False,
    region="us-west-2",
    secret_arn="AcessoRedshiftServerless",
    dag=dag
)


fim = EmptyOperator(
    task_id="fim",
    dag=dag
)

inicio >> move_data_from_trusted_to_refined >> fim