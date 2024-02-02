import os
import datetime as dt
from airflow import DAG, AirflowException
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.models import Variable

DAG_ID = os.path.realpath(__file__).split("/")[-1].replace(".py","")
path = os.getcwd()
PATH_SCRIPTS = f"{path}/scripts"
PATH_FILES = f"{path}/local_transient_area"

# Connections
AWS_CONN_ID="AWS_connection"
REDSHIFT_CONN_ID="REDSHIFT_connection"
DATASUS_BUCKET_S3=Variable.get("datasus_s3")
REDSHIFT_DB=Variable.get("redshift_db")
WORKGROUP_NAME = "datasus-workgroup"

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    template_searchpath="/home/ronanripasy/airflow_pysus",
    tags=["datasus", "puc", "ingestion", "etl"]
)

def _create_staging_folders():

    UFS = [
        'AC','AL','AP','AM',
        'BA','CE','DF','ES',
        'GO','MA','MT','MS',
        'MG','PA','PB','PR',
        'PE','PI','RJ','RN',
        'RS','RO','RR','SC',
        'SP','SE','TO'
    ]

    for uf in UFS:
        path_staging_area = f"{PATH_FILES}/datasus/staging/{uf}"
        if not os.path.exists(path_staging_area):
            os.mkdir(path_staging_area)

inicio = EmptyOperator(
    task_id="inicio",
    dag=dag
)

create_staging_folders =PythonOperator(
    task_id="create_staging_folders",
    python_callable=_create_staging_folders,
    dag=dag
)

prepare_raw_tables = RedshiftDataOperator(
    task_id="prepare_raw_tables",
    database=REDSHIFT_DB,
    workgroup_name=WORKGROUP_NAME,
    sql="/scripts/create_table_raw.sql",
    aws_conn_id=AWS_CONN_ID,
    wait_for_completion=True,
    return_sql_result=False,
    region="us-west-2",
    secret_arn="AcessoRedshiftServerless",
    dag=dag
)

prepare_trusted_tables = RedshiftDataOperator(
    task_id="prepare_trusted_tables",
    database=REDSHIFT_DB,
    workgroup_name=WORKGROUP_NAME,
    sql="/scripts/create_table_trusted.sql",
    aws_conn_id=AWS_CONN_ID,
    wait_for_completion=True,
    return_sql_result=False,
    region="us-west-2",
    secret_arn="AcessoRedshiftServerless",
    dag=dag
)

prepare_refined_tables = RedshiftDataOperator(
    task_id="prepare_refined_tables",
    database=REDSHIFT_DB,
    workgroup_name=WORKGROUP_NAME,
    sql="/scripts/create_table_refined.sql",
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

inicio >> create_staging_folders >> prepare_raw_tables >> prepare_trusted_tables >> prepare_refined_tables >> fim