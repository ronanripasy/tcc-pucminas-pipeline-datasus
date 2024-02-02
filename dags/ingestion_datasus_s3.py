import os
import shutil
import datetime as dt
import pandas as pd
import pyarrow
from airflow import DAG, AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from custom_s3_to_redshift import CustomS3ToRedshiftOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import Variable

from pysus.online_data.SIH import download

DAG_ID = os.path.realpath(__file__).split("/")[-1].replace(".py","")
PATH_FILES = "/home/ronanripasy/airflow_pysus/local_transient_area/datasus"

# Connections
AWS_CONN_ID="AWS_connection"
REDSHIFT_CONN_ID="REDSHIFT_connection"
DATASUS_BUCKET_S3=Variable.get("datasus_s3")
REDSHIFT_TABLE_RAW=Variable.get("redshift_table_raw")

UFS = [
    'AC','AL','AP','AM',
    'BA','CE','DF','ES',
    'GO','MA','MT','MS',
    'MG','PA','PB','PR',
    'PE','PI','RJ','RN',
    'RS','RO','RR','SC',
    'SP','SE','TO'
]
#UFS = ['SP','RJ']

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="@monthly",
    start_date=dt.datetime(2023,5,1),
    catchup=True,
    tags=["datasus", "puc", "ingestion", "etl"]
)

def _download_datasus_sih_uf(uf, path_landing_area, **context):
    data_processamento = context["ds"]
    dt_processamento = dt.datetime.strptime(data_processamento, "%Y-%m-%d")
    ano_mes = dt_processamento.strftime("%Y-%m").replace("-0", "-")
    ano = int(ano_mes[0:4])
    mes = int(ano_mes[5:])

    download(data_dir=path_landing_area, years=ano, months=mes, states=uf, groups="RD")

    path_to_datasus = os.path.join(path_landing_area, f"RD{uf}{dt_processamento.strftime('%Y')[-2:]}{dt_processamento.strftime('%m')}.parquet")
    if not os.path.exists(path_to_datasus):
        raise AirflowException(f"Falha ao processar: {uf} em {data_processamento}.")

def _move_data_to_staging_with_datetime(uf, path_landing_area, path_staging_area, **context):
    data_processamento = context["ds"]
    dt_processamento = dt.datetime.strptime(data_processamento, "%Y-%m-%d")
    path_to_datasus = os.path.join(path_landing_area, f"RD{uf}{dt_processamento.strftime('%Y')[-2:]}{dt_processamento.strftime('%m')}.parquet")

    df = pd.read_parquet(path_to_datasus, engine='pyarrow')
    df["DOWNLOADED_AT"] = dt.datetime.now()

    if not os.path.exists(path_staging_area):
        os.mkdir(path_staging_area)
    df.to_parquet(f"{path_staging_area}/RD{uf}{dt_processamento.strftime('%Y')[-2:]}{dt_processamento.strftime('%m')}.parquet.gzip", compression="gzip")

def _delete_dir_processed_from_landing_area(uf, path_landing_area, **context):
    data_processamento = context["ds"]
    dt_processamento = dt.datetime.strptime(data_processamento, "%Y-%m-%d")
    path_landing_datasus_uf = os.path.join(path_landing_area, f"RD{uf}{dt_processamento.strftime('%Y')[-2:]}{dt_processamento.strftime('%m')}.parquet")

    if os.path.exists(path_landing_datasus_uf):
        shutil.rmtree(path_landing_datasus_uf)

def _moving_local_data_to_s3(uf, path_staging_area, **context):
    source_s3_bucket = DATASUS_BUCKET_S3
    source_s3 = S3Hook(AWS_CONN_ID)

    data_processamento = context["ds"]
    dt_processamento = dt.datetime.strptime(data_processamento, "%Y-%m-%d")
    file = f"RD{uf}{dt_processamento.strftime('%Y')[-2:]}{dt_processamento.strftime('%m')}.parquet.gzip"
    
    source_s3.load_file(filename=f"{path_staging_area}/{file}", key=f"{uf}/{file}", bucket_name=source_s3_bucket, replace=True)


for uf in UFS:
    
    PATH_LANDING_AREA = f'{PATH_FILES}/landing/{uf}'
    PATH_STAGING_AREA = f'{PATH_FILES}/staging/{uf}'

    inicio = EmptyOperator(
        task_id=f"inicio_{uf}",
        dag=dag
    )

    ingestion_datasus_sih_uf = PythonOperator(
        task_id=f"ingestion_datasus_sih_uf_{uf}",
        python_callable=_download_datasus_sih_uf,
        op_kwargs={
            "uf": uf, 
            "path_landing_area": PATH_LANDING_AREA
        },
        dag=dag
    )

    wait_file_datasus_uf = TimeDeltaSensor(
        task_id=f"wait_file_datasus_uf_{uf}",
        delta=dt.timedelta(seconds=30),
        dag=dag
    )

    move_data_to_staging_with_datetime = PythonOperator(
        task_id=f"move_data_to_staging_with_datetime_{uf}",
        python_callable=_move_data_to_staging_with_datetime,
        op_kwargs={
            "uf": uf, 
            "path_landing_area": PATH_LANDING_AREA,
            "path_staging_area": PATH_STAGING_AREA
        },
        dag=dag
    )

    delete_dir_processed_from_landing_area = PythonOperator(
        task_id=f"delete_dir_processed_from_landing_area_{uf}",
        python_callable=_delete_dir_processed_from_landing_area,
        op_kwargs={
            "uf": uf, 
            "path_landing_area": PATH_LANDING_AREA
        },
        dag=dag
    ) 

    transfer_local_to_s3_job = PythonOperator(
        task_id=f"transfer_local_to_s3_job_{uf}",
        python_callable=_moving_local_data_to_s3,
        op_kwargs={
            "uf": uf, 
            "path_staging_area": PATH_STAGING_AREA
        },
        dag=dag
    )

    wait_s3_datasus_uf = TimeDeltaSensor(
        task_id=f"wait_s3_datasus_uf_{uf}",
        delta=dt.timedelta(seconds=30),
        dag=dag
    )

    transfer_s3_to_redshift = CustomS3ToRedshiftOperator(
        task_id=f"transfer_s3_to_redshift_{uf}",
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_bucket=DATASUS_BUCKET_S3,
        aws_conn_id=AWS_CONN_ID,
        uf=uf,
        schema="sih_raw",
        table=REDSHIFT_TABLE_RAW,
        copy_options=["format as parquet"],
        dag=dag
    )

    fim = EmptyOperator(
        task_id=f"fim_{uf}",
        dag=dag
    )

    inicio >> ingestion_datasus_sih_uf >> wait_file_datasus_uf >> move_data_to_staging_with_datetime >> delete_dir_processed_from_landing_area >> transfer_local_to_s3_job >> wait_s3_datasus_uf >> transfer_s3_to_redshift >> fim