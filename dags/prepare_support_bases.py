import os
import datetime as dt
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

DAG_ID = os.path.realpath(__file__).split("/")[-1].replace(".py","")
PATH_FILES = "/home/ronanripasy/airflow_pysus/local_transient_area"

# Connections
AWS_CONN_ID="AWS_connection"
REDSHIFT_CONN_ID="REDSHIFT_connection"
DATASUS_BUCKET_S3=Variable.get("datasus_s3")
WORKGROUP_NAME=Variable.get("workgroup_name")
REDSHIFT_DB=Variable.get("redshift_db")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    template_searchpath="/home/ronanripasy/airflow_pysus",
    tags=["datasus", "puc", "ingestion", "etl"]
) as dag:
    def _moving_support_bases_to_s3():
        source_s3_bucket = DATASUS_BUCKET_S3
        source_s3 = S3Hook(AWS_CONN_ID)

        path_support_bases = f"{PATH_FILES}/support_bases"

        for file in os.listdir(path_support_bases):
            source_s3.load_file(filename=f"{path_support_bases}/{file}", key=f"support_bases/{file}", bucket_name=source_s3_bucket, replace=True)

    inicio = EmptyOperator(
        task_id="inicio"
    )

    transfer_support_bases_to_s3 = PythonOperator(
        task_id="transfer_support_bases_to_s3",
        python_callable=_moving_support_bases_to_s3
    )

    with TaskGroup(group_id="create_table") as tg_create_table:
        create_uf_table = RedshiftDataOperator(
            task_id="create_uf_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_uf.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_regiao_uf_table = RedshiftDataOperator(
            task_id="create_regiao_uf_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_regiao_uf.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_municipio_table = RedshiftDataOperator(
            task_id="create_municipio_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_municipio.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_leito_table = RedshiftDataOperator(
            task_id="create_leito_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_leito.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_procedimento_table = RedshiftDataOperator(
            task_id="create_procedimento_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_procedimento.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_cid_10_capitulo_table = RedshiftDataOperator(
            task_id="create_cid_10_capitulo_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_cid_10_capitulo.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_cid_10_categoria_table = RedshiftDataOperator(
            task_id="create_cid_10_categoria_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_cid_10_categoria.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_cid_10_subcategoria_table = RedshiftDataOperator(
            task_id="create_cid_10_subcategoria_table",
            database=REDSHIFT_DB,
            workgroup_name=WORKGROUP_NAME,
            sql="/scripts/create_table_cid_10_subcategoria.sql",
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            return_sql_result=False,
            region="us-west-2",
            secret_arn="AcessoRedshiftServerless"
        )

        create_uf_table >> create_regiao_uf_table >> create_municipio_table >> create_leito_table >> create_procedimento_table
        create_procedimento_table >> create_cid_10_capitulo_table >> create_cid_10_categoria_table >> create_cid_10_subcategoria_table 

    with TaskGroup(group_id="s3_to_redshift") as tg_s3_to_redshift:
        s3_uf_to_redshift = S3ToRedshiftOperator(
            task_id='s3_uf_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_uf',
            s3_key='support_bases/UF.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_regiao_uf_to_redshift = S3ToRedshiftOperator(
            task_id='s3_regiao_uf_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_regiao_uf',
            s3_key='support_bases/REGIAO_UF.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_municipio_to_redshift = S3ToRedshiftOperator(
            task_id='s3_municipio_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_municipio',
            s3_key='support_bases/MUNICIPIO.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_leito_to_redshift = S3ToRedshiftOperator(
            task_id='s3_leito_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_leito',
            s3_key='support_bases/LEITO.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_procedimento_to_redshift = S3ToRedshiftOperator(
            task_id='s3_procedimento_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_procedimento',
            s3_key='support_bases/PROCEDIMENTO.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_cid10_cap_to_redshift = S3ToRedshiftOperator(
            task_id='s3_cid10_cap_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_cid_10_capitulo',
            s3_key='support_bases/CID_10_CAPITULO.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_cid10_cat_to_redshift = S3ToRedshiftOperator(
            task_id='s3_cid10_cat_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_cid_10_categoria',
            s3_key='support_bases/CID_10_CATEGORIA.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_cid10_subcat_to_redshift = S3ToRedshiftOperator(
            task_id='s3_cid10_subcat_to_redshift',
            redshift_conn_id=REDSHIFT_CONN_ID,
            s3_bucket=DATASUS_BUCKET_S3,
            aws_conn_id=AWS_CONN_ID,
            schema='sih_refined',
            table='tb_cid_10_subcategoria',
            s3_key='support_bases/CID_10_SUBCATEGORIA.csv',
            copy_options=[
                "IGNOREHEADER 1",
                "DELIMITER AS ';'",
                "removequotes"
            ],
            method='REPLACE'
        )

        s3_uf_to_redshift >> s3_regiao_uf_to_redshift >> s3_municipio_to_redshift >> s3_leito_to_redshift
        s3_leito_to_redshift >> s3_procedimento_to_redshift >> s3_cid10_cap_to_redshift >> s3_cid10_cat_to_redshift >> s3_cid10_subcat_to_redshift

    fim = EmptyOperator(
        task_id="fim"
    )

    inicio >> transfer_support_bases_to_s3 >> tg_create_table >> tg_s3_to_redshift >> fim