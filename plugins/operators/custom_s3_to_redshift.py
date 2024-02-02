from __future__ import annotations

from datetime import datetime

from typing import TYPE_CHECKING, Iterable, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block

if TYPE_CHECKING:
    from airflow.utils.context import Context


AVAILABLE_METHODS = ["APPEND", "REPLACE", "UPSERT"]


class CustomS3ToRedshiftOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToRedshiftOperator`

    :param schema: reference to a specific schema in redshift database
    :param table: reference to a specific table in redshift database
    :param s3_bucket: reference to a specific S3 bucket
    :param uf: UF Sendo processada no momento que a dag está em execução
    :param redshift_conn_id: reference to a specific redshift database OR a redshift data-api connection
    :param aws_conn_id: reference to a specific S3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
    :param column_list: list of column names to load
    :param copy_options: reference to a list of COPY options
    :param method: Action to be performed on execution. Available ``APPEND``, ``UPSERT`` and ``REPLACE``.
    :param upsert_keys: List of fields to use as key on upsert action
    :param redshift_data_api_kwargs: If using the Redshift Data API instead of the SQL-based connection,
        dict of arguments for the hook's ``execute_query`` method.
        Cannot include any of these kwargs: ``{'sql', 'parameters'}``
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "uf",
        "schema",
        "table",
        "column_list",
        "copy_options",
        "redshift_conn_id",
        "method",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#ed9b9a"

    def __init__(
        self,
        *,
        schema: str,
        table: str,
        s3_bucket: str,
        uf: str,
        redshift_conn_id: str = "redshift_default",
        aws_conn_id: str = "aws_default",
        verify: bool | str | None = None,
        column_list: list[str] | None = None,
        copy_options: list | None = None,
        autocommit: bool = False,
        method: str = "APPEND",
        upsert_keys: list[str] | None = None,
        redshift_data_api_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.uf = uf
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.column_list = column_list
        self.copy_options = copy_options or []
        self.autocommit = autocommit
        self.method = method
        self.upsert_keys = upsert_keys
        self.redshift_data_api_kwargs = redshift_data_api_kwargs or {}

        if self.redshift_data_api_kwargs:
            for arg in ["sql", "parameters"]:
                if arg in self.redshift_data_api_kwargs:
                    raise AirflowException(f"Cannot include param '{arg}' in Redshift Data API kwargs")

    def _build_copy_query(
        self, copy_destination: str, credentials_block: str, region_info: str, copy_options: str, s3_key_custom
    ) -> str:
        column_names = "(" + ", ".join(self.column_list) + ")" if self.column_list else ""
        return f"""
                    COPY {copy_destination} {column_names}
                    FROM 's3://{self.s3_bucket}/{s3_key_custom}'
                    credentials
                    '{credentials_block}'
                    {region_info}
                    {copy_options};
        """

    def execute(self, context: Context) -> None:
        if self.method not in AVAILABLE_METHODS:
            raise AirflowException(f"Method not found! Available methods: {AVAILABLE_METHODS}")

        redshift_hook: RedshiftDataHook | RedshiftSQLHook
        if self.redshift_data_api_kwargs:
            redshift_hook = RedshiftDataHook(aws_conn_id=self.redshift_conn_id)
        else:
            redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        conn = S3Hook.get_connection(conn_id=self.aws_conn_id)
        region_info = ""
        if conn.extra_dejson.get("region", False):
            region_info = f"region '{conn.extra_dejson['region']}'"
        if conn.extra_dejson.get("role_arn", False):
            credentials_block = f"aws_iam_role={conn.extra_dejson['role_arn']}"
        else:
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
            credentials = s3_hook.get_credentials()
            credentials_block = build_credentials_block(credentials)

        copy_options = "\n\t\t\t".join(self.copy_options)
        destination = f"{self.schema}.{self.table}"
        copy_destination = f"#{self.table}" if self.method == "UPSERT" else destination

        # Construindo o key_value dentro do bucket
        data_processamento = context["ds"]
        dt_processamento = datetime.strptime(data_processamento, "%Y-%m-%d")
        s3_key_custom = f"{self.uf}/RD{self.uf}{dt_processamento.strftime('%Y')[-2:]}{dt_processamento.strftime('%m')}.parquet.gzip"

        copy_statement = self._build_copy_query(
            copy_destination, credentials_block, region_info, copy_options, s3_key_custom
        )

        sql: str | Iterable[str]

        if self.method == "REPLACE":
            sql = ["BEGIN;", f"DELETE FROM {destination};", copy_statement, "COMMIT"]
        elif self.method == "UPSERT":
            if isinstance(redshift_hook, RedshiftDataHook):
                keys = self.upsert_keys or redshift_hook.get_table_primary_key(
                    table=self.table, schema=self.schema, **self.redshift_data_api_kwargs
                )
            else:
                keys = self.upsert_keys or redshift_hook.get_table_primary_key(self.table, self.schema)
            if not keys:
                raise AirflowException(
                    f"No primary key on {self.schema}.{self.table}. Please provide keys on 'upsert_keys'"
                )
            where_statement = " AND ".join([f"{self.table}.{k} = {copy_destination}.{k}" for k in keys])

            sql = [
                f"CREATE TABLE {copy_destination} (LIKE {destination} INCLUDING DEFAULTS);",
                copy_statement,
                "BEGIN;",
                f"DELETE FROM {destination} USING {copy_destination} WHERE {where_statement};",
                f"INSERT INTO {destination} SELECT * FROM {copy_destination};",
                "COMMIT",
            ]

        else:
            sql = copy_statement

        self.log.info("Executing COPY command...")
        if isinstance(redshift_hook, RedshiftDataHook):
            redshift_hook.execute_query(sql=sql, **self.redshift_data_api_kwargs)
        else:
            redshift_hook.run(sql, autocommit=self.autocommit)
        self.log.info("COPY command complete...")
