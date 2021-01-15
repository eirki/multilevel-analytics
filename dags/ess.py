from pathlib import Path

from airflow import DAG
import pandas as pd
import numpy as np
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime as dt

from helpers import queries

default_args = {
    "owner": "ebs",
    "depends_on_past": False,
    "start_date": dt.datetime(2021, 1, 12),
    "retries": 0,
    "email_on_retry": False,
    "catchup_by_default": False,
}

col_subset = [
    "idno",
    "region",
    "regunit",
    "cntry",
    "nwspol",
    "netusoft",
    "netustm",
    "ppltrst",
    "pplfair",
    "pplhlp",
    "polintr",
    "psppsgva",
    "actrolga",
    "psppipla",
    "cptppola",
    "trstprl",
    "trstlgl",
    "trstplc",
    "trstplt",
    "trstprt",
    "trstep",
    "trstun",
]


def convert_to_csv():
    df: pd.DataFrame = pd.read_stata(
        "/opt/airflow/data/ESS9e03.dta", convert_categoricals=False
    )
    df = df[col_subset]
    df.to_csv("/tmp/csv/ess9.csv", index=False)


def check_data_quality():
    path = Path("/tmp/csv/ess9.csv")
    df = pd.read_csv(path)
    if df is None:
        raise Exception
    if not path.exists():
        raise IOError(f"Could not find csv file for data: {path.name}")
    df = pd.read_csv(path)
    columns = list(df.columns)
    if not columns == col_subset:
        raise ValueError(
            f"Column deviates from expected: {columns} vs expected {col_subset} "
        )

    column_types = df.dtypes.to_dict()
    expected_column_types = {
        "idno": np.dtype("int64"),
        "region": np.dtype("O"),
        "regunit": np.dtype("int64"),
        "cntry": np.dtype("O"),
        "nwspol": np.dtype("float64"),
        "netusoft": np.dtype("float64"),
        "netustm": np.dtype("float64"),
        "ppltrst": np.dtype("float64"),
        "pplfair": np.dtype("float64"),
        "pplhlp": np.dtype("float64"),
        "polintr": np.dtype("float64"),
        "psppsgva": np.dtype("float64"),
        "actrolga": np.dtype("float64"),
        "psppipla": np.dtype("float64"),
        "cptppola": np.dtype("float64"),
        "trstprl": np.dtype("float64"),
        "trstlgl": np.dtype("float64"),
        "trstplc": np.dtype("float64"),
        "trstplt": np.dtype("float64"),
        "trstprt": np.dtype("float64"),
        "trstep": np.dtype("float64"),
        "trstun": np.dtype("float64"),
    }
    if not column_types == expected_column_types:
        raise ValueError(
            f"Column types deviates from expected: {column_types} vs expected {expected_column_types} "
        )

    key_cols = ["cntry", "idno"]
    duplicated = df[df.duplicated(subset=key_cols, keep=False)]
    if not duplicated.empty:
        raise ValueError(f"Data contains duplicated key records: {duplicated}")
    for colname in key_cols:
        missing = df[df[colname].isna()]
        if not missing.empty:
            raise ValueError(f"Rows with no data on column {colname}: {missing.index}")


dag = DAG(
    "ess",
    default_args=default_args,
    description="Load data from .dta and save to database",
    schedule_interval=None,
    catchup=False,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
convert_data_task = PythonOperator(
    dag=dag,
    task_id="convert_data",
    python_callable=convert_to_csv,
)
quality_task = PythonOperator(
    dag=dag,
    task_id="quality_check",
    python_callable=check_data_quality,
)
create_table = PostgresOperator(
    postgres_conn_id="data_postgres",
    dag=dag,
    task_id="create_table",
    sql=queries.create_ess9_table,
)
stage_to_database = PostgresOperator(
    postgres_conn_id="data_postgres",
    dag=dag,
    task_id="stage_data",
    sql=queries.copy_from_s3.format(table="ESS9", filename="ess9"),
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> convert_data_task >> quality_task >> stage_to_database
start_operator >> create_table >> stage_to_database
stage_to_database >> end_operator
