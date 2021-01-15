import datetime as dt
from pathlib import Path
import operator
import typing as t

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.s3_to_redshift import s3ToRedshiftOperator
from operators.create_table import CreateTableOperator
import pandas as pd
import requests

from helpers import datasets, queries
from helpers.s3_move import upload_to_S3


def convert_response_to_df(data: dict) -> pd.DataFrame:
    """
    Convert the JSON response from Eurostat to a pandas DataFrame
    """
    s = pd.Series(data["value"])
    s.index = s.index.astype(int)
    # "value" uses str index for some reason {'0': 48702, '1': 48702, '2': 2021, '3': 2021}
    s.name = "value"
    option_lists = []
    for dimension in data["id"]:
        options = data["dimension"][dimension]["category"]["index"]
        options = sorted(options.items(), key=operator.itemgetter(1))
        options = [option for option, index in options]
        option_lists.append(options)
    # multiindex with cartesian product of dimension options
    index = pd.MultiIndex.from_product(option_lists, names=data["id"])
    # convert multiindex to dataframe and merge with series containing data.
    index = index.to_frame().reset_index(drop=True)
    # Merges on indices contained in request response
    df = pd.merge(index, s, left_index=True, right_index=True)
    df["downloaded_at"] = dt.datetime.now()
    return df


def path_for_dataset(dataset_code: str, indicator_code: str) -> Path:
    """
    Returns the temporary on-disk path for a stored Eurostat csv """
    filename = f"{dataset_code}_{indicator_code}"
    path = (Path("/tmp/csv/") / filename).with_suffix(".csv")
    return path


def download_data(dataset_code: str, indicator_code: str, indicator_name: str):
    """
    Accesses the Eurostat JSON API and downloads a dataset with the given dataset_code
    and with the gived indicator_name set to indicator_code
    """
    params: t.Dict[str, t.Union[int, str]] = {
        "precision": 2,
        indicator_name.lower(): indicator_code,
    }
    response = requests.get(
        f"http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/{dataset_code}",
        params=params,
    )
    response.raise_for_status()
    response_data = response.json()
    if response_data.get("error", {}).get("status") is not None:
        raise Exception(response_data)
    df = convert_response_to_df(response_data)
    path = path_for_dataset(dataset_code, indicator_code)
    df.to_csv(path, index=False)


def check_data_quality(dataset: dict, indicator_code: str):
    """
    Performs relevant checks on the Eurostat data. Raises Exception if anything is wrong.
    """
    dataset_code = dataset["dataset_code"]
    expected_columns = dataset["expected_columns"]
    expected_column_types = dataset["expected_column_types"]
    path = path_for_dataset(dataset_code, indicator_code)
    if not path.exists():
        raise IOError(f"Could not find csv file for data: {path.name}")
    df = pd.read_csv(path, parse_dates=["downloaded_at"])
    columns = list(df.columns)
    if not columns == expected_columns:
        raise ValueError(
            f"Column deviates from expected: {columns} vs expected {expected_columns} "
        )

    column_types = df.dtypes.to_dict()
    if not column_types == expected_column_types:
        raise ValueError(
            f"Column types deviates from expected: {column_types} vs expected {expected_column_types} "
        )

    key_cols = [colname for colname in df if colname not in {"value", "downloaded_at"}]
    duplicated = df[df.duplicated(subset=key_cols, keep=False)]
    if not duplicated.empty:
        raise ValueError(f"Data contains duplicated key records: {duplicated}")
    for colname in key_cols:
        missing = df[df[colname].isna()]
        if not missing.empty:
            raise ValueError(f"Rows with no data on column {colname}: {missing.index}")


default_args = {
    "owner": "ebs",
    "depends_on_past": False,
    "start_date": dt.datetime(2021, 1, 12),
    "retries": 3,
    "retry_delay": dt.timedelta(minutes=5),
    "email_on_retry": False,
    "catchup_by_default": False,
}


def make_dag(dataset: t.Dict):
    dataset_code = dataset["dataset_code"]
    indicator_name = dataset["indicator_name"]
    dag = DAG(
        dataset_code,
        default_args=default_args,
        description=f"Download from Eurostat and save to s3: {dataset['description']}",
        schedule_interval="@daily",
        catchup=False,
    )

    start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
    end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

    create_table = CreateTableOperator(
        postgres_conn_id="redshift",
        dag=dag,
        task_id="create_table",
        redshift_conn_id="redshift",
        query=dataset["create_table_query"],
    )

    start_operator >> create_table
    for indicator_code, label in dataset["indicator_values"]:
        download_task = PythonOperator(
            dag=dag,
            task_id=f"download_{indicator_code.lower()}",
            python_callable=download_data,
            op_kwargs={
                "dataset_code": dataset_code,
                "indicator_code": indicator_code,
                "indicator_name": indicator_name,
            },
        )

        quality_task = PythonOperator(
            dag=dag,
            task_id=f"quality_check_{indicator_code.lower()}",
            python_callable=check_data_quality,
            op_kwargs={
                "dataset": dataset,
                "indicator_code": indicator_code,
            },
        )

        load_task = PythonOperator(
            dag=dag,
            task_id=f"load_{indicator_code.lower()}",
            python_callable=upload_to_S3,
            op_kwargs={
                "from_path": path_for_dataset(dataset_code, indicator_code),
            },
        )

        filename = f"{dataset_code}_{indicator_code}"

        stage_to_database = s3ToRedshiftOperator(
            dag=dag,
            task_id=f"stage_{indicator_code.lower()}",
            aws_credentials="aws_credentials",
            redshift_conn_id="redshift",
            table=dataset_code,
            filename=filename,
            query=queries.copy_from_s3
        )

        start_operator >> download_task
        download_task >> quality_task
        quality_task >> load_task
        load_task >> stage_to_database
        create_table >> stage_to_database
        stage_to_database >> end_operator
    return dag


demo_r_pjanind3_dag = make_dag(datasets.demo_r_pjanind3)
demo_r_pjangrp3_dag = make_dag(datasets.demo_r_pjangrp3)
