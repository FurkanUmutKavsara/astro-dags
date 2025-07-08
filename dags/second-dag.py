import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

@task
def extract():
    """
    Pushes the estimated population (in millions) of
    various cities into XCom for the ETL pipeline.
    Obviously in reality this would be fetching this
    data from some source, not hardcoded values.
    """
    sample_data = {"Tokyo": 3.7, "Jakarta": 3.3, "Delhi": 2.9}
    return json.dumps(sample_data)


@task
def transform(raw_data: str):
    """
    Loads the provided raw data from XCom and pushes
    the name of the largest city in the set to XCom.
    """
    data = json.loads(raw_data)

    largest_city = max(data, key=data.get)
    return largest_city


@task
def load(largest_city):
    """
    Prints the name of the largest city in
    the set as determined by the transform.
    """
    print(largest_city)


with DAG(
    dag_id="city_pop_etl_taskflow",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    extracted_data = extract()
    largest_city = transform(extracted_data)
    load(largest_city)