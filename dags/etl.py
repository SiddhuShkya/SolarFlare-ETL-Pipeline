from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from datetime import timedelta
import json

## Define the DAG
with DAG(
    dag_id="NASA_APOD_Postgres",
    start_date=pendulum.datetime(
        2025, 1, 1, tz="UTC"
    ),  # Best practice: Use a fixed past date
    schedule="@daily",
    catchup=False,
) as dag:
    ## Step 1 : Create the table if it doesn't exist
    @task
    def create_postgres_table():
        ## Initialize the Postgreshook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        ## SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_apod_data (
            id SERIAL_PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        ## Executr the table creation query
        postgres_hook.run(create_table_query)

    ## Step 2 : Extract the Nasa API data (APOD) - ASTRONOMY PICTURE OF THE DAY
    extract_nasa_apod_data = HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api",  ## Connection id defined in airflow for NASA API
        endpoint="planetary/apod",  ## NASA API Endpoint for APOD
        method="GET",
        params={
            "api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"
        },  ## Use the api key from the connection
        response_filter=lambda response: response.json(),  ## Convert response to json
        log_response=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    ## Step 3 : Transform the data (Pick the information that i need to save)
    @task
    def transform_apod_data(response):
        # select only some necessary fields from the api data
        apod_data = {
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""),
            "date": response.get("date", ""),
            "media_type": response.get("media_type", ""),
        }
        return apod_data

    ## Step 4 : Load the data into Postgres SQL
    @task
    def load_data_postgres(apod_data):
        ## Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        ## Define the SQL insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type) VALUES (%s, %s, %s, %s, %s);
        """
        ## Execute the SQL Query
        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data["title"],
                apod_data["explanation"],
                apod_data["url"],
                apod_data["date"],
                apod_data["media_type"],
            ),
        )

    ## Step 5 : Verify the data DBViewer

    ## Step 6 : Define the task dependencies

    ## Extract
    (
        create_postgres_table() >> extract_nasa_apod_data
    )  # Ensures the table is created before extraction
    api_response = extract_nasa_apod_data.output
    ## Transform
    transformed_data = transform_apod_data(api_response)
    ## Load
    load_data_postgres(transformed_data)
