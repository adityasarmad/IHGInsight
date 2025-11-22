import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException
import pandas as pd
from datetime import datetime, timezone
import os

# Configuration
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"
CSV_FILE_PATH = "/opt/airflow/data/ToProcess/telecom_custom_data.csv"
DAG_ID = "telecom_custom"
RAW_TABLE_NAME = "raw_telecom_data"
POSTGRES_CONN_ID = "POSTGRES_CONN_ID"
DBT_PROFILE = "elt_pipeline"


@dag(
    dag_id="telecom_churn_elt_hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 * * * *",  # Run every hour (configurable)
    catchup=False,
    tags=["elt", "dbt", "postgres", "hourly"],

)
def telecom_elt_pipeline():
    """
    Hourly ELT pipeline for Telecom Customer Churn dataset.
    Loads data to Staging, transforms it with dbt, and creates a Reporting Mart.
    """

    @task
    def extract_and_load_data():
        """
        Reads the CSV and loads it into the PostgreSQL staging schema (public.raw_telecom_data).
        """
        print(f"Reading CSV file from: {CSV_FILE_PATH}")
        if not os.path.exists(CSV_FILE_PATH):
            # raise FileNotFoundError(f"CSV file not found at: {CSV_FILE_PATH}")
            raise AirflowSkipException("File Not Found so.. skipping task.")


        df = pd.read_csv(CSV_FILE_PATH)
        # Clean column names for SQL compatibility
        df.columns = df.columns.str.replace(r'[^A-Za-z0-9]+', '', regex=True).str.strip()

       ## Fill the Null values
        df["Gender"] = df["Gender"].fillna("Unknown")
        df["Age"] = df["Age"].fillna(0).astype(int)
        df["Tenure"] = df["Tenure"].fillna(0).astype(int)
        df["MonthlyCharges"] = pd.to_numeric(df["MonthlyCharges"], errors="coerce").fillna(0.0)
        # TotalCharges sometimes an empty string; coerce to float or 0
        df["TotalCharges"] = pd.to_numeric(df["TotalCharges"].replace("", pd.NA), errors="coerce").fillna(0.0)
        df["ContractType"] = df["ContractType"].fillna("Unknown")
        df["InternetService"] =df["InternetService"].fillna("Unknown")
        df["Churn"] = df["Churn"].fillna("No")

        # create is female flag
        df["is_female"] = df["Gender"].apply(lambda x: 1 if str(x).strip().lower() == "female" else 0)
        df["is_senior"] = df["Age"].apply(lambda x: 1 if x >64 else 0)

        ## Add When was this processed
        df["Last_load_time"] = datetime.now(timezone.utc)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Ensure staging schema exists
        hook.run("CREATE SCHEMA IF NOT EXISTS staging_schema;")

        hook.run("CREATE SCHEMA IF NOT EXISTS reporting_schema;")

        # Insert data using pandas to_sql via the psycopg2 connection
        print(f"Loading {len(df)} rows into PostgreSQL staging_schema.{RAW_TABLE_NAME}...")

        # Get connection engine for pandas
        conn = hook.get_sqlalchemy_engine()
        df.to_sql(
            name=RAW_TABLE_NAME,
            con=conn,
            schema='staging_schema',
            if_exists='append',
            index=False
        )

        print("Data load complete.")
        os.rename(CSV_FILE_PATH,"/opt/airflow/data/Processed/telecom_custom_data_"+datetime.now().strftime('%Y_%m_%d-%H_%M_%S')+".csv")

    # Task to run dbt transformations
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"""
        # Change to the dbt project folder
        cd {DBT_PROJECT_DIR}

        # Ensure reporting schema exists before running dbt
        export PGPASSWORD=hginsight_password
        psql -h airflow-postgres -U hginsight -d hginsight -c "CREATE SCHEMA IF NOT EXISTS reporting_schema;"
        # Test before running dbt
        dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --profile {DBT_PROFILE}
        # Run the dbt project 
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --profile {DBT_PROFILE}

        """,
        trigger_rule="all_success",
    )

    # Task to generate flow stats
    @task
    def generate_flow_stats():

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # 1. Raw Row Count
        raw_count = hook.get_first(f"SELECT count(*) FROM staging_schema.{RAW_TABLE_NAME}")[0]
        # 2. Mart Row Count
        mart_count = hook.get_first('SELECT count(*) FROM reporting_schema.fact_customer_telcome')[0]
        # 3. Churn Rate
        churn_rate = hook.get_first('SELECT avg(churned_flag) FROM reporting_schema.fact_customer_telcome')[0]

        stats = {
            "raw_record_count": raw_count,
            "transformed_record_count": mart_count,
            "data_loss_percentage": f"{((raw_count - mart_count) / raw_count) * 100:.2f}%" if raw_count > 0 else 0,
            "calculated_churn_rate": f"{churn_rate * 100:.2f}%"
        }

        print("\n--- ELT Flow Statistics ---")
        for key, value in stats.items():
            print(f"| {key.ljust(30)}: {value}")
        print("---------------------------\n")
        return stats

    load_task = extract_and_load_data()
    load_task >> run_dbt_models >> generate_flow_stats()


telecom_pipeline = telecom_elt_pipeline()