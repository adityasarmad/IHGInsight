export PGPASSWORD=hginsight_password
psql -h airflow-postgres -U hginsight  -c "CREATE USER airflow WITH PASSWORD 'airflow_password'"
psql -h airflow-postgres -U hginsight  -c "CREATE DATABASE airflow"
psql -h airflow-postgres -U hginsight  -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow"

