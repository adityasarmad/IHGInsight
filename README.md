# IHG Insight Work 

## ELT using Airflow + dbt-core + Metabase 

### Assumptions for usage 

Have basic knowledge about 

-- Airflow 
-- Metabase 
-- dbt  
-- Docker ( docker compose ) 

### Prerequisite 

Have Docker, web browser and a modern laptop 

## This project runs a small ELT pipeline using Airflow, dbt and Metabase. 

-- Ingest Raw file from folder  
-- Load data into staging table of PostgreSQL 
-- Transform (clean + anonymize PII) and load into postreg 
-- In airflow we had scheduled to runs periodically (default once per hour)  
-- Visualize via Metabase at http://localhost:3000 ( we need to configure like create username and connection to postreg) 

## Setup 

-- Clone this repository and run the docket compose file as shown below 

`/Applications/Docker.app/Contents/Resources/bin/docker compose -f /Users/aditya/PycharmProjects/HGInsights/docker-compose.yml -p hginsights up -d ` 

-- After Setup login into airflow url http://localhost:8080/login/ login and password is default to admin 
-- Run the dag "telecom_churn_elt_hourly" -- this will load the data and does the transformations -- In last task log we can see this pipeline stats  

-- Configure Metabase 
  -- Open http://localhost:3000 
  -- On initial setup, create an admin user. 
  -- Add a new database connection: 
    --- Type: PostgreSQL 
    --- Display name : HGInsight_PostgreSQL_db 
    --- Host : airflow-postgres 
    --- Port : 5432 
    --- Database name : hginsight 
    --- Username : hginsight 
    --- Password : hginsight_password 
    --- Schemas : All 

### How the scheduler accepts a feed every hour 

-- Drop CSV files into ./data/ToProcess/. The ETL service will pick them up once the dag is started and once started by default the dag runs every hour. 

-- After processing, files are moved to ./data/Processed/ with a timestamp in file name. 

### Anonymization & defaults 

-- `customer_key` is hashed using SHA-256  
-- Missing numeric replaced with 0 (MonthlyCharges, Tenure, TotalCharges). 
-- Missing strings replaced with Unknown or default labels. 

## Note: -  

I have placed the passwords in the config files and in code as I don't want to complicate things 

I assume csv file name and structure is constant 

 
