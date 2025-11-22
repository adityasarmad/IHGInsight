
  create view "hginsight"."reporting_schema"."stg_telecome_data__dbt_tmp"
    
    
  as (
    -- dbt/models/staging/stg_telecom_data.sql

SELECT
        "CustomerID"::Text AS customer_id_raw,
        "Gender" AS gender,
        "Age"::INTEGER AS age,

        "Tenure"::INTEGER AS tenure_months,
        "InternetService" AS internet_service,
        "ContractType" AS contract_type,
        "MonthlyCharges"::NUMERIC AS monthly_charges,
        -- Impute missing TotalCharges with 0.0, typically null when tenure is 0
        COALESCE("TotalCharges"::NUMERIC, 0.0) AS total_charges,
        "TechSupport" as tech_support,
        "Churn" AS churn_status
    FROM
    staging_schema.raw_telecom_data
  );