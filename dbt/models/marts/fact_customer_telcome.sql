-- dbt/models/marts/fact_customer_telcome.sql
{{ config(
    materialized='table',
) }}

SELECT
    -- PII Anonymization: Use SHA256 hashing for a non-reversible identifier
    encode(digest(customer_id_raw, 'sha256'), 'hex') AS customer_key,
    gender,
    age,
    tenure_months,
    contract_type,
    internet_service,
    monthly_charges,
    total_charges,
    CASE
        WHEN tech_support = 'Yes' THEN 1
        ELSE 0
    END AS tech_support_flag,

    CASE
        WHEN churn_status = 'Yes' THEN 1
        ELSE 0
    END AS churned_flag,

    -- Categorization: Group tenure into buckets for easier analysis
    CASE
        WHEN tenure_months < 12 THEN '0-1 Year'
        WHEN tenure_months BETWEEN 12 AND 36 THEN '1-3 Years'
        WHEN tenure_months > 36 THEN '3+ Years'
        ELSE 'Unknown'
    END AS tenure_bucket

FROM

    {{ ref('stg_telecome_data') }}