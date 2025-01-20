{{ config(
    materialized='view',
    schema='finance'
) }}

WITH transaction_by_time AS (
    SELECT
        t.transaction_id,
        t.account_id,
        tt.transaction_type_name,
        t.transaction_amount,
        EXTRACT(HOUR FROM to_date(t.date, 'yyyy-mm-dd HH24:MI:SS')) AS transaction_hour
    FROM
        {{ ref('fact_transactions') }} t
    JOIN
        {{ ref('dim_transaction_type') }} tt
        ON t.transaction_type_id = tt.transaction_type_id
    WHERE
        t.date IS NOT NULL
)

SELECT
    transaction_hour,
    COUNT(t.transaction_id) AS total_transactions,
    SUM(t.transaction_amount) AS total_transaction_value
FROM
    transaction_by_time t
GROUP BY
    transaction_hour
ORDER BY
    transaction_hour
