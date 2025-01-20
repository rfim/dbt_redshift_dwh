{{ config(
    materialized='table',
    schema='finance'
) }}

WITH transaction_data AS (
    SELECT
        t.account_id,
        tt.transaction_type_name,
        t.transaction_amount,
        DATE_TRUNC('month', to_date(t.date, 'yyyy-mm-dd')) AS transaction_month
    FROM
        {{ ref('fact_transactions') }} t
    JOIN
        {{ ref('dim_transaction_type') }} tt
        ON t.transaction_type_id = tt.transaction_type_id  -- Updated join column
    WHERE
        t.date IS NOT NULL
)

SELECT
    account_id,
    transaction_month,
    SUM(CASE WHEN transaction_type_name IN ('Deposit', 'Payment') THEN transaction_amount ELSE 0 END) AS total_credit,
    SUM(CASE WHEN transaction_type_name IN ('Withdrawal', 'Loan Payment') THEN transaction_amount ELSE 0 END) AS total_debit,
    (SUM(CASE WHEN transaction_type_name IN ('Deposit', 'Payment') THEN transaction_amount ELSE 0 END) - 
     SUM(CASE WHEN transaction_type_name IN ('Withdrawal', 'Loan Payment') THEN transaction_amount ELSE 0 END)) AS net_balance
FROM
    transaction_data
GROUP BY
    account_id, transaction_month
ORDER BY
    account_id, transaction_month