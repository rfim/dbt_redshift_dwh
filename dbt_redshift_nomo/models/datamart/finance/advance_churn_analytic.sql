{{ config(
    materialized='table',
    schema='finance'
) }}

WITH transaction_activity AS (
    -- Count transactions per customer each month
    SELECT
        t.account_id,
        t.date, 
        COUNT(t.transaction_id) AS count_transaction,
        DATE_TRUNC('month', to_date(t.date, 'yyyy-mm-dd')) AS period
    FROM
        {{ ref('fact_transactions') }} t
    WHERE
        t.date IS NOT NULL
    GROUP BY
        t.account_id, period, t.date
),

moving_avg AS (
    -- Calculate the moving average of transactions over the last 3 months
    SELECT
        mv.period,
        mv.account_id,
        AVG(mv.count_transaction) OVER (PARTITION BY mv.account_id ORDER BY mv.period ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS moving_avg_transaction,
        da.account_balance,
        CASE
            WHEN da.account_balance >= 5000 THEN 'High'  -- High balance segment
            WHEN da.account_balance BETWEEN 1000 AND 4999 THEN 'Medium'  -- Medium balance segment
            ELSE 'Low'  -- Low balance segment
        END AS customer_segment
    FROM transaction_activity mv
    JOIN {{ ref('dim_account') }} da
        ON mv.account_id = da.account_id
),

churn_with_segment AS (
    -- Identify churned customers based on the moving average
    SELECT
        mv.period,
        mv.account_id,
        mv.moving_avg_transaction,
        CASE
            WHEN mv.moving_avg_transaction <= 0 THEN 'Churned'  -- Churned if no transactions in the last 3 months
            ELSE 'Active'
        END AS churn_status,
        mv.account_balance,
        mv.customer_segment
    FROM moving_avg mv
    WHERE mv.moving_avg_transaction <= 0
),

churn_rate AS (
    -- Calculate churned customers and average account balance by segment and period
    SELECT
        period,
        churn_status,
        customer_segment,
        COUNT(account_id) AS churned_customers,
        AVG(account_balance) AS avg_account_balance
    FROM churn_with_segment
    GROUP BY period, churn_status, customer_segment
),

active_customers AS (
    -- Get total active customers at the start of each period
    SELECT
        period,
        customer_segment,
        COUNT(account_id) AS total_active_customers
    FROM moving_avg
    WHERE moving_avg_transaction > 0
    GROUP BY period, customer_segment
)

-- Final calculation of churn rate
SELECT
    cr.period,
    cr.churn_status,
    cr.customer_segment,
    cr.churned_customers,
    cr.avg_account_balance,
    ac.total_active_customers,
    -- Calculate churn rate
    CASE 
        WHEN ac.total_active_customers > 0 THEN cr.churned_customers * 1.0 / ac.total_active_customers
        ELSE 0
    END AS churn_rate
FROM churn_rate cr
JOIN active_customers ac
    ON cr.period = ac.period AND cr.customer_segment = ac.customer_segment
ORDER BY cr.period, cr.customer_segment