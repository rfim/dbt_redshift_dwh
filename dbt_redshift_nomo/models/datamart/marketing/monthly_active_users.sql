{{
    config(
        alias='monthly_active_users',
        schema='marketing'
    )
}}

SELECT
    DATE_TRUNC('month', to_date(interaction_date, 'yyyy-mm-dd')) AS month,
    count(distinct customer_id) as monthly_active_users
FROM
    {{ ref('fact_customer_interactions') }}
WHERE EXTRACT(YEAR FROM to_date(interaction_date, 'yyyy-mm-dd')) = 2022
group by 1
order by 1