{{
    config(
        materialized='incremental',
        alias='stg_dim_channel',
        schema=var('silver_schema'),
        unique_key='channel_id',
        incremental_stragey='delete+insert'
    )
}}

SELECT
    channel_id,
    channel_name,
    getdate() as created_at
FROM
    {{ var('bronze_schema') }}.ext_channels