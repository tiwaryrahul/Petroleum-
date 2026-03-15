{% snapshot vendor_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='vendor_id',
        strategy='check',
        check_cols=['vendor_name', 'service_type', 'location']
    )
}}

SELECT *
FROM {{ source('bronze','vendor_master') }}

{% endsnapshot %}
