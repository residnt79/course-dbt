{% snapshot int_sessions_snapshot %}

  {{
    config(
      target_schema='snapshots',
      unique_key='session_guid',

      strategy='timestamp',
      updated_at='created_at_timestamp_utc',
    )
  }}

  SELECT * FROM {{ ref('int_fact_sessions_agg') }}

{% endsnapshot %}