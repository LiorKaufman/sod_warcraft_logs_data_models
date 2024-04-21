{{ config(materialized='table') }}

with source as (
      select * from {{ source('wcl', 'wcl_report') }}
),
renamed as (
    select
    filename as file_name,
    regexp_replace(filename, '^.*_data_(\d{8})_.*$', '\1')::varchar AS date_str,
    strptime(date_str, '%Y%m%d') AS created_date,  -- Use to_date with format
    data as json_payload,
    errors as json_request_errors,
    uploaded_at

    from source
)
select * from renamed
  