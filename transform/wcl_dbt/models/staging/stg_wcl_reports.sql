

{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: 
  wcl: "wcl_reports"
derived_columns:
  SOURCE: "!warcraft_logs_api"
  LOAD_DATETIME: "uploaded_at"
  EFFECTIVE_FROM: "file_created_at"
  START_DATE: "file_created_at"
  END_DATE: "TO_DATE('9999-12-31','YYYY-MM-DD')"
  dv_object: "json_col"
  report_id: "json_col->'data'->'reportData'->'report'->'code'"
  report_object: "json_col->'data'->'reportData'->'report'"
hashed_columns:
  REPORT_ID_HK: "report_id"
  WCL_REPORT_HASHDIFF:
    is_hashdiff: true
    columns:
      - "report_id"
      - "report_object"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}

