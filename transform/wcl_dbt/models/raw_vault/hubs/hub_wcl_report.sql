{{ config(
    materialized = 'incremental'
) }}

{%- set source_model = "stg_wcl_reports" -%}
{%- set src_pk = "REPORT_ID_HK" -%}
{%- set src_nk = "report_id" -%}
{%- set src_ldts = "LOAD_DATETIME" -%}
{%- set src_source = "source" -%}

{{ automate_dv.hub(
    src_pk = src_pk,
    src_nk = src_nk,
    src_ldts = src_ldts,
    src_source = src_source,
    source_model = source_model
) }}
