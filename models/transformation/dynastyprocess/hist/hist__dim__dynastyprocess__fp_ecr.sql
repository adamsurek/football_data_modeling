{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('dynastyprocess', 'fp_ecr') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'scrape_date',
    'fp_page',
    'id'
] -%}
{%- set data_columns = [
    'scrape_date',
    'fp_page',
    'id',
    'page_type',
    'ecr_type',
    'ecr',
    'sd',
    'best',
    'worst',
    'player_owned_avg'
] -%}

{%- set pre_filter = """
    scrape_date::date > '2020-01-04'
""" -%}

{{ standard_scd_2_hist(
    this=this,
    source_table=source_table,
    is_incremental=is_incremental(),
    date_column=date_column,
    pk_columns=pk_columns,
    data_columns=data_columns,
    pre_filter=pre_filter
) }}
