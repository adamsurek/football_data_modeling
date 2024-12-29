{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'injuries') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'season',
    'team',
    'week',
    'gsis_id',
    'date_modified'
] -%}
{%- set data_columns = [
    'season',
    'team',
    'week',
    'gsis_id',
    'date_modified',
    'report_primary_injury',
    'report_secondary_injury',
    'report_status',
    'practice_primary_injury',
    'practice_secondary_injury',
    'practice_status',
] -%}

{%- set pre_filter = """""" -%}

{{ standard_scd_2_hist(
    this=this,
    source_table=source_table,
    is_incremental=is_incremental(),
    date_column=date_column,
    pk_columns=pk_columns,
    data_columns=data_columns,
    pre_filter=pre_filter
) }}