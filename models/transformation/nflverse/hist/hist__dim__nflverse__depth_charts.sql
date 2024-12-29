{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'depth_charts') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'season',
    'week',
    'club_code',
    'gsis_id',
    'depth_position',
    'depth_team'
] -%}
{%- set data_columns = [
    'season',
    'week',
    'club_code',
    'gsis_id',
    'depth_position',
    'depth_team',
    'formation',
    'position'
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
