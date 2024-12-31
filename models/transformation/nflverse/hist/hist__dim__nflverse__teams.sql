{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'teams') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'season',
    'nfl_team_id'
] -%}
{%- set data_columns = [
    'season',
    'nfl_team_id',
    'team',
    'nfl',
    'espn',
    'pfr',
    'pff',
    'fo',
    '\"full\"',
    'location',
    'nickname',
    'sbr'
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
