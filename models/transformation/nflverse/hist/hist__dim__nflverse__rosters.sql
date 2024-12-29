{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'rosters') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'gsis_it_id',
    'season',
    'week'
] -%}
{%- set data_columns = [
    'gsis_it_id',
    'season',
    'week',
    'game_type',
    'team',
    'position',
    'ngs_position',
    'depth_chart_position',
    'status',
    'status_description_abbr',
    'gsis_id',
    'espn_id',
    'sportradar_id',
    'yahoo_id',
    'rotowire_id',
    'pff_id',
    'pfr_id',
    'fantasy_data_id',
    'sleeper_id',
    'esb_id',
    'smart_id'
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
