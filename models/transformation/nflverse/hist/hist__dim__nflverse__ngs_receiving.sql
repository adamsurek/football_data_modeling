{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'ngs_receiving') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'season',
    'week',
    'team_abbr',
    'player_gsis_id'
] -%}
{%- set data_columns = [
    'season',
    'week',
    'team_abbr',
    'player_gsis_id',
    'season_type',
    'avg_cushion',
    'avg_separation',
    'avg_intended_air_yards',
    'percent_share_of_intended_air_yards',
    'receptions',
    'targets',
    'catch_percentage',
    'yards',
    'rec_touchdowns',
    'avg_yac',
    'avg_expected_yac',
    'avg_yac_above_expectation'
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
