{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'ngs_passing') -%}
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
    'avg_time_to_throw',
    'avg_completed_air_yards',
    'avg_intended_air_yards',
    'avg_air_yards_differential',
    'aggressiveness',
    'max_completed_air_distance',
    'avg_air_yards_to_sticks',
    'attempts',
    'pass_yards',
    'pass_touchdowns',
    'interceptions',
    'passer_rating',
    'completions',
    'completion_percentage',
    'expected_completion_percentage',
    'completion_percentage_above_expectation',
    'avg_air_distance',
    'max_air_distance'
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
