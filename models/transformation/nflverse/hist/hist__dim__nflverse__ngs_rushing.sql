{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'ngs_rushing') -%}
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
    'efficiency',
    'percent_attempts_gte_eight_defenders',
    'avg_time_to_los',
    'rush_attempts',
    'rush_yards',
    'avg_rush_yards',
    'rush_touchdowns',
    'expected_rush_yards',
    'rush_yards_over_expected',
    'rush_yards_over_expected_per_att',
    'rush_pct_over_expected'
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
