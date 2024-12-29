{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'player_stats') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'player_id',
    'season',
    'week',
    'recent_team'
] -%}
{%- set data_columns = [
    'player_id',
    'season',
    'week',
    'recent_team',
    'season_type',
    'opponent_team',
    'completions',
    'attempts',
    'passing_yards',
    'passing_tds',
    'interceptions',
    'sacks',
    'sack_yards',
    'sack_fumbles',
    'sack_fumbles_lost',
    'passing_air_yards',
    'passing_yards_after_catch',
    'passing_first_downs',
    'passing_epa',
    'passing_2pt_conversions',
    'pacr',
    'dakota',
    'carries',
    'rushing_yards',
    'rushing_tds',
    'rushing_fumbles',
    'rushing_fumbles_lost',
    'rushing_first_downs',
    'rushing_epa',
    'rushing_2pt_conversions',
    'receptions',
    'targets',
    'receiving_yards',
    'receiving_tds',
    'receiving_fumbles',
    'receiving_fumbles_lost',
    'receiving_air_yards',
    'receiving_yards_after_catch',
    'receiving_first_downs',
    'receiving_epa',
    'receiving_2pt_conversions',
    'racr',
    'target_share',
    'air_yards_share',
    'wopr',
    'special_teams_tds',
    'fantasy_points',
    'fantasy_points_ppr'
] -%}

{%- set pre_filter = """
    not (player_id = '00-0026498' and season = '2010' and week = '8' and completions = '0')
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
