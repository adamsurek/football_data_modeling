{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'games') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'game_id'
] -%}
{%- set data_columns = [
    'game_id',
    'season',
    'game_type',
    'week',
    'gameday',
    'away_team',
    'home_team',
    'away_score',
    'home_score',
    'location',
    'total',
    'overtime',
    'away_rest',
    'home_rest',
    'away_moneyline',
    'home_moneyline',
    'spread_line',
    'away_spread_odds',
    'home_spread_odds',
    'total_line',
    'under_odds',
    'over_odds',
    'div_game',
    'roof',
    'surface',
    'temp',
    'wind',
    'stadium_id'
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
