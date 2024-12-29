{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'player_stats_defense') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'player_id',
    'season',
    'week',
    'team'
] -%}
{%- set data_columns = [
    'player_id',
    'season',
    'week',
    'team',
    'season_type',
    'def_tackles',
    'def_tackles_solo',
    'def_tackles_with_assist',
    'def_tackle_assists',
    'def_tackles_for_loss',
    'def_tackles_for_loss_yards',
    'def_fumbles_forced',
    'def_sacks',
    'def_sack_yards',
    'def_qb_hits',
    'def_interceptions',
    'def_pass_defended',
    'def_tds',
    'def_fumbles',
    'def_fumble_recovery_own',
    'def_fumble_recovery_yards_own',
    'def_fumble_recovery_opp',
    'def_fumble_recovery_yards_opp',
    'def_safety',
    'def_penalty',
    'def_penalty_yards'
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
