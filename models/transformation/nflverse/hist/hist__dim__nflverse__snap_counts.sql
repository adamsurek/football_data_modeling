{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'snap_counts') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'game_id',
    'pfr_player_id'
] -%}
{%- set data_columns = [
    'game_id',
    'pfr_player_id',
    'season',
    'week',
    'game_type',
    'team',
    'opponent',
    'offense_snaps',
    'offense_pct',
    'defense_snaps',
    'defense_pct',
    'st_snaps',
    'st_pct',
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
