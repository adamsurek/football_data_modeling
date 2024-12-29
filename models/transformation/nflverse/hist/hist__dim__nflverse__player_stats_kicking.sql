{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'player_stats_kicking') -%}
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
    'fg_made',
    'fg_att',
    'fg_missed',
    'fg_blocked',
    'fg_long',
    'fg_pct',
    'fg_made_0_19',
    'fg_made_20_29',
    'fg_made_30_39',
    'fg_made_40_49',
    'fg_made_50_59',
    'fg_made_60_',
    'fg_missed_0_19',
    'fg_missed_20_29',
    'fg_missed_30_39',
    'fg_missed_40_49',
    'fg_missed_50_59',
    'fg_missed_60_',
    'fg_made_distance',
    'fg_missed_distance',
    'fg_blocked_distance',
    'fg_made_list',
    'fg_missed_list',
    'fg_blocked_list',
    'pat_made',
    'pat_att',
    'pat_missed',
    'pat_blocked',
    'gwfg_att',
    'gwfg_made',
    'gwfg_missed',
    'gwfg_blocked'
] -%}

{%- set pre_filter = """
    not (player_id = '00-0004811' and season = '2000' and week = '11' and team = 'LV')
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
