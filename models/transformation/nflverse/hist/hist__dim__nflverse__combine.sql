{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'combine') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'season',
    'player_name',
    'pos'
] -%}
{%- set data_columns = [
    'season',
    'player_name',
    'pos',
    'pfr_id',
    'cfb_id',
    'ht',
    'wt',
    'forty',
    'bench',
    'vertical',
    'broad_jump',
    'cone',
    'shuttle'
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
