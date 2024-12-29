{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'player_ids') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'mfl_id'
] -%}
{%- set data_columns = [
    'mfl_id',
    'fantasypros_id',
    'gsis_id',
    'pff_id',
    'nfl_id',
    'espn_id',
    'yahoo_id',
    'pfr_id',
    'cfbref_id',
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
