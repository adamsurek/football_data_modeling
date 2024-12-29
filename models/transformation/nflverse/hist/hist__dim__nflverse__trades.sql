{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'trades') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'trade_id',
    'season',
    'gave',
    'received',
    'pick_season',
    'pick_round',
    'pfr_id'
] -%}
{%- set data_columns = [
    'trade_id',
    'season',
    'gave',
    'received',
    'pick_season',
    'pick_round',
    'pfr_id',
    'trade_date',
    'conditional'
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
