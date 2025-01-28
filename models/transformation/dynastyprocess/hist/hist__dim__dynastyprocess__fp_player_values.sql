{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('dynastyprocess', 'fp_player_values') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    '\"\"\"scrape_date\"\"\"',
    '\"\"\"fp_id\"\"\"'
] -%}
{%- set data_columns = [
    '\"\"\"scrape_date\"\"\"',
    '\"\"\"fp_id\"\"\"',
    '\"\"\"ecr_1qb\"\"\"',
    '\"\"\"ecr_pos\"\"\"',
    '\"\"\"value_1qb\"\"\"'
] -%}

{%- set pre_filter = """
    upper(\"\"\"pos\"\"\") != 'PICK'
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
