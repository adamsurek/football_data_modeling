{{-
	config(
		materialized='incremental',
		unique_key='sk'
	)
-}}

{%- set source_table = source('nflverse', 'players') -%}
{%- set date_column = 'dts_loaded' -%}
{%- set pk_columns = [
    'gsis_id',
    'smart_id',
    'esb_id'
] -%}
{%- set data_columns = [
    'gsis_id',
    'smart_id',
    'esb_id',
    'first_name',
    'last_name',
    'football_name',
    'short_name',
    'status',
    'status_description_abbr',
    'status_short_description',
    'position_group',
    'position',
    'jersey_number',
    'height',
    'weight',
    'birth_date',
    'years_of_experience',
    'entry_year',
    'rookie_year',
    'current_team_id',
    'team_abbr',
    'team_seq',
    'draft_club',
    'draft_round',
    'draft_number',
    'college_name',
    'college_conference',
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
