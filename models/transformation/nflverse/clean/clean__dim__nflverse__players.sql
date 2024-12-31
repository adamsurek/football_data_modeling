{{-
	config(
		materialized='view'
	)
-}}

select
    flag_latest,
    flag_tombstoned,
    dts_effective_from,
    dts_effective_to,
    {{ clean_string('gsis_id') }} as gsis_player_id,
    {{ clean_string('smart_id') }} as smart_player_id,
    {{ clean_string('esb_id') }} as esb_plauer_id,
    {{ clean_string('first_name') }} as name_first,
    {{ clean_string('last_name') }} as name_last,
    {{ clean_string('football_name') }} as name_football,
    {{ clean_string('short_name') }} as name_short,
    {{ clean_string('status') }} as status_code,
    {{ clean_string('status_description_abbr') }} as status_description_code,
    {{ clean_string('status_short_description') }} as status_description,
    {{ clean_string('position_group') }} as team_position_group,
    {{ clean_string('position') }} as team_position,
    {{ clean_integer('jersey_number') }} as jersey_number,
    {{ clean_integer('height') }} as height_inches,
    {{ clean_integer('weight') }} as weight_pounds,
    {{ clean_date('birth_date') }} as date_birth,
    {{ clean_string('years_of_experience') }} as years_of_experience,
    {{ clean_string('entry_year') }} as entry_year,
    {{ clean_string('rookie_year') }} as rookie_year,
    {{ clean_string('current_team_id') }} as current_team_id,
    {{ clean_string('team_abbr') }} as nfl_team_code,
    {{ clean_string('team_seq') }} as team_sequence,
    {{ clean_string('draft_club') }} as nfl_team_code_drafted,
    {{ clean_integer('draft_round') }} as draft_round,
    {{ clean_integer('draft_number') }} as draft_number,
    {{ clean_string('college_name') }} as college_name,
    {{ clean_string('college_conference') }} as college_conference
from
    {{ ref('hist__dim__nflverse__players') }}
    