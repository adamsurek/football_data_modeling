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
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_string('nfl_team_id') }} as nfl_team_id,
    {{ clean_string('team') }} as nfl_team_code,
    {{ clean_string('espn') }} as espn_team_code,
    {{ clean_string('pfr') }} as pfr_team_code,
    {{ clean_string('pff') }} as pff_team_code,
    {{ clean_string('fo') }} as fo_team_code,
    {{ clean_string('\"full\"') }} as name_full,
    {{ clean_string('nickname') }} as name_short,
    {{ clean_string('location') }} as location,
    {{ clean_string('sbr') }} as sbr_team_code
from
    {{ ref('hist__dim__nflverse__teams') }}