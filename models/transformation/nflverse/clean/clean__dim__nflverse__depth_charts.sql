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
    {{ clean_integer('week') }} as nfl_week,
    {{ clean_string('club_code') }} as nfl_team_code,
    {{ clean_string('gsis_id') }} as gsis_player_id,
    {{ clean_string('formation') }} as player_formation,
    {{ clean_string('position') }} as player_primary_position,
    {{ clean_string('depth_position') }} as depth_chart_player_position,
    {{ clean_integer('depth_team') }} as depth_chart_string_position
from
    {{ ref('hist__dim__nflverse__depth_charts') }}