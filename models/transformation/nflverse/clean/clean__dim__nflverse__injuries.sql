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
    {{ clean_string('team') }} as nfl_team_code,
    {{ clean_string('gsis_id') }} as gsis_player_id,
    {{ clean_timestamp('date_modified', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') }} as dts_report_modified,
    {{ clean_string('report_primary_injury') }} as game_injury_primary,
    {{ clean_string('report_secondary_injury') }} as game_injury_secondary,
    {{ clean_string('report_status') }} as game_injury_status,
    {{ clean_string('practice_primary_injury') }} as practice_injury_primary,
    {{ clean_string('practice_secondary_injury') }} as practice_injury_secondary,
    {{ clean_string('practice_status') }} as practice_injury_status
from
    {{ ref('hist__dim__nflverse__injuries') }}