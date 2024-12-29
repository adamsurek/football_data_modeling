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
    {{ clean_string('game_id') }} as nfl_game_id,
    {{ clean_string('pfr_player_id') }} as pfr_player_id,
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_integer('week') }} as nfl_week,
    {{ clean_string('game_type') }} as nfl_game_type,
    {{ clean_string('team') }} as nfl_team_id,
    {{ clean_string('opponent') }} as nfl_team_id_opponent,
    {{ clean_integer('offense_snaps') }} as total_offensive_snaps,
    {{ clean_decimal('offense_pct') }} as offensive_snap_percentage,
    {{ clean_integer('defense_snaps') }} as total_defensive_snaps,
    {{ clean_decimal('defense_pct') }} as defensive_snap_percentage,
    {{ clean_integer('st_snaps') }} as total_special_teams_snaps,
    {{ clean_decimal('st_pct') }} as special_teams_snap_percentage
from
    {{ ref('hist__dim__nflverse__snap_counts') }}
    