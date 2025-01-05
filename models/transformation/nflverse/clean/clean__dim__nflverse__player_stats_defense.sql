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
    {{ clean_string('player_id') }} as gsis_player_id,
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_integer('week') }} as nfl_week,
    {{ clean_string('team') }} as nfl_team_code,
    {{ clean_string('season_type') }} as nfl_game_type,
    {{ clean_integer('def_tackles') }} as total_tackles,
    {{ clean_integer('def_tackles_solo') }} as total_solo_tackles,
    {{ clean_integer('def_tackles_with_assist') }} as total_tackles_with_assist,
    {{ clean_integer('def_tackle_assists') }} as total_tackle_assists,
    {{ clean_integer('def_tackles_for_loss') }} as total_tackles_for_loss,
    {{ clean_integer('def_tackles_for_loss_yards') }} as total_tackle_for_loss_yards,
    {{ clean_integer('def_fumbles_forced') }} as total_fumbles_forced,
    {{ clean_decimal('def_sacks', 1) }} as total_sacks,
    {{ clean_decimal('def_sack_yards', 1) }} as total_sack_yards,
    {{ clean_integer('def_qb_hits') }} as total_qb_hits,
    {{ clean_integer('def_interceptions') }} as total_interceptions,
    {{ clean_integer('def_pass_defended') }} as total_pass_defended,
    {{ clean_integer('def_tds') }} as total_touchdowns,
    {{ clean_integer('def_fumbles') }} as total_fumbles,
    {{ clean_integer('def_fumble_recovery_own') }} as total_own_fumble_recoveries,
    {{ clean_integer('def_fumble_recovery_yards_own') }} as total_own_fumble_recovery_yards,
    {{ clean_integer('def_fumble_recovery_opp') }} as total_opponent_fumble_recoveries,
    {{ clean_integer('def_fumble_recovery_yards_opp') }} as total_opponent_fumble_recovery_yards,
    {{ clean_integer('def_safety') }} as total_forced_safeties,
    {{ clean_integer('def_penalty') }} as total_penalties,
    {{ clean_integer('def_penalty_yards') }} as total_penalty_yards
from
    {{ ref('hist__dim__nflverse__player_stats_defense') }}