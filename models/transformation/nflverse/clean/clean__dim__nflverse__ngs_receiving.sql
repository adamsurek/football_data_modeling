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
    {{ clean_string('team_abbr') }} as nfl_team_code,
    {{ clean_string('player_gsis_id') }} as gsis_player_id,
    {{ clean_string('season_type') }} as nfl_game_type,
    {{ clean_decimal('avg_cushion', 16) }} as avg_cushion_from_defender,
    {{ clean_decimal('avg_separation', 16) }} as avg_separation_from_defender,
    {{ clean_decimal('avg_intended_air_yards', 16) }} as avg_intended_air_yards,
    {{ clean_decimal('percent_share_of_intended_air_yards', 16) }} as yard_share_percentage,
    {{ clean_integer('receptions') }} as receptions,
    {{ clean_integer('targets') }} as targets,
    {{ clean_decimal('catch_percentage', 16) }} as catch_percentage,
    {{ clean_decimal('yards', 0) }} as total_receiving_yards,
    {{ clean_integer('rec_touchdowns') }} as receiving_touchdowns,
    {{ clean_decimal('avg_yac', 16) }} as avg_yards_after_catch,
    {{ clean_decimal('avg_expected_yac', 16) }} as avg_expected_yards_after_catch,
    {{ clean_decimal('avg_yac_above_expectation', 16) }} as avg_yards_after_catch_above_expectation
from
    {{ ref('hist__dim__nflverse__ngs_receiving') }}