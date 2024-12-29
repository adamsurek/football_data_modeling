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
    {{ clean_string('team_abbr') }} as nfl_team_id,
    {{ clean_string('player_gsis_id') }} as gsis_player_id,
    {{ clean_string('season_type') }} as nfl_game_type,
    {{ clean_decimal('avg_time_to_throw', 16) }} as avg_time_to_throw,
    {{ clean_decimal('avg_completed_air_yards', 16) }} as avg_completed_air_yards,
    {{ clean_decimal('avg_intended_air_yards', 16) }} as avg_intended_air_yards,
    {{ clean_decimal('avg_air_yards_differential', 16) }} as avg_air_yards_differential,
    {{ clean_decimal('aggressiveness', 16) }} as aggressive_pass_percentage,
    {{ clean_decimal('max_completed_air_distance', 16) }} as max_completed_air_distance,
    {{ clean_decimal('avg_air_yards_to_sticks', 16) }} as avg_air_yards_to_sticks,
    {{ clean_integer('attempts') }} as pass_attempts,
    {{ clean_integer('completions') }} as pass_completions,
    {{ clean_decimal('completion_percentage', 16) }} as completion_percentage,
    {{ clean_decimal('expected_completion_percentage', 16) }} as expected_completion_percentage,
    {{ clean_decimal('completion_percentage_above_expectation', 16) }} as completion_percentage_above_expectation,
    {{ clean_integer('pass_yards') }} as pass_yards,
    {{ clean_integer('pass_touchdowns') }} as pass_touchdowns,
    {{ clean_integer('interceptions') }} as interceptions,
    {{ clean_decimal('passer_rating', 16) }} as passer_rating,
    {{ clean_decimal('avg_air_distance', 16) }} as avg_air_distance,
    {{ clean_decimal('max_air_distance', 16) }} as max_air_distance
from
    {{ ref('hist__dim__nflverse__ngs_passing') }}