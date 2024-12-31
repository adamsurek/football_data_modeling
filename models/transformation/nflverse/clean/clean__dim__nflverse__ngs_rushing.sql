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
    {{ clean_decimal('efficiency', 16) }} as rushing_efficiency,
    {{ clean_decimal('percent_attempts_gte_eight_defenders', 16) }} as rush_attempts_with_gte_eight_defenders_percentage,
    {{ clean_decimal('avg_time_to_los', 16) }} as avg_time_to_line_of_scrimmage,
    {{ clean_integer('rush_attempts') }} as rush_attempts,
    {{ clean_integer('rush_yards') }} as rush_yards,
    {{ clean_decimal('avg_rush_yards', 16) }} as avg_rush_yards,
    {{ clean_integer('rush_touchdowns') }} as rushing_touchdowns,
    {{ clean_decimal('expected_rush_yards', 16) }} as expected_rush_yards,
    {{ clean_decimal('rush_yards_over_expected', 16) }} as rush_yards_over_expected,
    {{ clean_decimal('rush_yards_over_expected_per_att', 16) }} as rush_yards_over_expected_per_attempt,
    {{ clean_decimal('rush_pct_over_expected', 16) }} as rushing_percentage_over_expected
from
    {{ ref('hist__dim__nflverse__ngs_rushing') }}