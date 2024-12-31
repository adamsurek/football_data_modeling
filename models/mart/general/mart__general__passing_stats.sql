{{-
    config(
        materialized='table'
    )
-}}

with
    ngs_passing as (
        select
            flag_tombstoned,
            dts_effective_from,
            nfl_season,
            nfl_week,
            nfl_team_code,
            gsis_player_id,
            nfl_game_type,
            avg_time_to_throw,
            avg_completed_air_yards,
            avg_intended_air_yards,
            avg_air_yards_differential,
            aggressive_pass_percentage,
            max_completed_air_distance,
            avg_air_yards_to_sticks,
            completion_percentage,
            expected_completion_percentage,
            completion_percentage_above_expectation,
            passer_rating,
            avg_air_distance,
            max_air_distance
        from
            {{ ref('clean__dim__nflverse__ngs_passing') }}
        where
            flag_latest
    ),
    standard_passing_stats as (
        select
            flag_tombstoned,
            dts_effective_from,
            gsis_player_id,
            nfl_season,
            nfl_week,
            nfl_team_code,
            nfl_game_type,
            nfl_team_code_opponent,
            total_passing_completions,
            total_passing_attempts,
            total_passing_yards,
            total_passing_touchdowns,
            total_interceptions,
            total_sacks,
            total_sack_yards,
            total_sack_fumbles,
            total_sack_fumbles_lost,
            total_passing_air_yards,
            total_passing_yards_after_catch,
            total_passing_first_downs,
            total_passing_expected_points_added,
            total_passing_2pt_conversions,
            passing_air_yard_conversion_ratio,
            adjusted_epa_plus_cpoe
        from
            {{ ref('clean__dim__nflverse__player_stats_offense') }}
        where
            flag_latest and
            total_passing_attempts > 0
    )
select
    t1.flag_tombstoned,
    t1.dts_effective_from,
    t1.nfl_season,
    t1.nfl_week,
    t1.nfl_game_type,
    t1.nfl_team_code,
    t2.nfl_team_code_opponent,
    t1.gsis_player_id,
    t1.passer_rating,
    t2.total_passing_completions,
    t2.total_passing_attempts,
    t1.completion_percentage,
    t1.expected_completion_percentage,
    t2.total_passing_yards,
    t2.total_passing_air_yards,
    t2.total_passing_yards_after_catch,
    t2.total_passing_touchdowns,
    t2.total_passing_first_downs,
    t2.total_interceptions,
    t2.total_sacks,
    t2.total_sack_yards,
    t2.total_sack_fumbles,
    t2.total_sack_fumbles_lost,
    t1.avg_time_to_throw,
    t1.avg_completed_air_yards,
    t1.avg_intended_air_yards,
    t1.avg_air_yards_differential,
    t1.aggressive_pass_percentage,
    t1.max_completed_air_distance,
    t1.avg_air_yards_to_sticks,
    t1.completion_percentage_above_expectation,
    t1.avg_air_distance,
    t1.max_air_distance,
    t2.total_passing_expected_points_added,
    t2.total_passing_2pt_conversions,
    t2.passing_air_yard_conversion_ratio,
    t2.adjusted_epa_plus_cpoe
from
    ngs_passing t1
left join
    standard_passing_stats t2
on
    t1.gsis_player_id = t2.gsis_player_id and
    t1.nfl_team_code = t2.nfl_team_code and
    t1.nfl_season = t2.nfl_season and
    t1.nfl_week = t2.nfl_week
