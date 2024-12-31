{{-
    config(
        materialized='table'
    )
-}}

with
    ngs_rushing as (
        select
            flag_tombstoned,
            dts_effective_from,
            nfl_season,
            nfl_week,
            nfl_team_code,
            gsis_player_id,
            nfl_game_type,
            rushing_efficiency,
            rush_attempts_with_gte_eight_defenders_percentage,
            avg_time_to_line_of_scrimmage,
            rush_attempts,
            rush_yards,
            avg_rush_yards,
            rushing_touchdowns,
            expected_rush_yards,
            rush_yards_over_expected,
            rush_yards_over_expected_per_attempt,
            rushing_percentage_over_expected
        from
            {{ ref('clean__dim__nflverse__ngs_rushing') }}
        where
            flag_latest
    ),
    standard_rushing_stats as (
        select
            flag_tombstoned,
            dts_effective_from,
            gsis_player_id,
            nfl_season,
            nfl_week,
            nfl_team_code,
            nfl_game_type,
            nfl_team_code_opponent,
            total_carries,
            total_rushing_yards,
            total_rushing_touchdowns,
            total_rushing_fumbles,
            total_rushing_fumbles_lost,
            total_rushing_first_downs,
            total_rushing_expected_points_added,
            total_rushing_2pt_conversions
        from
            {{  ref('clean__dim__nflverse__player_stats_offense') }}
        where
            flag_latest
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
    t2.total_carries,
    t2.total_rushing_yards,
    t2.total_rushing_touchdowns,
    t2.total_rushing_fumbles,
    t2.total_rushing_fumbles_lost,
    t2.total_rushing_first_downs,
    t2.total_rushing_expected_points_added,
    t2.total_rushing_2pt_conversions,
    t1.rushing_efficiency,
    t1.avg_rush_yards,
    t1.expected_rush_yards,
    t1.rush_yards_over_expected,
    t1.rush_yards_over_expected_per_attempt,
    t1.avg_time_to_line_of_scrimmage,
    t1.rush_attempts_with_gte_eight_defenders_percentage,
    t1.rushing_percentage_over_expected
from
    ngs_rushing t1
left join
    standard_rushing_stats t2
on
    t1.gsis_player_id = t2.gsis_player_id and
    t1.nfl_team_code = t2.nfl_team_code and
    t1.nfl_season = t2.nfl_season and
    t1.nfl_week = t2.nfl_week
