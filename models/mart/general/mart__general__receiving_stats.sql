{{-
    config(
        materialized='table'
    )
-}}

with
    ngs_receiving as (
        select
            flag_tombstoned,
            dts_effective_from,
            nfl_season,
            nfl_week,
            nfl_team_code,
            gsis_player_id,
            nfl_game_type,
            avg_cushion_from_defender,
            avg_separation_from_defender,
            avg_intended_air_yards,
            yard_share_percentage,
            receptions,
            targets,
            catch_percentage,
            total_receiving_yards,
            receiving_touchdowns,
            avg_yards_after_catch,
            avg_expected_yards_after_catch,
            avg_yards_after_catch_above_expectation
        from
            {{ ref('clean__dim__nflverse__ngs_receiving') }}
        where
            flag_latest
            and nfl_week != 0
    ),
    standard_receiving_stats as (
        select
            t1.flag_tombstoned,
            t1.dts_effective_from,
            t1.gsis_player_id,
            t1.nfl_season,
            t2.nfl_game_week_ngs as nfl_week_ngs,
            t1.nfl_team_code,
            t1.nfl_game_type,
            t1.nfl_team_code_opponent,
            t1.total_receptions,
            t1.total_targets,
            t1.total_receiving_yards,
            t1.total_receiving_touchdowns,
            t1.total_receiving_fumbles,
            t1.total_receiving_fumbles_lost,
            t1.total_receiving_air_yards,
            t1.total_receiving_yards_after_catch,
            t1.total_receiving_first_downs,
            t1.total_receiving_expected_points_added,
            t1.total_receiving_2pt_conversions,
            t1.receiving_air_yard_conversion_ratio,
            t1.total_target_share,
            t1.total_air_yard_share,
            t1.weighted_opportunity_rating
        from
            {{  ref('clean__dim__nflverse__player_stats_offense') }} t1
        left join
            {{ ref('combine__dim__games_teams') }} t2
        on
            t1.nfl_season = t2.nfl_season and
            t1.nfl_week = t2.nfl_game_week and
            (t1.nfl_team_code = t2.nfl_team_code_home_current or t1.nfl_team_code = t2.nfl_team_code_away_current) and
            t2.flag_latest
        where
            t1.flag_latest
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
    t2.total_receptions,
    t2.total_targets,
    t1.catch_percentage,
    t1.total_receiving_yards,
    t2.total_receiving_air_yards,
    t2.total_receiving_yards_after_catch,
    t2.total_receiving_touchdowns,
    t2.total_receiving_first_downs,
    t2.total_receiving_expected_points_added,
    t2.total_receiving_2pt_conversions,
    t2.total_target_share,
    t2.total_air_yard_share,
    t1.yard_share_percentage,
    t2.total_receiving_fumbles,
    t2.total_receiving_fumbles_lost,
    t1.avg_cushion_from_defender,
    t1.avg_separation_from_defender,
    t1.avg_intended_air_yards,
    t1.avg_yards_after_catch,
    t1.avg_expected_yards_after_catch,
    t1.avg_yards_after_catch_above_expectation,
    t2.receiving_air_yard_conversion_ratio,
    t2.weighted_opportunity_rating
from
    ngs_receiving t1
left join
    standard_receiving_stats t2
on
    t1.gsis_player_id = t2.gsis_player_id and
    t1.nfl_season = t2.nfl_season and
    t1.nfl_week = t2.nfl_week_ngs
