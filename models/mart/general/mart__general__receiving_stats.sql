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
            nfl_team_id,
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
    ),
    standard_receiving_stats as (
        select
            flag_tombstoned,
            dts_effective_from,
            gsis_player_id,
            nfl_season,
            nfl_week,
            nfl_team_id,
            nfl_game_type,
            nfl_team_id_opponent,
            total_receptions,
            total_targets,
            total_receiving_yards,
            total_receiving_touchdowns,
            total_receiving_fumbles,
            total_receiving_fumbles_lost,
            total_receiving_air_yards,
            total_receiving_yards_after_catch,
            total_receiving_first_downs,
            total_receiving_expected_points_added,
            total_receiving_2pt_conversions,
            receiving_air_yard_conversion_ratio,
            total_target_share,
            total_air_yard_share,
            weighted_opportunity_rating
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
    t1.nfl_team_id,
    t2.nfl_team_id_opponent,
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
    t1.nfl_team_id = t2.nfl_team_id and
    t1.nfl_season = t2.nfl_season and
    t1.nfl_week = t2.nfl_week
