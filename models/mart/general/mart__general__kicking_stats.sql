{{-
    config(
        materialized='table'
    )
-}}

with
    kicking_stats as (
        select
            flag_tombstoned,
            dts_effective_from,
            nfl_season,
            nfl_week,
            nfl_game_type,
            nfl_team_code,
            gsis_player_id,
            total_field_goals_made,
            total_field_goals_attempted,
            total_field_goals_missed,
            total_field_goals_blocked,
            longest_field_goal,
            field_goal_percentage,
            total_field_goals_made_0_19,
            total_field_goals_made_20_29,
            total_field_goals_made_30_39,
            total_field_goals_made_40_49,
            total_field_goals_made_50_59,
            total_field_goals_made_60_plus,
            total_field_goals_missed_0_19,
            total_field_goals_missed_20_29,
            total_field_goals_missed_30_39,
            total_field_goals_missed_40_49,
            total_field_goals_missed_50_59,
            total_field_goals_missed_60_plus,
            total_field_goals_made_distance,
            total_field_goals_missed_distance,
            total_field_goals_blocked_distance,
            list_field_goals_made,
            list_field_goals_missed,
            list_field_goals_blocked,
            total_extra_points_made,
            total_extra_points_attempted,
            total_extra_points_missed,
            total_extra_points_blocked,
            total_game_winning_field_goals_attempted,
            total_game_winning_field_goals_made,
            total_game_winning_field_goals_missed,
            total_game_winning_field_goals_blocked
        from
            {{ ref('clean__dim__nflverse__player_stats_kicking') }}
        where
            flag_latest
    )
select
    t1.flag_tombstoned,
    t1.dts_effective_from,
    t1.nfl_season,
    t1.nfl_week,
    t2.nflverse_game_id,
    t1.nfl_game_type,
    t1.nfl_team_code,
    iff(t2.nfl_team_code_home = t1.nfl_team_code, t2.nfl_team_code_away, t2.nfl_team_code_home ) as nfl_team_code_opponent,
    t1.gsis_player_id,
    t1.total_field_goals_made,
    t1.total_field_goals_attempted,
    t1.total_field_goals_missed,
    t1.total_field_goals_blocked,
    t1.longest_field_goal,
    t1.field_goal_percentage,
    t1.total_field_goals_made_0_19,
    t1.total_field_goals_made_20_29,
    t1.total_field_goals_made_30_39,
    t1.total_field_goals_made_40_49,
    t1.total_field_goals_made_50_59,
    t1.total_field_goals_made_60_plus,
    t1.total_field_goals_missed_0_19,
    t1.total_field_goals_missed_20_29,
    t1.total_field_goals_missed_30_39,
    t1.total_field_goals_missed_40_49,
    t1.total_field_goals_missed_50_59,
    t1.total_field_goals_missed_60_plus,
    t1.total_field_goals_made_distance,
    t1.total_field_goals_missed_distance,
    t1.total_field_goals_blocked_distance,
    t1.list_field_goals_made,
    t1.list_field_goals_missed,
    t1.list_field_goals_blocked,
    t1.total_extra_points_made,
    t1.total_extra_points_attempted,
    t1.total_extra_points_missed,
    t1.total_extra_points_blocked,
    t1.total_game_winning_field_goals_attempted,
    t1.total_game_winning_field_goals_made,
    t1.total_game_winning_field_goals_missed,
    t1.total_game_winning_field_goals_blocked
from
    kicking_stats t1
left join
    {{ ref('combine__dim__games_teams') }} t2
on
    t1.nfl_season = t2.nfl_season and
    t1.nfl_week = t2.nfl_game_week and
    (t1.nfl_team_code = t2.nfl_team_code_home_current or t1.nfl_team_code = t2.nfl_team_code_away_current) and
    t2.flag_latest
