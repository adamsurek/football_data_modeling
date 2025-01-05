{{-
    config(
        materialized='table'
    )
-}}

with
    defensive_stats as (
        select
            flag_tombstoned,
            dts_effective_to,
            gsis_player_id,
            nfl_season,
            nfl_week,
            nfl_game_type,
            nfl_team_code,
            total_tackles,
            total_solo_tackles,
            total_tackles_with_assist,
            total_tackle_assists,
            total_tackles_for_loss,
            total_tackle_for_loss_yards,
            total_fumbles_forced,
            total_sacks,
            total_sack_yards,
            total_qb_hits,
            total_interceptions,
            total_pass_defended,
            total_touchdowns,
            total_fumbles,
            total_own_fumble_recoveries,
            total_own_fumble_recovery_yards,
            total_opponent_fumble_recoveries,
            total_opponent_fumble_recovery_yards,
            total_forced_safeties,
            total_penalties,
            total_penalty_yards
        from
            {{ ref('clean__dim__nflverse__player_stats_defense') }}
        where
            flag_latest
    )
select
    t1.flag_tombstoned,
    t1.dts_effective_to,
    t1.gsis_player_id,
    t1.nfl_season,
    t1.nfl_week,
    t1.nfl_game_type,
    t1.nfl_team_code,
    iff(t2.nfl_team_code_home = t1.nfl_team_code, t2.nfl_team_code_away, t2.nfl_team_code_home ) as nfl_team_code_opponent,
    t1.total_tackles,
    t1.total_solo_tackles,
    t1.total_tackles_with_assist,
    t1.total_tackle_assists,
    t1.total_tackles_for_loss,
    t1.total_tackle_for_loss_yards,
    t1.total_fumbles_forced,
    t1.total_sacks,
    t1.total_sack_yards,
    t1.total_qb_hits,
    t1.total_interceptions,
    t1.total_pass_defended,
    t1.total_touchdowns,
    t1.total_fumbles,
    t1.total_own_fumble_recoveries,
    t1.total_own_fumble_recovery_yards,
    t1.total_opponent_fumble_recoveries,
    t1.total_opponent_fumble_recovery_yards,
    t1.total_forced_safeties,
    t1.total_penalties,
    t1.total_penalty_yards
from
    defensive_stats t1
left join
    {{  ref('combine__dim__games_teams') }} t2
on
    t1.nfl_season = t2.nfl_season and
    t1.nfl_week = t2.nfl_game_week and
    (t1.nfl_team_code = t2.nfl_team_code_home_current or t1.nfl_team_code = t2.nfl_team_code_away_current) and
    t2.flag_latest
