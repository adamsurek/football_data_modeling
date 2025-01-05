{{-
    config(
        materialized='table'
    )
-}}

with
    current_season as (
        select
            max(nfl_season) as current_season
        from
            {{ ref('clean__dim__nflverse__teams') }}
    ),
    nfl_team_code_history as (
        select
            nfl_team_id,
            nfl_team_code,
            min(nfl_season) as nfl_season_start,
            max(nfl_season) as nfl_season_end
        from
            {{ ref('clean__dim__nflverse__teams') }}
        where
            flag_latest
        group by
            nfl_team_id,
            nfl_team_code
    ),
    nfl_team_codes_current as (
        select
            t1.nfl_team_id,
            t1.nfl_team_code,
            iff(t1.nfl_season_start = 2002, 1900, t1.nfl_season_start) as nfl_season_start,
            t1.nfl_season_end,
            t3.nfl_team_code as nfl_team_code_current
        from
            nfl_team_code_history t1
        cross join
            current_season t2
        left join
            nfl_team_code_history t3
        on
            t1.nfl_team_id = t3.nfl_team_id
            and t3.nfl_season_end = t2.current_season
    )
select
    t1.flag_latest,
    t1.dts_effective_from,
    t1.nflverse_game_id,
    t1.nfl_season,
    t1.nfl_game_type,
    t1.nfl_game_week,
    iff(t1.nfl_game_type = 'SB', t1.nfl_game_week + 1, t1.nfl_game_week) as nfl_game_week_ngs,
    t1.date_gameday,
    t1.nfl_team_code_away,
    t2.nfl_team_code_current as nfl_team_code_away_current,
    t1.nfl_team_code_home,
    t3.nfl_team_code_current as nfl_team_code_home_current,
    t1.score_away,
    t1.score_home,
    t1.score_total,
    t1.flag_overtime_occurred,
    t1.rest_days_away,
    t1.rest_days_home,
    t1.moneyline_away,
    t1.moneyline_home,
    t1.spread_line,
    t1.spread_odds_away,
    t1.spread_odds_home,
    t1.total_line,
    t1.under_odds,
    t1.over_odds,
    t1.flag_division_game,
    t1.game_location,
    t1.nfl_stadium_id,
    t1.stadium_roof_type,
    t1.stadium_surface_type,
    t1.temperature_farenheit,
    t1.wind_miles_per_hour
from
    {{ ref('clean__dim__nflverse__games') }} t1
left join
    nfl_team_codes_current t2
on
    t1.nfl_team_code_away = t2.nfl_team_code
    and t1.nfl_season between t2.nfl_season_start and t2.nfl_season_end
left join
    nfl_team_codes_current t3
on
    t1.nfl_team_code_home = t3.nfl_team_code
    and t1.nfl_season between t3.nfl_season_start and t3.nfl_season_end
