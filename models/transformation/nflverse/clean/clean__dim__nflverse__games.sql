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
    {{ clean_string('game_id') }} as nflverse_game_id,
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_string('game_type') }} as nfl_game_type,
    {{ clean_integer('week') }} as nfl_game_week,
    {{ clean_date('gameday') }} as date_gameday,
    {{ clean_string('away_team') }} as nfl_team_id_away,
    {{ clean_string('home_team') }} as nfl_team_id_home,
    {{ clean_integer('away_score') }} as score_away,
    {{ clean_integer('home_score') }} as score_home,
    {{ clean_integer('total') }} as score_total,
    {{ clean_bool('overtime') }} as flag_overtime_occurred,
    {{ clean_integer('away_rest') }} as rest_days_away,
    {{ clean_integer('home_rest') }} as rest_days_home,
    {{ clean_integer('away_moneyline') }} as moneyline_away,
    {{ clean_integer('home_moneyline') }} as moneyline_home,
    {{ clean_decimal('spread_line', 1) }} as spread_line,
    {{ clean_integer('away_spread_odds') }} as spread_odds_away,
    {{ clean_integer('home_spread_odds') }} as spread_odds_home,
    {{ clean_decimal('total_line', 1) }} as total_line,
    {{ clean_integer('under_odds') }} as under_odds,
    {{ clean_integer('over_odds') }} as over_odds,
    {{ clean_bool('div_game') }} as flag_division_game,
    {{ clean_string('location') }} as game_location,
    {{ clean_string('stadium_id') }} as nfl_stadium_id,
    {{ clean_string('roof') }} as stadium_roof_type,
    {{ clean_string('surface') }} as stadium_surface_type,
    {{ clean_integer('temp') }} as temperature_farenheit,
    {{ clean_integer('wind') }} as wind_miles_per_hour
from
    {{ ref('hist__dim__nflverse__games') }}