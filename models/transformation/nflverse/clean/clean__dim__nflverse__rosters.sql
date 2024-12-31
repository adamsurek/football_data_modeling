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
    {{ clean_string('gsis_it_id') }} as gsis_it_player_id,
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_integer('week') }} as nfl_week,
    {{ clean_string('game_type') }} as nfl_game_type,
    {{ clean_string('team') }} as nfl_team_code,
    {{ clean_string('position') }} as team_position,
    {{ clean_string('ngs_position') }} as team_position_next_gen_stats,
    {{ clean_string('depth_chart_position') }} as depth_chart_position,
    {{ clean_string('status') }} as status,
    {{ clean_string('status_description_abbr') }} as status_description_code,
    {{ clean_string('gsis_id') }} as gsis_player_id,
    {{ clean_string('espn_id') }} as espn_player_id,
    {{ clean_string('sportradar_id') }} as sportradar_player_id,
    {{ clean_string('yahoo_id') }} as yahoo_player_id,
    {{ clean_string('rotowire_id') }} as rotowire_player_id,
    {{ clean_string('pff_id') }} as pff_player_id,
    {{ clean_string('pfr_id') }} as pfr_player_id,
    {{ clean_string('fantasy_data_id') }} as fantasy_data_player_id,
    {{ clean_string('sleeper_id') }} as sleeper_player_id,
    {{ clean_string('esb_id') }} as esb_player_id,
    {{ clean_string('smart_id') }} as smart_player_id
from
    {{ ref('hist__dim__nflverse__rosters') }}
    