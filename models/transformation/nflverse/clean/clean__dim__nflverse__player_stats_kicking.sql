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
    {{ clean_string('player_id') }} as gsis_player_id,
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_integer('week') }} as nfl_week,
    {{ clean_string('team') }} as nfl_team_id,
    {{ clean_string('season_type') }} as nfl_game_type,
    {{ clean_integer('fg_made') }} as total_field_goals_made,
    {{ clean_integer('fg_att') }} as total_field_goals_attempted,
    {{ clean_integer('fg_missed') }} as total_field_goals_missed,
    {{ clean_integer('fg_blocked') }} as total_field_goals_blocked,
    {{ clean_integer('fg_long') }} as longest_field_goal,
    {{ clean_decimal('fg_pct', 3) }} as field_goal_percentage,
    {{ clean_integer('fg_made_0_19') }} as total_field_goals_made_0_19,
    {{ clean_integer('fg_made_20_29') }} as total_field_goals_made_20_29,
    {{ clean_integer('fg_made_30_39') }} as total_field_goals_made_30_39,
    {{ clean_integer('fg_made_40_49') }} as total_field_goals_made_40_49,
    {{ clean_integer('fg_made_50_59') }} as total_field_goals_made_50_59,
    {{ clean_integer('fg_made_60_') }} as total_field_goals_made_60_plus,
    {{ clean_integer('fg_missed_0_19') }} as total_field_goals_missed_0_19,
    {{ clean_integer('fg_missed_20_29') }} as total_field_goals_missed_20_29,
    {{ clean_integer('fg_missed_30_39') }} as total_field_goals_missed_30_39,
    {{ clean_integer('fg_missed_40_49') }} as total_field_goals_missed_40_49,
    {{ clean_integer('fg_missed_50_59') }} as total_field_goals_missed_50_59,
    {{ clean_integer('fg_missed_60_') }} as total_field_goals_missed_60_plus,
    {{ clean_integer('fg_made_distance') }} as total_field_goals_made_distance,
    {{ clean_integer('fg_missed_distance') }} as total_field_goals_missed_distance,
    {{ clean_integer('fg_blocked_distance') }} as total_field_goals_blocked_distance,
    {{ clean_array('fg_made_list') }} as list_field_goals_made,
    {{ clean_array('fg_missed_list') }} as list_field_goals_missed,
    {{ clean_array('fg_blocked_list') }} as list_field_goals_blocked,
    {{ clean_integer('pat_made') }} as total_extra_points_made,
    {{ clean_integer('pat_att') }} as total_extra_points_attempted,
    {{ clean_integer('pat_missed') }} as total_extra_points_missed,
    {{ clean_integer('pat_blocked') }} as total_extra_points_blocked,
    {{ clean_integer('gwfg_att') }} as total_game_winning_field_goals_attempted,
    {{ clean_integer('gwfg_made') }} as total_game_winning_field_goals_made,
    {{ clean_integer('gwfg_missed') }} as total_game_winning_field_goals_missed,
    {{ clean_integer('gwfg_blocked') }} as total_game_winning_field_goals_blocked

from
    {{ ref('hist__dim__nflverse__player_stats_kicking') }}