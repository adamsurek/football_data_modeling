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
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_string('player_name') }} as player_name,
    {{ clean_string('pos') }} as player_position,
    {{ clean_string('pfr_id') }} as pfr_id,
    {{ clean_string('cfb_id') }} as cfb_id,
    split_part({{ clean_string('ht') }}, '-', 1) as player_height_feet,
    split_part({{ clean_string('ht') }}, '-', 2) as player_height_inches,
    {{ clean_integer('wt') }} as player_weight,
    {{ clean_decimal('forty', 2) }} as forty_yard_dash_seconds,
    {{ clean_integer('bench') }} as bench_reps,
    {{ clean_decimal('vertical', 1) }} as vertical_jump_inches,
    {{ clean_integer('broad_jump') }} as broad_jump_inches,
    {{ clean_decimal('cone') }} as cone_drill_seconds,
    {{ clean_decimal('shuttle') }} as shuttle_run_seconds
from
    {{ ref('hist__dim__nflverse__combine') }}