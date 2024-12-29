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
    {{ clean_string('trade_id') }} as trade_id,
    {{ clean_integer('season') }} as nfl_season,
    {{ clean_string('gave') }} as team_giving,
    {{ clean_string('received') }} as team_receiving,
    {{ clean_integer('pick_season') }} as draft_pick_season,
    {{ clean_integer('pick_round') }} as draft_pick_round,
    {{ clean_string('pfr_id')}} as pfr_id,
    {{ clean_date('trade_date')}} as date_trade,
    {{ clean_bool('conditional')}} as flag_conditional_pick
from
    {{ ref('hist__dim__nflverse__trades') }}