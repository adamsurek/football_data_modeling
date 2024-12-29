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
    {{ clean_string('iff(mfl_id = \'NA\', null, mfl_id)') }} as mfl_player_id,
    {{ clean_string('iff(fantasypros_id = \'NA\', null, fantasypros_id)') }} as fantasypros_player_id,
    {{ clean_string('iff(gsis_id = \'NA\', null, gsis_id)') }} as gsis_player_id,
    {{ clean_string('iff(pff_id = \'NA\', null, pff_id)') }} as pff_player_id,
    {{ clean_string('iff(nfl_id = \'NA\', null, nfl_id)') }} as nfl_player_id,
    {{ clean_string('iff(espn_id = \'NA\', null, espn_id)') }} as espn_player_id,
    {{ clean_string('iff(yahoo_id = \'NA\', null, yahoo_id)') }} as yahoo_player_id,
    {{ clean_string('iff(pfr_id = \'NA\', null, pfr_id)') }} as pfr_player_id,
    {{ clean_string('iff(cfbref_id = \'NA\', null, cfbref_id)') }} as cfbref_player_id
from
    {{ ref('hist__dim__nflverse__player_ids') }}