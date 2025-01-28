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
    {{ clean_date('scrape_date') }} as date_rating,
    {{ clean_string('fp_page') }} as fantasypros_ecr_page_name,
    {{ clean_string('id') }} as fantasypros_player_id,
    {{ clean_string('page_type') }} as fantasypros_ecr_page_type,
    {{ clean_string('ecr_type') }} as fantasypros_ecr_type_code,
    {{ clean_decimal('ecr', 1) }} as ecr_avg,
    {{ clean_decimal('sd', 1) }} as ecr_std,
    {{ clean_decimal('best', 0) }} as ecr_best,
    {{ clean_decimal('worst', 0) }} as ecr_worst,
    {{ clean_decimal('player_owned_avg', 1) }} as fantasy_ownership
from
    {{ ref('hist__dim__dynastyprocess__fp_ecr') }}