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
    {{ clean_date('\"\"\"scrape_date\"\"\"') }} as date_player_value,
    {{ clean_string('\"\"\"fp_id\"\"\"') }} as fantasypros_player_id,
    {{ clean_decimal('\"\"\"ecr_1qb\"\"\"', 1) }} as ecr_single_qb,
    {{ clean_decimal('\"\"\"ecr_pos\"\"\"', 1) }} as ecr_position,
    {{ clean_integer('\"\"\"value_1qb\"\"\"') }} as value_single_qb
from
    {{ ref('hist__dim__dynastyprocess__fp_player_values') }}