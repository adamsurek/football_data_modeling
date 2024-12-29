{% macro standard_scd_2_hist(
    this,
    source_table,
    is_incremental,
    date_column,
    pk_columns,
    data_columns,
    pre_filter
) %}

with
    {%- if is_incremental %}
    latest_data as (
        select
            flag_latest,
            flag_tombstoned,
            pk,
            sk,
            sk_prev,
            sk_next,
            dts_effective_from,
            dts_effective_to,
            {% for column in data_columns -%}
            {{ column -}}{% if not loop.last %},{% endif %}
            {% endfor %}
        from
            {{ this }}
        where
            flag_latest
    ),
    {%- endif %}
    max_load_pk as (
        select
            {%- for column in pk_columns %}
            {{ ifnull(column, "''") }} {% if not loop.last -%} || '^' || {% endif -%}
                {%- endfor %} as pk,
            max(cast(dts_loaded as timestamp)) as max_dts_load_this
        from
            {{ source_table }}
        group by
            pk
    ),
    last_load as (
        select
            max(cast({{ date_column }} as timestamp)) as max_dts_load_source
        from
            {{ source_table }}
    ),
    new_records as  (
        select
            {%- for column in pk_columns %}
            {{ ifnull(column, "''") }} {% if not loop.last -%} || '^' || {% endif -%}
                {%- endfor %} as pk,
            {{ dbt_utils.generate_surrogate_key(data_columns) }} as sk,
            cast({{ date_column }} as timestamp) as {{ date_column }},
            {% for column in data_columns -%}
            {{ column -}}{% if not loop.last %},{% endif %}
            {% endfor %}
        from
            {{ source_table }}
            {%- if is_incremental %}
        where
            cast({{ date_column }} as timestamp) > (
                select
                    max(dts_effective_from)
                from
                    latest_data
            )
            {%- if pre_filter != '' %}
          and {{ pre_filter -}}
    {% endif -%}
    {% else %}
    {%- if pre_filter != '' %}
where
    {{- pre_filter -}}
    {% endif -%}
    {% endif %}
    ),
    union_new_old as (
    (
    select
    pk,
    sk,
    null as sk_prev,
    null as sk_next,
    {{ date_column }} as dts_effective_from,
    {% for column in data_columns -%}
    {{ column -}}{% if not loop.last %},{% endif %}
    {% endfor %}
    from
    new_records
    )
    {% if is_incremental %}
union all

(
select
    pk,
    sk,
    sk_prev,
    sk_next,
    dts_effective_from,
    {% for column in data_columns -%}
    {{ column -}}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    latest_data
    )
    {% endif %}
    ),
    changed_records_only as (
select distinct on (pk, sk)
    pk,
    sk,
    dts_effective_from,
    max_dts_load_this,
    sk_prev,
    sk_next,
    {% for column in data_columns -%}
    {{ column -}}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    union_new_old
    left join
    max_load_pk
    using
    (pk)
order by
    pk,
    sk,
    dts_effective_from
    ),
    historize as (
select
    pk,
    sk,
    dts_effective_from,
    max_dts_load_this,
    case
    when sk_prev is null then lag(sk) over (partition by pk order by dts_effective_from)
    else sk_prev
    end as sk_prev,
    case
    when sk_next is null then lead(sk) over (partition by pk order by dts_effective_from)
    else sk_next
    end as sk_next,
    {% for column in data_columns -%}
    {{ column -}}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    changed_records_only
    ),
    sk_flags as (
select
    (t1.sk_prev is null) as flag_sk_first,
    (t1.sk_next is null) as flag_sk_last,
    t1.pk,
    t1.sk,
    t1.sk_prev,
    t1.sk_next,
    t2.max_dts_load_source,
    max(t1.max_dts_load_this) over (partition by t1.pk order by t1.dts_effective_from) as max_dts_load_this,
    t1.dts_effective_from,
    lead(t1.dts_effective_from) over (partition by t1.pk order by t1.dts_effective_from) as dts_effective_to,
    {% for column in data_columns -%}
    t1.{{ column -}}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    historize t1
    cross join
    last_load t2
    )
select
    flag_sk_last as flag_latest,
    (not flag_sk_last or max_dts_load_source != max_dts_load_this) as flag_tombstoned,
    pk,
    sk,
    sk_prev,
    sk_next,
    dts_effective_from,
    case
        when max_dts_load_source != max_dts_load_this then max_dts_load_this
        when dts_effective_to is null then '9999-12-31 23:59:59'
        else dts_effective_to
        end as dts_effective_to,
    {% for column in data_columns -%}
    {{ column -}}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    sk_flags

{% endmacro %}
