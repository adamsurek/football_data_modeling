{% test one_pk_per_load(model) %}

    select
        pk,
        dts_effective_from,
        count(*) cnt
    from
        {{ model }}
    group by
        pk,
        dts_effective_from
    having
        count(*) > 1

{% endtest %}