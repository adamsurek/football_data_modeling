{% test one_flag_latest_per_pk(model) %}

    select
        pk,
        count(*) cnt
    from
        {{ model }}
    where
        flag_latest
    group by
        pk
    having
        count(*) > 1

{% endtest %}