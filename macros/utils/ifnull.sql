{% macro ifnull(column_name, val_if_null) -%}
    coalesce({{ column_name }}, {{ val_if_null }})
{%- endmacro %}