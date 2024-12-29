{%- macro clean_decimal(column_name, precision=2) -%}
    nullif(trim({{ column_name }}), '')::decimal(38, {{ precision }})
{%- endmacro -%}