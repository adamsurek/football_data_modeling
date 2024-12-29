{%- macro clean_integer(column_name) -%}
    nullif(trim({{ column_name }}), '')::int
{%- endmacro -%}