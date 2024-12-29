{%- macro clean_array(column_name, delimiter=',', value_to_null=None) -%}

string_to_array(
    {{- column_name }}, '{{ delimiter }}'
{%- if value_to_null is not none %}
    , '{{ value_to_null }}'    
{% endif -%})
                   
{%- endmacro -%}