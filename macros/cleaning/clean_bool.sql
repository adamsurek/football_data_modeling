{%- macro clean_bool(column_name, null_value=None) -%}
{%- if null_value is not none and null_value not in [True, False] -%}
{{- exceptions.raise_compiler_error("Invalid `null_value` param value. Expected `None` or boolean, got " ~ null_value) -}}
{%- endif %} 
    
case trim(lower({{ column_name }}))
    when 'y' then true
    when 'yes' then true
    when '1' then true
    when 'true' then true
    when 'n' then false
    when 'no' then false
    when '0' then false
    when 'false' then false
    when null then {% if null_value is none %}null{% else %}{{ null_value }}{% endif %} 
    else false
end
{%- endmacro -%}