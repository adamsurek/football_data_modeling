{%- macro clean_string(column_name) -%}
    case trim({{ column_name }})
        when '' then null
        else trim({{ column_name }})
    end
{%- endmacro -%}