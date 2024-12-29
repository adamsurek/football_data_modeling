{%- macro clean_date(column_name, date_format='YYYY-MM-DD') -%}
    case trim({{ column_name }})
        when '' then null
        else to_date(trim({{ column_name }}), '{{ date_format }}')
    end
{%- endmacro -%}