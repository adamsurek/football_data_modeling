{%- macro clean_timestamp(column_name, timestamp_format='YYYY-MM-DD HH24:MI:SS.US') -%}
    case trim({{ column_name }})
        when '' then null
        else to_timestamp(trim({{ column_name }}), '{{ timestamp_format }}')
    end
{%- endmacro -%}