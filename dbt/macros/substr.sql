{% macro substr(column, start_position, num_characters) -%}
    {{ return(adapter.dispatch('substr')(column, start_position, num_characters)) }}
{%- endmacro %}


{% macro default__substr(column, start_position, num_characters) -%}
    substr({{ column }}, {{ start_position }}, {{ num_characters }})
{%- endmacro %}


{% macro redshift__substr(column, start_position, num_characters) %}
    substring({{ column }}, {{ start_position }}, {{ num_characters }})
{% endmacro %}


{% macro synapse__substr(column, start_position, num_characters) %}
    substring({{ column }}, {{ start_position }}, {{ num_characters }})
{% endmacro %}
