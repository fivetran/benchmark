{% macro stddev(column) -%}
    {{ return(adapter.dispatch('stddev')(column)) }}
{%- endmacro %}


{% macro default__stddev(column) -%}
    stddev({{ column }})
{%- endmacro %}


{% macro synapse__stddev(column) %}
    Stdev({{ column }})
{% endmacro %}
