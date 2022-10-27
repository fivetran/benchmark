{%- materialization query, default -%}
  -- build model
  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}

  {{ return({'relations': []}) }}

{%- endmaterialization -%}
