{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- elif target.database | upper == 'AI_USAGE_ANALYTICS'
          and target.schema | upper == 'SILVER'
          and custom_schema_name | trim | upper == 'GOLD' -%}

        GOLD

    {%- else -%}

        {{ target.schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}