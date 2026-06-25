{%- macro drop_table_in_schema_if_exists(table_name) -%}
    {%- set relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=table_name
    ) -%}

    {%- if relation is not none -%}
        {% do adapter.drop_relation(relation) %}
    {%- endif -%}
{%- endmacro -%}
