{% macro drop_ci_schema() %}

    {#- Drops the temporary dataset of a CI run. Refuses to run on any other target. -#}
    {% if target.name != 'ci' %}
        {{ exceptions.raise_compiler_error("drop_ci_schema can only run against the ci target, got: " ~ target.name) }}
    {% endif %}

    {% do adapter.drop_schema(api.Relation.create(database=target.database, schema=target.schema)) %}
    {{ log("Dropped CI schema " ~ target.schema, info=True) }}

{% endmacro %}