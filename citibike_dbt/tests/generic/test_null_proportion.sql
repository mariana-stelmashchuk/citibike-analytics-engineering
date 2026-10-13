{% test null_proportion(model, column_name, max_proportion, group_by_column=none) %}

{#-
    Fails when the share of nulls in column_name is above max_proportion (0 to 1).
    With group_by_column, the share is checked separately for each group, so one
    bad batch cannot hide among many good ones.
-#}

with grouped as (

    select
        {% if group_by_column %}
        cast({{ group_by_column }} as string) as group_value,
        {% else %}
        'all rows' as group_value,
        {% endif %}
        count(*) as total_cnt,
        sum(case when {{ column_name }} is null then 1 else 0 end) as null_cnt
    from {{ model }}
    {% if group_by_column %}
    group by {{ group_by_column }}
    {% endif %}

)

select
    group_value,
    total_cnt,
    null_cnt,
    null_cnt * 1.0 / total_cnt as null_proportion
from grouped
where null_cnt * 1.0 / total_cnt > {{ max_proportion }}

{% endtest %}