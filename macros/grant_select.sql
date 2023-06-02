{% macro grant_select(project, dataset) %}

    {% set query %}
        grant select on {{ project }}
    {% endset %}
    
{% endmacro %}