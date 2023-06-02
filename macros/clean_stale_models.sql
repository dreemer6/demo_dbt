{% macro clean_stale_models(project=target.project, dataset=target.dataset, dry_run=True) %}
    {# Get models that currently exist in dbt #}
    {% if execute %}
       {% set current_models=[] %} 
       {% for node in graph.nodes.values()
            | selectattr("resource_type", "in", ["model", "seed", "snapshot"]) %}
        {% do current_models.append(node.name) %}
       {% endfor %}
    {% endif %}

    {% set clean_query %}
        with stale_models as 
            (select
                case when table_type = 'VIEW' then table_type
                    else 'TABLE' end as drop_type,
                table_schema || '.' || table_name as relation_name
            from `{{ target.project }}.{{ target.dataset }}`.INFORMATION_SCHEMA.TABLES
            where table_schema = '{{ dataset }}'
                and table_name not in 
                ({% for model in current_models %}
                    '{{ model }}'
                    {% if not loop.last %}
                        ,
                    {% endif %}
                {% endfor %})
            )
        select 
            'DROP ' || drop_type || ' ' || relation_name || ';' as drop_commands
        from stale_models
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}

    {% set drop_commands = run_query(clean_query).columns[0].values() %}
    {# Execute the drop commands for the dataset #}
    {% if drop_commands %}
        {% for drop_command in drop_commands %}
            {% if dry_run %}
                {{ log(drop_command, info=True) }}
            {% else %}
                {{ log("Dropping relation with command: " ~ drop_command, info=True) }}
                {% do run_query(drop_command) %}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("No relations to drop", info=True) }}
    {% endif %}
        
{% endmacro %}