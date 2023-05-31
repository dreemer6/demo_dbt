{%- macro limit_data_in_dev(column_name, interval=3) -%}

{%- if target.name == 'default' %}
where {{ column_name }} > date_add(current_date, interval -{{ interval }} day)
{% endif -%}

{%- endmacro -%}