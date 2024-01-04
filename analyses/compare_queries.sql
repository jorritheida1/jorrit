{# in dbt Develop #}

{% set old_etl_relation=ref('customer_orders_practice') %}
{% set dbt_relation=ref('fct_customer_orders_practice') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="order_id"
) }}