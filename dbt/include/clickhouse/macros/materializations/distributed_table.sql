{% materialization distributed_table, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set backup_relation = none -%}
  {%- set preexisting_backup_relation = none -%}
  {%- set preexisting_intermediate_relation = none -%}

  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {% if not existing_relation.can_exchange %}
      {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
      {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {% endif %}
  {% endif %}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ clickhouse__drop_relation(preexisting_intermediate_relation) }}
  {{ clickhouse__drop_relation(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if backup_relation is none %}
    {{ log('Creating new relation ' + target_relation.name )}}
    -- There is not existing relation, so we can just create
    {% call statement('main') -%}
      {{ get_create_distributed_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}
  {% elif existing_relation.can_exchange %}
    -- We can do an atomic exchange, so no need for an intermediate
    {% call statement('main') -%}
      {{ get_create_distributed_table_as_sql(False, backup_relation, sql) }}
    {%- endcall %}
    {% do exchange_tables_atomic(backup_relation, existing_relation) %}
  {% else %}
    -- We have to use an intermediate and rename accordingly
    {% call statement('main') -%}
      {{ get_create_distributed_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% endif %}

  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ clickhouse__drop_relation(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro create_distributed_table_or_empty(temporary, relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set remote_database = config.get('remote_database', relation.identifier+'_local') -%}
    {%- set sharding_key = config.get('sharding_key', 'rand()') -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary -%}
        create temporary table {{ relation.name }}
        engine Memory
        {{ order_cols(label="order by") }}
        {{ partition_cols(label="partition by") }}
        {{ adapter.get_model_settings(model) }}
    {%- else %}
        create table {{ relation.include(database=False) }}
        {{ on_cluster_clause()}}
        {{ distributed_engine_clause(relation.schema, remote_database, sharding_key) }}
        {{ order_cols(label="order by") }}
        {{ primary_key_clause(label="primary key") }}
        {{ partition_cols(label="partition by") }}
        {{ adapter.get_model_settings(model) }}
        {% if not adapter.is_before_version('22.7.1.2484') -%}
            empty
        {%- endif %}
    {%- endif %}
    as (
        {{ sql }}
    )
{%- endmacro %}


{% macro distributed_engine_clause(schema, remote_database, sharding_key) %}
    engine = ({{adapter.get_clickhouse_cluster_name()}}, {{schema}}, {{remote_database}}, {{sharding_key}})
{%- endmacro %}


{% macro get_create_distributed_table_as_sql(temporary, relation, sql) %}
    {{ return(create_distributed_table_as(temporary, relation, sql)) }}
{%- endmacro %}

{% macro create_distributed_table_as(temporary, relation, compiled_code, language='sql') -%}
  {# backward compatibility for create_table_as that does not support language #}
  {% if language == "sql" %}
    {{ adapter.dispatch('create_distributed_table_as', 'dbt')(temporary, relation, compiled_code)}}
  {% else %}
    {{ adapter.dispatch('create_distributed_table_as', 'dbt')(temporary, relation, compiled_code, language) }}
  {% endif %}

{%- endmacro %}
{% macro clickhouse__create_distributed_table_as(temporary, relation, sql) -%}
    {% set create_table = create_distributed_table_or_empty(temporary, relation, sql) %}
    {% if adapter.is_before_version('22.7.1.2484') -%}
        {{ create_table }}
    {%- else %}
        {% call statement('create_table_empty') %}
            {{ create_table }}
        {% endcall %}
        {{ clickhouse__insert_into(relation.include(database=False), sql) }}
    {%- endif %}
{%- endmacro %}