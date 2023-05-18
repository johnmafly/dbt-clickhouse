{% materialization distributed_table, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ log(existing_relation) }}
  {{ log(target_relation) }}
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

{% macro create_distributed_remote_table_or_empty(temporary, relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary -%}
        create temporary table {{ relation.name }}
        engine Memory
        {{ order_cols(label="order by") }}
        {{ partition_cols(label="partition by") }}
        {{ adapter.get_model_settings(model) }}
    {%- else %}
        create table {{ relation.include(database=False) }}_local
        {{ on_cluster_clause()}}
        {{ engine_clause() }}
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
    engine = Distributed({{adapter.get_clickhouse_cluster_name()}}, {{schema}}, {{remote_database}}, {{sharding_key}})
{%- endmacro %}


{% macro get_create_distributed_table_as_sql(temporary, relation, sql) %}
    {{ return(create_distributed_table_as(temporary, relation, sql)) }}
{%- endmacro %}


{% macro create_distributed_table_as(temporary, relation, sql) -%}
    {% set create_remote_table = create_distributed_remote_table_or_empty(temporary, relation, sql) %}
    {% if adapter.is_before_version('22.7.1.2484') -%}
        {{ create_table }}
    {%- else %}
        {% call statement('create_remote_table') %}
            {{ create_remote_table }}
        {% endcall %}
        {% call statement('create_distributed_table') %}
            {{ create_distributed_table(relation) }}
        {% endcall %}
        {{ clickhouse__insert_into(relation.include(database=False), sql) }}

    {%- endif %}
{%- endmacro %}

{% macro create_distributed_persistent_table(target_relation, intermediate_relation) -%}
    {%- set sharding_key = config.get('sharding_key', 'rand()') -%}
    create table if not exists {{ intermediate_relation.include(database=False) }} {{ on_cluster_clause() }} as {{ intermediate_relation.include(database=False) }}_local
    engine = Distributed({{ adapter.get_clickhouse_cluster_name() }}, {{ target_relation.schema }}, {{ target_relation.identifier }}_local, sipHash64({{ sharding_key }}))

{%- endmacro %}

{% macro create_distributed_table(relation) -%}
    {%- set sharding_key = config.get('sharding_key', 'rand()') -%}
    create table if not exists {{ relation.include(database=False) }} {{ on_cluster_clause()}} as {{ relation.include(database=False) }}_local
      engine = Distributed({{ adapter.get_clickhouse_cluster_name() }}, {{ relation.schema }}, {{ relation.identifier }}_local, sipHash64({{ sharding_key }}))

{%- endmacro %}