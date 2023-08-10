{% materialization distributed_incremental, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {%- set unique_key = config.get('unique_key') -%}
  {% if unique_key is not none and unique_key|length == 0 %}
    {% set unique_key = none %}
  {% endif %}
  {% if unique_key is iterable and (unique_key is not string and unique_key is not mapping) %}
     {% set unique_key = unique_key|join(', ') %}
  {% endif %}
  {%- set inserts_only = config.get('inserts_only') -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ clickhouse__drop_relation(preexisting_intermediate_relation) }}
  {{ clickhouse__drop_relation(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}

  {% if existing_relation is none %}
    {{log('existing_relation')}}
    {%- call statement('main') -%}
        {{log('drop_relation(intermediate_relation)')}}
        {%- do clickhouse__drop_relation(intermediate_relation) -%}
        {{log('get_create_distributed_table_as_sql')}}
        {{ get_create_distributed_table_as_sql(False, target_relation, sql) }}
    {%- endcall -%}

  {% elif full_refresh_mode %}
    {{log('full_refresh_mode')}}
    -- Completely replacing the old table, so create a temporary table and then swap it
    {% call statement('main') %}
        {{log('drop_relation(intermediate_relation)')}}
        {% do clickhouse__drop_relation(intermediate_relation) %}

        {{log('get_create_distributed_table_as_sql')}}
        {{ get_create_distributed_table_as_sql(False, intermediate_relation, sql) }}
    {% endcall %}
    {% set need_swap = true %}

  {% elif inserts_only or unique_key is none -%}
    -- There are no updates/deletes or duplicate keys are allowed.  Simply add all of the new rows to the existing
    -- table. It is the user's responsibility to avoid duplicates.  Note that "inserts_only" is a ClickHouse adapter
    -- specific configurable that is used to avoid creating an expensive intermediate table.
    {% set incremental_strategy = adapter.calculate_incremental_strategy(config.get('incremental_strategy'))  %}
    {% if incremental_strategy == 'append' %}
        {% call statement('main') %}
            {{ clickhouse__insert_into(target_relation, sql) }}
        {% endcall %}
    {% elif incremental_strategy == 'merge' %}
        {% call statement('main') %}
            {{ distributed_incremental_merge(target_relation, sql) }}
        {% endcall %}
    {% else %}
      {% do exceptions.raise_compiler_error('Cannot use incremental strategy ' + incremental_strategy + ' with inserts_only=True') %}
    {% endif %}

  {% else %}
    {% set schema_changes = none %}
    {% set incremental_strategy = adapter.calculate_incremental_strategy(config.get('incremental_strategy'))  %}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% if on_schema_change != 'ignore' %}
      {%- set schema_changes = check_for_schema_changes(existing_relation, target_relation) -%}
      {% if schema_changes['schema_changed'] and incremental_strategy in ('append', 'delete_insert') %}
        {% set incremental_strategy = 'legacy' %}
        {% do log('Schema changes detected, switching to legacy incremental strategy') %}
      {% endif %}
    {% endif %}
    {% if incremental_strategy == 'legacy' %}
      {% do clickhouse__distributed_incremental_legacy(existing_relation, intermediate_relation, schema_changes, unique_key) %}
      {% set need_swap = true %}
    {% elif incremental_strategy == 'delete_insert' %}
      {% do clickhouse__distributed_incremental_delete_insert(existing_relation, unique_key, incremental_predicates) %}
    {% elif incremental_strategy == 'merge' %}
      {% call statement('main') %}
          {{ distributed_incremental_merge(target_relation, sql, unique_key) }}
      {% endcall %}
    {% elif incremental_strategy == 'append' %}
      {% call statement('main') %}
        {{ clickhouse__insert_into(target_relation, sql) }}
      {% endcall %}
    {% endif %}
  {% endif %}

  {% if need_swap %}
      {% if existing_relation.can_exchange %}
        -- if existing_relation.can_exchange
      {{log('drop_distributed_temp_table1')}}
      {% call statement('drop_distributed_temp_table') %}
        drop table if exists {{intermediate_relation}} {{ on_cluster_clause() }}
      {% endcall %}
      {{log('drop_distributed_temp_table2')}}
      {% call statement('drop_distributed_temp_table') %}
        {{create_distributed_persistent_table(target_relation, intermediate_relation)}}
      {% endcall %}
        {% do adapter.rename_relation(intermediate_relation, backup_relation) %}
        {% do exchange_tables_atomic(backup_relation, target_relation) %}
      {% else %}
        -- else existing_relation.can_exchange
        {% do adapter.rename_relation(target_relation, backup_relation) %}
        {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% endif %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do clickhouse__drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}




{% macro clickhouse__distributed_incremental_legacy(existing_relation, intermediate_relation, on_schema_change, unique_key) %}
    -- First create a temporary table for all of the new data
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": model['name'] + '__dbt_new_data'}) %}
    {{ clickhouse__drop_relation(new_data_relation) }}
    {% call statement('create_new_data_temp') %}
        {{ get_create_distributed_table_as_sql(False, new_data_relation, sql) }}
    {% endcall %}

    -- Next create another temporary table that will eventually be used to replace the existing table.  We can't
    -- use the table just created in the previous step because we don't want to override any updated rows with
    -- old rows when we insert the old data
    {% call statement('main') %}
       create table {{ intermediate_relation }} as {{ new_data_relation }}
    {% endcall %}

    -- Insert all the existing rows into the new temporary table, ignoring any rows that have keys in the "new data"
    -- table.
    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% call statement('insert_existing_data') %}
        insert into {{ intermediate_relation }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ existing_relation }}
          where ({{ unique_key }}) not in (
            select {{ unique_key }}
            from {{ new_data_relation }}
          )
       {{ adapter.get_model_settings(model) }}
    {% endcall %}

    -- Insert all of the new data into the temporary table
    {% call statement('insert_new_data') %}
     insert into {{ intermediate_relation }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ new_data_relation }}
      {{ adapter.get_model_settings(model) }}
    {% endcall %}

    {% do clickhouse__drop_relation(new_data_relation) %}

{% endmacro %}

{% macro distributed_incremental_merge(target_relation, sql, unique_key = None) %}
    -- First create a temporary table for all of the new data
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}



  {%- set insert_into_sql -%}
  insert into {{ target_relation }}{{config.get('is_local', '')}} ({{ dest_cols_csv }})
  {%- endset %}

  {{ insert_into_sql }}
  {%- set renew_sql -%}
    with iv_source_sql as (
        -- Dbt Real Start Query Sql
        {{ sql }}
        -- Dbt Real End Query Sql
    )
    -- Default Start Get Min Block Time
    ,iv_source_data as (select * from {{this}}{{config.get('is_local', '')}} where {{config.get('merge_condition')}})
    -- Default End Get Min Block Time
    select * from iv_source_sql
    {% if unique_key is none %}
    except
    select * from iv_source_data
    {% else %}
    -- Merge Handle Sql
    where ({{unique_key}}) in (
      select {{ unique_key }} from iv_source_sql
      except
      select {{ unique_key }}
      from iv_source_data
    )
    {%endif%}
  {%endset%}
  {{ batch_insert_unmask(renew_sql, insert_into_sql) }}
{% endmacro %}

{% macro clickhouse__distributed_incremental_delete_insert(existing_relation, unique_key, incremental_predicates) %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": model['name']
       + '__dbt_new_data_' + invocation_id.replace('-', '_')}) %}
    {{ clickhouse__drop_relation(new_data_relation) }}
    -- clickhouse__distributed_incremental_delete_insert
    {% call statement('main') %}
        {{ get_create_distributed_table_as_sql(False, new_data_relation, sql) }}
    {% endcall %}
    {% call statement('delete_existing_data') %}
      delete from {{ existing_relation }} where ({{ unique_key }}) in (select {{ unique_key }}
                                          from {{ new_data_relation }})
      {%- if incremental_predicates %}
        {% for predicate in incremental_predicates %}
            and {{ predicate }}
        {% endfor %}
      {%- endif -%};
    {% endcall %}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% call statement('insert_new_data') %}
        insert into {{ existing_relation}} select {{ dest_cols_csv}} from {{ new_data_relation }}
    {% endcall %}
    {% do clickhouse__drop_relation(new_data_relation) %}
{% endmacro %}


