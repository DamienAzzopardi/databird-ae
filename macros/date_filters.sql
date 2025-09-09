{% macro filter_last_n_periods(column_name, datepart, n=1) -%}
  {{ column_name }} >= {{ dbt.dateadd(datepart | lower, -1 * n, 'current_date') }}
{%- endmacro %}