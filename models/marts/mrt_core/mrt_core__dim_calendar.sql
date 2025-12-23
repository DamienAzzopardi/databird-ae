with date_spine as (
  {{ dbt_utils.date_spine(
      datepart = "day",
      start_date = "cast('2005-01-01' as date)",
      end_date = "cast('2030-01-01' as date)"
  ) }}
),

year_date as (
  select
    date_day,
    cast(extract(year from date_day) as int64) as year_actual2
  from date_spine
),

bank_holidays_fr as (
  select
    cast(date as date) as bank_holidays_fr_date,
    name_bank_holiday as bank_holidays_fr_desc
  from {{ ref('bank_holidays_france') }}
),

final as (
  select
    cast(year_date.date_day as date) as date_day,

    -- Components
    extract(year from year_date.date_day) as year_actual,
    extract(quarter from year_date.date_day) as quarter_actual,
    extract(month from year_date.date_day) as month_actual,
    extract(week from year_date.date_day) as week_actual,
    extract(day from year_date.date_day) as day_actual,
    extract(isoweek from year_date.date_day) as iso_week,
    cast(format_date('%u', year_date.date_day) as int64) as iso_day_of_week,

    -- Labels
    concat('Quarter ', cast(extract(quarter from year_date.date_day) as string)) as quarter_name,
    concat('Q', cast(extract(quarter from year_date.date_day) as string)) as quarter_name_short,
    format_date('%B', year_date.date_day) as month_name,
    substr(format_date('%b', year_date.date_day), 1, 3) as month_name_short,
    format_date('%A', year_date.date_day) as day_name,
    substr(format_date('%a', year_date.date_day), 1, 3) as day_name_short,

    -- Keys
    cast(extract(year from year_date.date_day) as int64) as year_key,

    cast(concat(
      cast(extract(year from year_date.date_day) as string),
      cast(extract(quarter from year_date.date_day) as string)
    ) as int64) as quarter_key,

    cast(concat(
      cast(extract(year from year_date.date_day) as string),
      lpad(cast(extract(month from year_date.date_day) as string), 2, '0')
    ) as int64) as month_key,

    cast(concat(
      cast(extract(year from year_date.date_day) as string),
      lpad(cast(extract(isoweek from year_date.date_day) as string), 2, '0')
    ) as int64) as week_key,

    -- Year boundaries
    cast(date_trunc(year_date.date_day, year) as date) as first_day_of_year,
    cast(last_day(year_date.date_day, year) as date) as last_day_of_year,

    -- Quarter boundaries
    cast(date_trunc(year_date.date_day, quarter) as date) as first_day_of_quarter,
    cast(last_day(year_date.date_day, quarter) as date) as last_day_of_quarter,

    -- Month boundaries
    cast(date_trunc(year_date.date_day, month) as date) as first_day_of_month,
    cast(last_day(year_date.date_day, month) as date) as last_day_of_month,
    extract(day from last_day(year_date.date_day, month)) as number_of_days_in_month,

    -- Week boundaries (ISO Monday start)
    cast(date_sub(year_date.date_day, interval (cast(format_date('%u', year_date.date_day) as int64) - 1) day) as date) as first_day_of_week,
    cast(date_add(year_date.date_day, interval (7 - cast(format_date('%u', year_date.date_day) as int64)) day) as date) as last_day_of_week,

    -- Position in time
    row_number() over (
      partition by extract(year from year_date.date_day)
      order by year_date.date_day
    ) as day_of_year,

    row_number() over (
      partition by extract(year from year_date.date_day),
                   extract(quarter from year_date.date_day)
      order by year_date.date_day
    ) as day_of_quarter,

    row_number() over (
      partition by extract(year from year_date.date_day),
                   extract(month from year_date.date_day),
                   extract(dayofweek from year_date.date_day)
      order by year_date.date_day
    ) as ordinal_weekday_of_month,

    -- Weekend/working day (ISO definition)
    (cast(format_date('%u', year_date.date_day) as int64) in (6, 7)) as is_weekend,
    (cast(format_date('%u', year_date.date_day) as int64) not in (6, 7)
      and bank_holidays_fr.bank_holidays_fr_desc is null) as is_working_day,

    -- Holidays FR
    (bank_holidays_fr.bank_holidays_fr_desc is not null) as is_holiday_fr,
    bank_holidays_fr.bank_holidays_fr_desc as holiday_fr_description

  from year_date
  left join bank_holidays_fr
    on cast(year_date.date_day as date) = bank_holidays_fr.bank_holidays_fr_date
)

select *
from final
