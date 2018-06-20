-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Calculate Growth for Month and Year
-- MAGIC 
-- MAGIC MoM Growth = Growth over previous month
-- MAGIC 
-- MAGIC YoY Growth = Current Year Aggregate value (Running total from beginning of the year untile the current month) over Previous Year Aggregate Value

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Calculate Year and Month totals

-- COMMAND ----------

drop table if exists samples.T1;
create table samples.T1 as
select 
  project,
  date_trunc('MM', reported_on) as reported_month,
  date_trunc('YYYY', reported_on) as reported_year,
  sum(hours_worked) as hours_worked
from 
  samples.project_timesheet
where
  date_trunc('MM', reported_on) <> '2016-10-01T00:00:00.000+0000' --create an hole in the time series
group by
  project,
  reported_year,
  reported_month

-- COMMAND ----------

select * from samples.T1 order by project, reported_month

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Calculate MoM Growth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Correctly get the previous month value

-- COMMAND ----------

select
  *,
  lag(hours_worked) over (partition by project order by reported_month) as prev_month_wrong,
  first_value(hours_worked) over (partition by project order by reported_month range between interval 1 month preceding and current row) as prev_month
from
  T1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Month Growth

-- COMMAND ----------

with cte as 
(
  select
    *,  
    first_value(hours_worked) over (partition by project order by reported_month range between interval 1 month preceding and current row) as prev_month
  from
    samples.T1
)
select
  *,
  cast(((hours_worked - prev_month) / prev_month) * 100.00 as decimal(5,2)) as month_growth
from
  cte
where
  project = 'Alpha'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculate Year-Over-Year Growth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate running total for current and previous year

-- COMMAND ----------

with cte as
(
  select
    *,  
    sum(hours_worked) over (partition by project, reported_year order by reported_month) as year_rt
  from
    samples.T1
)
select
  *,  
  last_value(year_rt) over (partition by project order by reported_month range between unbounded preceding and interval 1 year preceding) as prev_year_rt
from
  cte
where
  project = 'Alpha'
order by
  reported_month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate Growth

-- COMMAND ----------

with cte as
(
  select
    *,  
    sum(hours_worked) over (partition by project, reported_year order by reported_month) as year_rt
  from
    samples.T1
),
cte2 as
(
  select
    *,  
    last_value(year_rt) over (partition by project order by reported_month range between unbounded preceding and interval 1 year preceding) as prev_year_rt
  from
    cte
)
select
  *,
  cast(((year_rt - prev_year_rt) / prev_year_rt) * 100.00 as decimal(5,2)) as yoy_growth
from
  cte2
where
  project = 'Alpha' --and month(reported_month) = 6

-- COMMAND ----------

-

-- COMMAND ----------

