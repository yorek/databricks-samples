# Databricks notebook source
# MAGIC %md
# MAGIC # After how many days from the start each project reached the 75% completed milestone?

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.dmstore1.blob.core.windows.net",
  "s8aN23JQ1EboPql5lx++0zQOyYrYC2EvT7NbgewR/8yAmQzpPfojntRWrCr4XOuonMowUUXsEzSxP11Jzd3kTg==")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists samples

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists samples.project_timesheet;
# MAGIC create table samples.project_timesheet
# MAGIC   using csv
# MAGIC   options (path "wasbs://sample-data@dmstore1.blob.core.windows.net/timesheet/sample_data.csv", header "true", mode "FAILFAST", inferschema "true")
# MAGIC ;
# MAGIC cache table samples.project_timesheet

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 	*
# MAGIC from
# MAGIC 	samples.project_timesheet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Calculate the running total

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s1;
# MAGIC create table s1 as
# MAGIC select
# MAGIC 	*,
# MAGIC 	sum(hours_worked) over (partition by project order by reported_on rows between unbounded preceding and current row) as hours_worked_rt
# MAGIC from
# MAGIC 	samples.project_timesheet
# MAGIC ;
# MAGIC select * from s1 order by project, reported_on

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate the total amount of worked hours

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s2;
# MAGIC create table s2 as
# MAGIC select
# MAGIC 	*,
# MAGIC 	SUM(hours_worked) over (partition by project) as total_project_hours
# MAGIC from
# MAGIC 	s1
# MAGIC ;
# MAGIC select * from s2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##  Calculate running percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s3;
# MAGIC create table s3 as
# MAGIC select
# MAGIC 	*,
# MAGIC 	cast(hours_worked_rt as decimal) / total_project_hours as completed_perc
# MAGIC from
# MAGIC 	s2
# MAGIC ;
# MAGIC select * from s3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add project starting date (assuming is the first time someone reported working on the project)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s4;
# MAGIC create table s4 as
# MAGIC select 
# MAGIC 	*,
# MAGIC 	FIRST_VALUE(reported_on) over (partition by project order by reported_on rows between unbounded preceding and current row) as project_started_on
# MAGIC from 
# MAGIC 	s3
# MAGIC ;
# MAGIC select * from s4;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find when 75% milestone was reached

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s5;
# MAGIC create table s5 as
# MAGIC with cte as
# MAGIC (
# MAGIC 	select
# MAGIC 		*
# MAGIC 	from
# MAGIC 		s4
# MAGIC 	where
# MAGIC 		completed_perc >= 0.75
# MAGIC )
# MAGIC select
# MAGIC 	*,
# MAGIC 	row_number() over (partition by project order by completed_perc) as rn
# MAGIC from
# MAGIC 	cte
# MAGIC   ;
# MAGIC select * from s5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC 	*, 
# MAGIC 	datediff(reported_on, project_started_on) as elapsed_days 
# MAGIC from 
# MAGIC 	s5 
# MAGIC where 
# MAGIC 	rn = 1

# COMMAND ----------

