# Databricks notebook source
# MAGIC %md
# MAGIC # How much each person has contributed to the project?

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

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 	*
# MAGIC from
# MAGIC 	samples.project_timesheet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the total amount of worked hours per project

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s1;
# MAGIC create table s1 as
# MAGIC select
# MAGIC 	*,
# MAGIC 	SUM(hours_worked) over (partition by project) as total_project_hours
# MAGIC from
# MAGIC 	samples.project_timesheet
# MAGIC ;
# MAGIC select * from s1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate the percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s2;
# MAGIC create table s2 as
# MAGIC select
# MAGIC 	*,
# MAGIC 	100 * (cast(hours_worked as decimal) / SUM(hours_worked) over (partition by project)) as contribution_perc
# MAGIC from
# MAGIC 	s1
# MAGIC ;
# MAGIC select * from s2;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find top 10 contributors for each project

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists s3;
# MAGIC create table s3 as
# MAGIC select
# MAGIC 	row_number() over (partition by project order by contribution_perc desc, reported_on asc) as contribution_rank,
# MAGIC 	*
# MAGIC from
# MAGIC 	s2
# MAGIC ;
# MAGIC select * from s3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 	*
# MAGIC from
# MAGIC 	s3
# MAGIC where
# MAGIC 	contribution_rank <= 10

# COMMAND ----------

