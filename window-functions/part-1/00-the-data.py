# Databricks notebook source
# MAGIC %md
# MAGIC # Project Timesheet Source Data

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
# MAGIC describe table samples.project_timesheet

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 	*
# MAGIC from
# MAGIC 	samples.project_timesheet

# COMMAND ----------

