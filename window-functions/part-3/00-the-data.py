# Databricks notebook source
# MAGIC %md
# MAGIC # Project Timesheet Source Data

# COMMAND ----------

# MAGIC %md
# MAGIC *Access to csv data source has been configured at the cluster level so that you don't need to set the Azure Blob auth info in every workbook*

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

