# Databricks notebook source
# MAGIC %md
# MAGIC # Spark provides complete support to OVER clause

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.dmstore1.blob.core.windows.net",
  "s8aN23JQ1EboPql5lx++0zQOyYrYC2EvT7NbgewR/8yAmQzpPfojntRWrCr4XOuonMowUUXsEzSxP11Jzd3kTg==")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 	*,
# MAGIC     sum(hours_worked) over (partition by project order by reported_on) as tot,
# MAGIC     count(hours_worked) over (partition by project order by reported_on) as rows_touched
# MAGIC from
# MAGIC 	samples.project_timesheet

# COMMAND ----------

# MAGIC %md
# MAGIC ## RANGE framing with support to INTERVALS
# MAGIC 
# MAGIC Full support to the RANGE frame is an amazing feature since it allows to express complex scenarios like "sum all the data for the 5 days preceeding the current row" compared to the "sum all data for the 5 preceeding rows" available with the ROW frame, that is useless if you don't have data for ALL days

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 	*,
# MAGIC     sum(hours_worked) over (partition by project order by reported_on range between interval '5' days preceding and current row) as tot,
# MAGIC     count(hours_worked) over (partition by project order by reported_on range between interval '5' days preceding and current row) as rows_touched
# MAGIC from
# MAGIC 	samples.project_timesheet
# MAGIC order by
# MAGIC     project

# COMMAND ----------

