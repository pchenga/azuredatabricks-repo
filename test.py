# Databricks notebook source
print('test')

# COMMAND ----------

df = spark.createDataFrame([(1,),(2,),(3,)], ["id"])

display(df)

# COMMAND ----------

# MAGIC %fs 
# MAGIC 
# MAGIC ls /databricks-datasets/test/

# COMMAND ----------

df.write.option('path', "/temp/test/").saveAsTable('external_table')

# COMMAND ----------

df.write.saveAsTable('managed_table')
