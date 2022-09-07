# Databricks notebook source
list = [
    (1,"A"),
    (2,"B"),
    (3,"C")
]

# COMMAND ----------

for k,v in list:
    print(k, v)

# COMMAND ----------

df = spark.createDataFrame(list, ["id", "name"])

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()
