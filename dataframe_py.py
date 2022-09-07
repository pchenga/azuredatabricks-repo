# Databricks notebook source
list = [
    (1,"A"),
    (2,"B"),
    (3,"C"),
    (4, "D"), #testing purpose
    (5, "Prakash")
    
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
