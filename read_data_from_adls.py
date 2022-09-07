# Databricks notebook source
spark.conf.set("fs.azure.account.key.azuresaadlsgen2.dfs.core.windows.net", "4xpqX69HGsKN9a4rsSrEiBuqFGOmsRPxcwjQIcQ7UIZU0g3V8m2G2Do4rnRspRZSIk4mgLPPjusD+ASti368cQ==")

# COMMAND ----------

adls_path = "abfss://input@azuresaadlsgen2.dfs.core.windows.net/"
emp_file_name = "emp_data.csv"
emp_input_path = adls_path+emp_file_name

dept_input_path = adls_path + "dept_data.csv"

print(adls_path, emp_input_path, dept_input_path )


# COMMAND ----------

emp_df = spark.read.option('header', True).option('inferSchema', True).csv(emp_input_path)

display(emp_df)

emp_df.printSchema()
emp_df.show()

# COMMAND ----------

dept_df = spark.read.option('header', True).option('inferSchema', True).csv(dept_input_path)

display(dept_df)

dept_df.printSchema()

# COMMAND ----------

df1 = dept_df.filter("deptid = 101")
display(df1)

# COMMAND ----------

emp_df.printSchema()
emp_df.show()

display(emp_df)

# COMMAND ----------

# MAGIC %md 1.Spark SQL Way

# COMMAND ----------

#Register a temp table

emp_df.createOrReplaceTempView('emp_table')
dept_df.createOrReplaceTempView('dept_table')


# COMMAND ----------

# MAGIC %sql SELECT * FROM emp_table

# COMMAND ----------

df1 = spark.sql('SELECT * FROM emp_table')
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM emp_table --WHERE deptid=101

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dept_table

# COMMAND ----------

# DBTITLE 1,Join, select, derive a column and filter Transformation
# MAGIC %sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC e.eid,
# MAGIC e.ename,
# MAGIC e.sal,
# MAGIC d.deptname,
# MAGIC 'India' as country
# MAGIC FROM emp_table e 
# MAGIC LEFT JOIN dept_table d 
# MAGIC ON e.deptid = d.deptid 
# MAGIC WHERE d.deptname='D&A'

# COMMAND ----------

# DBTITLE 1,SaveAsTable 
output_df = spark.sql(
'''
SELECT 
e.eid,
e.ename,
e.sal,
d.deptname,
'India' as country
FROM emp_table e 
LEFT JOIN dept_table d 
ON e.deptid = d.deptid 
WHERE d.deptname='D&A'

'''
)

display(output_df)

# COMMAND ----------

# DBTITLE 1,Managed table (drops table and data)
output_df.write.format('csv').mode('overwrite').saveAsTable('emp_dept_final')

# COMMAND ----------

# DBTITLE 1,External table (drop a table structure and not a data) 
output_path = 'abfss://staging@azuresaadlsgen2.dfs.core.windows.net/emp_data'

output_df.write.mode('overwrite').option('path', output_path).saveAsTable('emp_dept_external')

# COMMAND ----------

# DBTITLE 1,External table 2 
output_path = 'abfss://staging@azuresaadlsgen2.dfs.core.windows.net/emp_data1'

output_df.write.mode('overwrite').option('path', output_path).save()

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE EXTERNAL TABLE emp_dept_external2
# MAGIC USING delta
# MAGIC LOCATION 'abfss://staging@azuresaadlsgen2.dfs.core.windows.net/emp_data1'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE emp_dept_external2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emp_dept_external2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY emp_dept_external2
