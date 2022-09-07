# Databricks notebook source
sales_header_df = spark.table('default.saleslt_salesorderheader_csv')

sales_header_df.printSchema()
sales_header_df.show()

# COMMAND ----------

display(sales_header_df)

# COMMAND ----------

sales_order_detail_df = spark.table('saleslt_salesorderdetail_csv')

display(sales_order_detail_df)

# COMMAND ----------

# MAGIC %md DataFrame Way

# COMMAND ----------

#join_df = sales_header_df.join(sales_order_detail_df, sales_header_df.SalesOrderID==sales_order_detail_df.SalesOrderID  , 'inner')

join_df = sales_header_df.join(sales_order_detail_df, "SalesOrderID"  , 'inner') #left_semi, left_anti
#join_df = sales_header_df.join(sales_order_detail_df, "SalesOrderID"  , 'left_semi') # inner join  (left tabe )
#join_df = sales_header_df.join(sales_order_detail_df, "SalesOrderID"  , 'left_anti') # (non matching records from left tabe )
display(join_df)

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,filter
#filtered_df = join_df.filter("SalesOrderID = 71780") #expression based 

#filtered_df = join_df.filter(join_df.SalesOrderID == 71780)
filtered_df = join_df.filter(col("SalesOrderID") != 71780)
display(filtered_df)

# COMMAND ----------

# DBTITLE 1,Select 
select_df = filtered_df.select('SalesOrderID',
                   'SalesOrderDetailID',
                   'OrderQty',
                   'ProductID',
                   'UnitPrice'
                  )

display(select_df)

# COMMAND ----------

# DBTITLE 1,Group By
select_df.groupBy("SalesOrderID").count().show()

# COMMAND ----------

final_df = select_df.groupBy("SalesOrderID").agg(
  sum("OrderQty").alias('total_qty'), 
  sum("UnitPrice").alias('total_price')

)

display(final_df)

# COMMAND ----------

# DBTITLE 1,SQL Way (on table)
select_df.createOrReplaceTempView("sales_table")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT
# MAGIC SalesOrderID, 
# MAGIC sum(OrderQty) as total_qty,
# MAGIC sum(UnitPrice) as total_price
# MAGIC FROM sales_table GROUP BY SalesOrderID

# COMMAND ----------

# DBTITLE 1,Per each orderId max unit price
# MAGIC %sql 
# MAGIC 
# MAGIC 
# MAGIC SELECT SalesOrderID, max(UnitPrice) as max_price FROM sales_table GROUP BY SalesOrderID

# COMMAND ----------

#row_number() 1, 2, 3, 4
#rank()  --  
#dense_rank() --

'''
row_number()
1000 1
1000 2
1000 3
900  4

rank()
1000 1
1000 1
1000 1
900 4

dense_rank()
1000 1
1000 1
1000 1
900 2 '''

# COMMAND ----------

# DBTITLE 1,SQL way (second highest unit price per order )
# MAGIC %sql 
# MAGIC select * from (
# MAGIC SELECT *, dense_rank() over(partition by SalesOrderID order by UnitPrice desc) rn  FROM sales_table
# MAGIC ) a 
# MAGIC where rn=3 --and salesOrderID=71782

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Dataframe way (second highest unit price per order )
a = 10 

window_spec = Window.partitionBy("SalesOrderID").orderBy(col("UnitPrice").desc())

rank_df = select_df.withColumn("rn", dense_rank().over(window_spec))

final_df = rank_df.filter("rn =2")

final_df = final_df.withColumnRenamed("SalesOrderDetailID", "OrderItemID")
display(final_df.drop("rn"))



