# Databricks notebook source
#Reserved words or keywords used in python
'''if 
elif
else
for 
while
do
break
continue
class 
None
True
False
try
except
lambda '''


# COMMAND ----------

# DBTITLE 1,Data types
n1 = 10.0


type(n1)

# COMMAND ----------

b = True #False

type(b)

# COMMAND ----------

c= None

type(c)

# COMMAND ----------

# DBTITLE 1,String
s1 = 'Learning Python'
s2 = "Learning python"
s3 = '''Learning python
Learning python line2
'''

#type(s1)
#type(s2)
#type(s3)
print(s3)


# COMMAND ----------

# DBTITLE 1,Collection or Datastructures

list = [1, 2, 3, 4, 5]

type(list)

list.append(6)

# COMMAND ----------

for n in list:
    print(n)

# COMMAND ----------

list1 = range(1, 11)

for n in list1:
    print(n)

# COMMAND ----------

# DBTITLE 1,Access an element
first = list[0]
print(first)

# COMMAND ----------

# DBTITLE 1,Tuples
s1 = (1,"Prakash", "prakash@gmail.com")

print(s1[0])
print(s1[1])

# COMMAND ----------

list = [
    
    (1,"Prakash", "prakash@gmail.com"),
    (2,"Prakash", "prakash@gmail.com"),
    (3,"Prakash", "prakash@gmail.com")
    
]

x= 10

df = spark.createDataFrame(list, ["id", "name", "email"] )

display(df)

df.createOrReplaceTempView("student")
print(type(df))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM student

# COMMAND ----------

# DBTITLE 1,Set
nums = {1,1,2,3,4,4,5}

for n in nums:
    print(n)

# COMMAND ----------

# DBTITLE 1,Dictionary
emp = {
    
    "eid" : 1,
    "ename" : "Prakash"
    
}

# COMMAND ----------

for k,v in emp.items():
    print(k,v)
