# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit , col

#Initializing spark session
spark = SparkSession.builder.appName("Quiz").getOrCreate()

# COMMAND ----------

#Ques-1: storing data in df
df = spark.read.options(inferSchema = 'True' , header = 'True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

#Ques-2 : Create a new Column in the DF for total makrs and let the total marks be 120

df = df.withColumn("Total marks", lit(120))
df.show()

# COMMAND ----------

#Ques-3 : Create a new column average to calculate the average marks of the student i.e. (marks/total marks)*100

df = df.withColumn("Average marks" , (col("marks")/col("total marks"))*100)
df.show()

# COMMAND ----------

#Ques-4 : Filter out all those students who achieved more than 80% marks in the OOP course and save it in a new DF.

df2 = df.filter((df.course == "OOP") & (col("average marks") > 80))
df2.show()

# COMMAND ----------

#Ques-5 : Print name and the marks of all the students from the above DFs

df.select("name","marks").show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


