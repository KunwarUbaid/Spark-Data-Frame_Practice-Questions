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

#Ques-6 : Display all the unique rows for age , gender and course columns

df3 = df.select("age","gender","course")
df3.distinct().show()


# COMMAND ----------
#Ques-7 : Group the data based on gender = male and then further group the data based on course and the
#gender and find total enrollements in each course and then return those courses where enrollment is more than 85.

df.filter(col("gender") == "Male").groupBy("course","gender").agg(count("*").alias("Total_Enrollments")).filter(col("Total_Enrollments") > 85).show()

#Alternate way
#df5 = df.filter(df.gender == "Male").groupBy("course","gender").agg(count("*").alias("total_enroll"))
#df5 = df5.filter(col("total_enroll") > 85)
#df5.show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


