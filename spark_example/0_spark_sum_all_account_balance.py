import pyspark
import pyspark.sql
from pyspark.sql import SparkSession

# create a session to Spark and its configuration
# utilizing all available cores, could be written as local[n] where n is the number of core in our CPU. or local[*] utilize all cores explicitly
spark = SparkSession.builder.master("local[8]").appName("sum_all_account_balance").getOrCreate()

# load all csv files into a dataframe from the folder and use the header as column names
data = spark.read.format("csv").option("header","true").load("/Users/sugi/learning/bigdata/data") # change this path to yours

# register the dataframe as a SQL temporary view
data.createOrReplaceTempView("data_bank")

# get aggregate sum of all account_balance using Spark SQL query
sum_balance = spark.sql("SELECT SUM(account_balance) AS sum_balance FROM data_bank")

# collect the result
result = sum_balance.select('sum_balance').collect()[0][0]

# save the result to a file
file = open('0_spark_sum_all_account_balance_result.txt','w')
file.write(int(result).__str__())
file.close()

# command to use to launch this program: "spark-submit 0_spark_sum_all_account_balance.py"