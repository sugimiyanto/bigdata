import pyspark
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# create a session to Spark and its configuration
# utilizing all available cores
spark = SparkSession.builder.master("local[*]").appName("sum_balance_per_account_group").getOrCreate()

# load all csv files into a dataframe from the folder and use the header as column names
data = spark.read.format("csv").option("header","true").load("gs://YOUR_GCS_BUCKET/data")

# register the dataframe as a SQL temporary view
data.createOrReplaceTempView("data_bank")

# cast the account_balance field to long, to enable aggregation operation
data.withColumn('account_balance', data['account_balance'].cast("Long"))

# get the sum of account balance grouped by first two characters off account_id as account_group using Spark SQL Query
groupedAcc = spark.sql('SELECT SUBSTRING(account_id,1,2) AS account_group, SUM(account_balance) AS sum_group FROM data_bank GROUP BY account_group')

# cast the result to long, and sort ascending by account_group
castSum = groupedAcc.withColumn('sum_group', groupedAcc['sum_group'].cast("Long")).sort(col('account_group'))

# convert to regular data type (string) after parallel computation result for writing purpose
res = castSum.select('*').rdd.map(lambda line: line.__str__().replace('Row(account_group=','').replace('sum_group=','').replace(')','').replace('\'','').replace(' ',''))
result = res.collect()

# save the result to a file
file = open('sum_balance_per_account_group.csv','w')
file.write('account_group,sum\n')
for r in result:
    file.write(r.__str__()+'\n')

file.close()

# before running this program, Apache Spark should be installed properly
# use this command on linux terminal to launch this program: spark-submit spark_sum_balance_per_account_group.py