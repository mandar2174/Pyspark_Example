'''
Created on Nov 4, 2017

@author: mandar
'''

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import from_unixtime, when
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.context import SQLContext
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from  pyspark.sql.functions import abs
from subprocess import call
import math
from collections import OrderedDict
from pyspark.sql.functions import monotonically_increasing_id

from time import time

# /usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/DataDifferenceSpark.py

if __name__ == '__main__':
    # Set Spark properties which will used to create sparkcontext
    conf = SparkConf().setAppName('SparkExample1').setMaster('local[*]')
    
    # create spark context and sql context 
    sc = SparkContext(conf=conf)
    hive_context = HiveContext(sc)
    
    # read the input data file and create pandas dataframe
    type_2_dataframe = hive_context.read.format("com.databricks.spark.csv")\
    .option("header", "false") \
    .option("inferschema", "true") \
    .option("delimiter", "|") \
    .option("mode", "DROPMALFORMED") \
    .load("/home/mandar/ProjectWorkspace/Example/resources/data_difference_input") \
    
    type_2_dataframe = type_2_dataframe .withColumnRenamed('_c0', 'date_value')
    
    # register dataframe to perform sql DSL    
    type_2_dataframe.registerTempTable("date_table")
    
    # get hash value for column first and second
    temp_table_query = "select from_unixtime(unix_timestamp(date_value, 'MMM dd yyyy hh:mm:ss.SSSaaa')) as convert_data,date_value from date_table"
    temp_table = hive_context.sql(temp_table_query)
    temp_table.show()
    
    window_func = Window().partitionBy().orderBy(temp_table["convert_data"])
    
    temp_table = temp_table.withColumn("lag_result",lag("convert_data").over(window_func))
    temp_table = temp_table.withColumn("lead_result",lead("convert_data").over(window_func))
    
    temp_table.show()
    temp_table = temp_table.na.fill('9999-12-31 00:00:00')
    temp_table.show()
    temp_table.printSchema()
    
    temp_table = temp_table.withColumn("date_difference_result",abs(datediff(temp_table["convert_data"], temp_table["lead_result"])))
    temp_table.show()
    
    
   
