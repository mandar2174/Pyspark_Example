'''
Created on Apr 3, 2017

@author: mandar
'''
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import when

'''
export PYTHONPATH=/home/mandar/ProjectWorkspace/TSPProject:$PYTHONPATH

How to run:
/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/TSPProject/src/main/controller/CaseExample.py  

'''


def create_data_from_csv(file_path=None, delimited=',', sql_context=None):
    
    data_frame = sql_context.read.format("com.databricks.spark.csv")\
    .option("header", "false") \
    .option("inferschema", "true") \
    .option("delimiter", delimited) \
    .option("mode", "DROPMALFORMED") \
    .load("file://" + file_path)
    return data_frame


if __name__ == '__main__':
    conf = SparkConf().setAppName('CaseExample').setMaster('local[*]')
    
    # create spark context and sql context 
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)
    
    # read the input data file and create spark  dataframe
    data_frame = create_data_from_csv(file_path='/home/mandar/ProjectWorkspace/TSPProject/src/resources/NCAInputFiles/Sample.txt', delimited='\t', sql_context=sql_context)
    
    data_frame = data_frame.withColumnRenamed("_c0", "Name").withColumnRenamed("_c1", "Age").withColumnRenamed("_c2", "Country").withColumnRenamed("_c3", "Salary").withColumnRenamed("_c4", "Lanaguage")
    data_frame.show()
    
    # Simple case stmt
    data_frame.withColumn("case_column", when(data_frame.Name == "ABC", "1").otherwise("0")).show()
    
    # complex case stmt with multiple case 
    data_frame.withColumn("case_column", when(data_frame.Age >= 27 , when(data_frame.Salary > 70000, "Applicable for visa").otherwise("Not applicable for visa")).otherwise("No applicable for visa")).show()
    
    #case with between condition
    data_frame.withColumn("case_column", when(data_frame.Age >= 27 , when(data_frame.Salary > 70000, "Applicable for visa").otherwise("Not applicable for visa")).otherwise("No applicable for visa")).show()
    
    
