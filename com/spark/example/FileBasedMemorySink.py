'''
Created on Jun 11, 2017

@author: mandar
'''
'''

How to run:

/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/FileBasedMemorySink.py   

'''

import logging

from pyspark.sql.functions import explode, split
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType


if __name__ == '__main__':

    
    spark = SparkSession \
    .builder \
    .appName("FileBaseSQLStreaming") \
    .getOrCreate()
    
    # COMMENT_ID,AUTHOR,DATE,CONTENT,CLASS
    userSchema = StructType().add("COMMENT_ID", "string")\
    .add("AUTHOR", "string")\
    .add("DATE", "string") \
    .add("CONTENT", "string") \
    .add("CLASS", "integer") \
    
    #.option("maxFilesPerTrigger", 1) \
    csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .schema(userSchema) \
    .csv("/home/mandar/Downloads/YouTube-Spam-Collection-v1") 
    
    # Generate Author-wise count
    lineCounts = csvDF.groupBy("AUTHOR").count()
    
    #Use memory sink 
    #start running the query and print the running result to file
    query = lineCounts \
    .writeStream \
    .queryName("author_aggregates") \
    .outputMode("complete") \
    .format("memory") \
    .start()
    
    
    spark.sql("select * from author_aggregates").show()   # interactively query in-memory table
  
    query.awaitTermination()

