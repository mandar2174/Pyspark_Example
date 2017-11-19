'''
Created on Jun 11, 2017

@author: mandar
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

'''

How to run:
Open new terminal to start client for sending data

nc -lk 9999

/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/SparkStreamingDFExample.py localhost 9999   

'''

if __name__ == '__main__':
    spark = SparkSession \
    .builder \
    .appName("SparkStreamingDF") \
    .getOrCreate()
    
    # Read text from socket 
    socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
    
    socketDF.printSchema()

    # Split the lines into words
    words = socketDF.select(
       explode(
           split(socketDF.value, " ")
       ).alias("word")
    )
    
    # Generate running word count
    #lineCounts = words.groupBy("word").count()
    
    # Start running the query that prints the running counts to the console
#     query = lineCounts \
#         .writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start()
#         
    
    lineCounts = words.select("word")    
    # write output to file instead of console
    # file output sink supports only append mode 
    # format : can be "orc", "json", "csv", etc.
    # path : path of folder where to write result
    
    query = lineCounts \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/home/mandar/ProjectWorkspace/Example/resources/CheckPointSocket") \
    .option("path", "/home/mandar/ProjectWorkspace/Example/resources/SocketOutput"). \
    start()
    
#     print(query.lastProgress)
#     
#     print(query.status)
    
    query.awaitTermination()

