'''
Created on Jun 10, 2017

@author: mandar
'''

'''

How to run:
Open new terminal to start client for sending data

nc -lk 9999

/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/SQLStreamingExample.py localhost 9999   

'''


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


if __name__ == '__main__':
    spark = SparkSession \
    .builder \
    .appName("StructuredStreamingNetworkWordCount") \
    .getOrCreate()
    
    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    
    # Split the lines into words
    words = lines.select(
       explode(
           split(lines.value, " ")
       ).alias("word")
    )
    
    # Generate running word count
    lineCounts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = lineCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    query.awaitTermination()

