'''
Created on Jun 11, 2017

@author: mandar
'''
from pyspark.sql.functions import explode, split
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery

'''

How to run:

/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/FileBaseSQLStreaming.py   

'''

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
    
    csvDF = spark \
    .readStream \
    .option("sep", ",") \
    .schema(userSchema) \
    .csv("/home/mandar/Downloads/YouTube-Spam-Collection-v1") 
    
    print csvDF.isStreaming
    
    # Generate Author-wise count
    lineCounts = csvDF.groupBy("AUTHOR").count()
    
    # Start running the query that prints the running counts to the console
    
    query = lineCounts.writeStream \
        .option("maxFilesPerTrigger", 1) \
        .outputMode("complete") \
        .format("console") \
        .start()
        
    print query.isActive
    
    print query.name
    
    query.awaitTermination()


#https://www.youtube.com/watch?v=rl8dIzTpxrI&feature=youtu.be
#https://www.youtube.com/watch?v=1a4pgYzeFwE
#https://spark.apache.org/docs/2.0.2/api/python/_modules/pyspark/sql/streaming.html