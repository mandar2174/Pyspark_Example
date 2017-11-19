'''
Created on Jun 11, 2017

@author: mandar
'''
from pyspark.sql.functions import explode, split
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType

'''

How to run:

/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/FileBasedSinkSQLStreaming.py   

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
    
    # Generate Author-wise count
    lineCounts = csvDF.groupBy("AUTHOR").count()
    
    #start running the query and print the running result to file
    query = lineCounts \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/home/mandar/ProjectWorkspace/Example/resources/CheckPointSocket") \
    .option("path", "/home/mandar/ProjectWorkspace/Example/resources/LineCount"). \
    start()
    
    #print query.id()          # get the unique identifier of the running query that persists across restarts from checkpoint data

    #print query.runId()       # get the unique id of this run of the query, which will be generated at every start/restart

    #print query.name()        # get the name of the auto-generated or user-specified name

    #print query.explain()   # print detailed explanations of the query
    #print query.recentProgress()  # an array of the most recent progress updates for this query

    #query.lastProgress()    # the most recent progress update of this streaming query
    query.awaitTermination()

#Getting Error:
#    Traceback (most recent call last):
#   File "/home/mandar/ProjectWorkspace/Example/com/spark/example/FileBasedSinkSQLStreaming.py", line 47, in <module>
#     .option("path", "/home/mandar/ProjectWorkspace/Example/resources/LineCount"). \
#   File "/usr/local/spark-2.0.2/python/lib/pyspark.zip/pyspark/sql/streaming.py", line 1022, in start
#   File "/usr/local/spark-2.0.2/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py", line 1133, in __call__
#   File "/usr/local/spark-2.0.2/python/lib/pyspark.zip/pyspark/sql/utils.py", line 69, in deco
# pyspark.sql.utils.AnalysisException: u'Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets;;\nAggregate [AUTHOR#1], [AUTHOR#1, count(1) AS count#18L]\n+- StreamingRelation DataSource(org.apache.spark.sql.SparkSession@27cf4e,csv,List(),Some(StructType(StructField(COMMENT_ID,StringType,true), StructField(AUTHOR,StringType,true), StructField(DATE,StringType,true), StructField(CONTENT,StringType,true), StructField(CLASS,IntegerType,true))),List(),None,Map(sep -> ,, path -> /home/mandar/Downloads/YouTube-Spam-Collection-v1)), FileSource[/home/mandar/Downloads/YouTube-Spam-Collection-v1], [COMMENT_ID#0, AUTHOR#1, DATE#2, CONTENT#3, CLASS#4]\n'
# 17/06/17 19:32:30 INFO SparkContext: Invoking stop() from shutdown hook
# 17/06/17 19:32:30 INFO SparkUI: Stopped Spark web UI at http://192.168.0.103:4040
# 17/06/17 19:32:30 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
# 17/06/17 19:32:30 INFO MemoryStore: MemoryStore cleared
# 17/06/17 19:32:30 INFO BlockManager: BlockManager stopped
# 17/06/17 19:32:30 INFO BlockManagerMaster: BlockManagerMaster stopped
# 17/06/17 19:32:30 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
# 17/06/17 19:32:30 INFO SparkContext: Successfully stopped SparkContext
# 17/06/17 19:32:30 INFO ShutdownHookManager: Shutdown hook called
# 17/06/17 19:32:30 INFO ShutdownHookManager: Deleting directory /tmp/spark-91b15c1b-e3a5-4761-8203-8b987aba6120/pyspark-1ad0ad55-d1d2-48cb-b135-dc2c552ada70
# 17/06/17 19:32:30 INFO ShutdownHookManager: Deleting directory /tmp/spark-91b15c1b-e3a5-4761-8203-8b987aba6120
