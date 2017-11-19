'''
Created on June 10, 2017

@author: mandar
'''
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp


#https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/sql/functions.html
#http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
#http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SQLContext
'''
export PYTHONPATH=/home/mandar/ProjectWorkspace/TSPProject:$PYTHONPATH

How to install spark in linux :

1) Create one folder inside linux
mkdir -p /home/<linux_user_name>/spark
e.g: 
mkdir -p /home/mandar/spark

where mandar : user which is present on your machine

2) Download spark from http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz inside spark folder
cd /home/mandar/spark
wget -c http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz

2)once download spark extract downloaded spark
tar -xvzf spark-2.0.2-bin-hadoop2.7.tgz

3)After extracting go inside spark package
cd spark-2.0.2

4)Now to use pyspark you need to copy 2 packages from spark to anaconda library(Reference to install anacodna :https://www.digitalocean.com/community/tutorials/how-to-install-the-anaconda-python-distribution-on-ubuntu-16-04)
cd /home/mandar/Software/spark-2.0.2/python/lib

We have to copy pyspark.zip,py4j-0.10.3-src.zip library from pyspark to anaconda site-package

before putting package into anaconda unzip both packages
unzip pyspark.zip
unzip py4j-0.10.3-src.zip

Now copy both folder pyspark,py4j-0.10.3-src and paste to your anaconda site-package
In my case anaconda install in following location /home/mandar/anaconda2/

cp -R pyspark /home/mandar/anaconda2/lib/python2.7/site-packages/
cp -R py4j-0.10.3-src /home/mandar/anaconda2/lib/python2.7/site-packages/

Once you copy both folder inside site-package now you can use any spark api in your pyspark code


How to run:
/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/YouTubeAnalysis.py  

'''

if __name__ == '__main__':
    
    
    conf = SparkConf().setAppName('You-Tube Analysis').setMaster('local[*]')
    
    # create spark context and sql context 
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)
    
    # read the input data file and create pandas dataframe
    you_tube_dataframe = sql_context.read.format("com.databricks.spark.csv")\
    .option("header", "true") \
    .option("inferschema", "true") \
    .option("delimiter", ",") \
    .option("mode", "DROPMALFORMED") \
    .load("/home/mandar/Downloads/YouTube-Spam-Collection-v1/Youtube05-Shakira.csv")
    
    you_tube_dataframe.printSchema()
    
    print "--------------------------Total number of record -------------------------"
    print you_tube_dataframe.count()
    
    you_tube_dataframe.select('CLASS').show(10)
    
    you_tube_dataframe.registerTempTable("you_tube_data")
    
       
    # Number of comment per user
    comment_per_user = "select count(*) as CLASS,AUTHOR as Authror_name from you_tube_data group by AUTHOR"
    comment_per_user_df = sql_context.sql(comment_per_user)
    comment_per_user_df.show()
    
    # select author data whose value =Leonel Hernandez
    select_author = "select *,CONCAT(YEAR(DATE), '-', MONTH(DATE),'-',DAY(DATE),DAYOFMONTH(DATE),'-',DAYOFYEAR(DATE))  as date_value from you_tube_data where AUTHOR = 'Sabina Pearson-Smith'"
    select_author_df = sql_context.sql(select_author)
    select_author_df.show()
    
    # select author data whose value =Leonel Hernandez
#     select_comment_per_date = "select count(*) as total_comment_count,CONCAT(YEAR(DATE), '-', MONTH(DATE),'-',DAY(DATE))  as comment_date from you_tube_data group by CONCAT(YEAR(DATE), '-', MONTH(DATE),'-',DAY(DATE))"
#     select_comment_per_date_df = sql_context.sql(select_comment_per_date)
#     select_comment_per_date_df.show()
    
    #     from_unixtime(Column ut, java.lang.String f)
    #2015-05-29T02:30:18.971000
    
    # convert different date string to date and time format
    date_you_tube_dataframe = you_tube_dataframe.select('COMMENT_ID','CLASS','DATE', from_unixtime(unix_timestamp('DATE', 'yyyy-MM-ddzHH:mm:ss.zzzzzz')).alias('date_value')) 
    date_you_tube_dataframe.show(20)
    date_you_tube_dataframe.printSchema()
    #write result dataframe to textfile
    date_you_tube_dataframe.write.format('json').save( 'date_result')
    
    #read json which is store in file
    json_dataframe = sql_context.read.format('json').load('date_result')
    json_dataframe.printSchema()
    print "Total count : " + str(json_dataframe.count)
    json_dataframe.show()
    
    
        
    
    
    
    
    
    
