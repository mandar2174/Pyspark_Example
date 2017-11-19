'''
Created on June 10, 2017

@author: mandar
'''
# https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/sql/functions.html
# http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
# http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SQLContext
# https://docs.databricks.com/spark/latest/spark-sql/udf-in-python.html
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
/usr/local/spark-2.0.2/bin/spark-submit /home/mandar/ProjectWorkspace/Example/com/spark/example/Type2_Example.py  

'''

import hashlib

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import from_unixtime, when
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StringType


def get_hash_value(column_value):
    return hashlib.md5(str(column_value)).hexdigest() 

def isNull(column_value):
    if column_value is None:
        return ''
    else:
        return column_value

if __name__ == '__main__':
    
    
    conf = SparkConf().setAppName('Type2_Example').setMaster('local[*]')
    
    # create spark context and sql context 
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)
    
    # read the input data file and create pandas dataframe
    type_2_dataframe = sql_context.read.format("com.databricks.spark.csv")\
    .option("header", "false") \
    .option("inferschema", "true") \
    .option("delimiter", "|") \
    .option("mode", "DROPMALFORMED") \
    .load("/home/mandar/ProjectWorkspace/Example/com/spark/example/type2_data")
    
    type_2_dataframe.printSchema()
    
    # print "--------------------------Total number of record -------------------------"
    # print type_2_dataframe.count()
    
    type_2_dataframe.show(10)
    
    # replace null value with space
    # type_2_dataframe.select('_c3').na.fill('').show()
    
    # register udf to calculate hash value of column using md5 algorithm(Using SQL DSL)
    sql_context.udf.register("get_hash_value", get_hash_value, StringType())
    
    # register udf to check column value is null not 
    sql_context.udf.register("is_null", isNull, StringType())
    
    # register dataframe to perform sql DSL    
    type_2_dataframe.registerTempTable("type2_data")
    
    # remove null value from column
    #null_value_query = "select _c3,is_null(_c3) as new_c3 from type2_data"
    #null_value_query_df = sql_context.sql(null_value_query)
    #null_value_query_df.show()
    
    
    # get hash value for column first and second
    hash_record_query = "select get_hash_value(CONCAT(_c1,_c2)) as record_hashvalue,*,get_hash_value(CONCAT(_c0,_c1,_c2,_c3,_c4,_c8,_c9,_c10)) as record_hash_row,from_unixtime(unix_timestamp(_c5, 'MMM dd yyyy hh:mm:ss.SSSaaa')) as init_effective_date,from_unixtime(unix_timestamp(_c6, 'MMM dd yyyy hh:mm:ss.SSSaaa')) as init_expiration_date from type2_data"
    hash_record_query_df = sql_context.sql(hash_record_query)
    hash_record_query_df.show()
    
    #populate effective date based on init_expiration_date and init_expiration_date and init_effective_date
    effective_date_df = hash_record_query_df. withColumn("effective_date", when(hash_record_query_df.init_expiration_date == '1991-01-01 00:00:00' , hash_record_query_df.init_effective_date).otherwise(hash_record_query_df.init_expiration_date))
    effective_date_df.show()
    
    # register dataframe to perform sql DSL    
    effective_date_df.registerTempTable("date_table")
    
    
    
       
   
        
    
    
    
    
    
    
