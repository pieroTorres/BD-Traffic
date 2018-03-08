
# coding: utf-8


from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.types import *
import bus_times


# In[ ]:

def toHalfHourBucket(x):
        hora= (x[2] // (60*60))%24
        minuto= (x[2] //60)%60
        if (minuto < 30):
            minuto=0.0
        else:
            minuto=0.5
        valor= hora+minuto
        x[2]=valor
        return x



#def toCSV(x):
#    y=str(x[0])
#    for i in range(1,len(x)):
#       y= y+","+str(x[i]) 
#    return y



def streamMainFilter(dstream):
	splitted_ds= dstream.map(lambda x:x.split('\t'))	
	discard_headers=splitted_ds.filter(lambda x : x[0]!="latitude")	
	format_time=discard_headers.map(lambda x: bus_times.forTim(x))
	hh_bucket=format_time.map(lambda x: toHalfHourBucket(x))
	selected_ds= hh_bucket.map(lambda x:(x[0],x[1],x[2],x[3],x[5],x[7],x[10]))

	return selected_ds

    
##########################################################################
#conf = SparkConf().setMaster("spark://King:7077").setAppName("batch_Main")
conf = SparkConf().setAppName("stream_Main")

sc = SparkContext(conf = conf)
sc.addPyFile("/home/bdata/sparkPrograms/BD-Traffic/bus_times.py")

ssc = StreamingContext(sc, 1)


#Dstream takes the day that is going to have predicted times (test), in this case it takes from the directory
dstream =ssc.textFileStream('hdfs://King:9000/user/bdata/mta_data_tests/')



#IMPORTANT: In order to dstream read, directory must be empty then cp file into directory while spark program is submitted
#streamMainFilter(dstream).pprint()


streamMainFilter(dstream).saveAsTextFiles('hdfs://King:9000/user/bdata/streamTests/streamTest','txt')


#starts streaming

ssc.start()
ssc.awaitTermination()



##########################################################################   
#CSV
#classTuple.map(lambda x: toCSV(x)).saveAsTextFile('hdfs:/user/bdata/demo_test.csv')

#halfHourBucket.map(lambda x: toCSV(x)).saveAsTextFile('hdfs://King:9000/user/bdata/demo_test_halloween.csv')


###########################################################################
##RDD TO DATAFRAME
#sparkMach_session = SparkSession.builder \
#.master("spark://King:7077") \
#.appName("sparkmach") \
#.config("spark.driver.allowMultipleContexts", "true") \
#.getOrCreate()

#bucket_schema= StructType([StructField("bus_id",StringType(), True),StructField("route_id",StringType(), True),StructField("next_stop_id",StringType(), True),StructField("direction",StringType(), True),StructField("half_hour_bucket",StringType(), True),StructField("class",StringType(), True) ])

#times_df= sparkMach_session.createDataFrame(halfHourBucket, bucket_schema)

#times_df.show()
###########################################################################


