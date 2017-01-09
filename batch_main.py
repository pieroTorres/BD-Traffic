
# coding: utf-8

# In[ ]:

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
import bus_times


# In[ ]:

def toCSV(x):
    y=str(x[0])
    for i in range(1,len(x)):
       y= y+","+str(x[i]) 
    return y
    
##########################################################################
#conf = SparkConf().setMaster("spark://King:7077").setAppName("MTAMain")

#sc = SparkContext(conf = conf)

rdd = sc.textFile('file:///home/cloudera/Downloads/MTA-Bus-Time_.2014-08-01.txt')

classTuple= bus_times.mainFilter(rdd)
halfHourBucket=classTuple.map(lambda x: bus_times.toHalfHourBucket(list(x)))

##########################################################################   
#CSV
#classTuple.map(lambda x: toCSV(x)).saveAsTextFile('hdfs:/user/bdata/demo_test.csv')
###########################################################################


###########################################################################
#RDD
#Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
#classTuple= formatTime.map(lambda x: (x[2],x[11],x[3],x[5],x[10],x[7]))

#classTuple.saveAsTextFile('hdfs:/user/bdata/classification_buses_test.txt')

#for i in classTuple.take(1000):
#for i in getResultsFromOneBus.take(3):
#for i in getResultsFromOneRoute.take(100):
#      print(i)


# In[ ]:

#print(classTuple.count())


# In[ ]:




# In[ ]:



