from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from datetime import datetime
import time


# In[2]:

def countTab(x):
        c=0
        for i in x:
                for j in i:
                        if (j=='\t'):
                                c=c+1
        return c


def cleanEl(x):
        r=''
        for i in x:
                for j in i:
                        if (j!='"'):
                                r=r+j
        return r




def forTim(x):
        fecha=datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S')
        x[2]=time.mktime(fecha.timetuple())

#probar
def hours(x):
        fecha=datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S')
        x[2]=fecha.hour
	x.append(fecha.weekday())
        return x


def toHourBucket(x):
        horaEnSeg=60*60
        timeStamp=x[4]
        x[4]= timeStamp//horaEnSeg
        return x



def mainFilter(rdd):
	#limpia ingresos repetidos
	cleaned=rdd.distinct()


	#descarta lineas corruptas que son aquellas que no cumplen las 26 comillas
	discardCorrupted= cleaned.filter(lambda x: countTab(x)==10)

	#va a mapear cada ingreso y lo  partira en elementos a traves de la tabulacin
	filteredT= discardCorrupted.map(lambda x:x.split('\t'))

	#descarta las lineas que son headers
	discardHeaders=filteredT.filter(lambda x : x[0]!="latitude" )

	#formatea los tiempos a segundos en UNX
	#formatTime= discardHeaders.map(lambda x: forTim(x))
	
	#formatea los tiempos a horas 
	formatTime= discardHeaders.map(lambda x: hours(x))


	#Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
	classTuple= formatTime.map(lambda x: (x[2],x[11],x[3],x[5],x[10],x[7]))

	return classTuple


##########################################################################
#conf = SparkConf().setMaster("spark://King:7077").setAppName("MTAMain")

#sc = SparkContext(conf = conf)

#rdd = sc.textFile('hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08-01.txt')


###########################################################################






















#collectedTuple=classTuple.collect()

#classTuple.saveAsTextFile('hdfs:/user/cloudera/wordcount/classification_buses.csv')
#classTuple.saveAsTextFile('file:///home/cloudera/Downloads/classification_buses.csv')
#collectedTuple.saveAsTextFile('file:///home/cloudera/Downloads/classification_buses.csv')


# In[15]:

#for i in classTuple.take(3):
#for i in getResultsFromOneBus.take(3):
#for i in getResultsFromOneRoute.take(100):
#        print(i)


# In[ ]:



