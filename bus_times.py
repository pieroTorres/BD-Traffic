
# coding: utf-8

# In[27]:


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
        return x

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

#error, cambiar orden del for para que evalue el ùltimo ìndice
#posibilidad de arreglo, encontrar si es el ultimo indice y verificar
#primero ver si es <= 35 y luego si es el ultimo o penultimo
def groupByPeriod(x):
    x=sorted(x)
    tiemposRecorrido=[]
    
    tsMinimo=x[0]
    tsMaximo=0
    cMediana=1
    periodoTs=[x[0]]
    for i in range(0,len(x)-1):
        if (i == len(x)-2):
            tsMaximo=x[i+1]
            periodoTs.append(x[i+1])
            cMediana+=1
            if (cMediana%2==0):
                    medianaTs=(periodoTs[(cMediana//2)-1] + periodoTs[cMediana//2])//2
            else:
                    medianaTs= periodoTs[cMediana//2]
                    
            tiemposRecorrido.append((medianaTs,tsMaximo-tsMinimo))
        else:    
            if (x[i+1]-x[i] <= 35):
                tsMaximo=x[i+1]
                periodoTs.append(x[i+1])
                cMediana+=1
                
            else:
                if (cMediana%2==0):
                    medianaTs=(periodoTs[(cMediana//2)-1] + periodoTs[cMediana//2])//2
                else:
                    medianaTs= periodoTs[cMediana//2]
                
                tiemposRecorrido.append((medianaTs,tsMaximo-tsMinimo))
                tsMinimo=x[i+1]
                tsMaximo=0
                cMediana=1
                periodoTs=[x[i+1]]
    return x    
    #return tiemposRecorrido


    



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
    formatTime= discardHeaders.map(lambda x: forTim(x))
	
	#formatea los tiempos a horas 
    #formatTime= discardHeaders.map(lambda x: hours(x))
    
    #devolver solo de un bus
    getResultsFromOneBus= formatTime.filter(lambda x: x[3]=='4215')
    
    getResultsFromOneRoute= getResultsFromOneBus.filter(lambda x: x[7]=='MTA NYCT_Q20B' )
    
    getResultsFromOneStop= getResultsFromOneRoute.filter(lambda x: x[10] =='MTA_505033') 
    
    #RDD con llaves
    keyedTuple= getResultsFromOneStop.map(lambda x: ((x[3],(x[7],x[10],x[5])),x[2]))
    
    groupedKeys=keyedTuple.groupByKey().mapValues(lambda x: groupByPeriod(x))
    
    #lambda x: (max(x),min(x),max(x)-min(x))
    #RDD
	#Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
	#classTuple= formatTime.map(lambda x: (x[2],x[11],x[3],x[5],x[10],x[7]))
    
    #CSV
    #Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
    #classTuple= formatTime.map(lambda x: str(x[2])+","+str(x[11])+","+x[3]+","+x[5]+","+x[10]+","+x[7])

    return groupedKeys


##########################################################################
#conf = SparkConf().setMaster("spark://King:7077").setAppName("MTAMain")

#sc = SparkContext(conf = conf)

rdd = sc.textFile('file:///home/cloudera/Downloads/MTA-Bus-Time_.2014-08-01.txt')

classTuple=mainFilter(rdd)





#Descomentar la siguiente linea para guardar en hdfs como csv
#classTuple.saveAsTextFile('hdfs:/user/cloudera/classification_buses.csv')
###########################################################################

#collectedTuple=classTuple.collect()

#classTuple.saveAsTextFile('hdfs:/user/bdata/classification_buses_test.txt')
#collectedTuple.saveAsTextFile('file:///home/cloudera/Downloads/classification_buses.csv')


for i in classTuple.take(50):
#for i in getResultsFromOneBus.take(3):
#for i in getResultsFromOneRoute.take(100):
      print(i)






# In[ ]:



