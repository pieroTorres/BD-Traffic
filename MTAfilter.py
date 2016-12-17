from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from datetime import datetime
import time

#Cuenta las comillas en la linea, si no se cumplen retorna falso

def countTab(x):
	c=0
	for i in x:
		for j in i:
			if (j=='\t'):
				c=c+1
	return c

#limpia las comillas
def cleanEl(x):
	r=''
	for i in x:
		for j in i:
			if (j!='"'):
				r=r+j
	return r	


#La Funcion formateara la fecha y hora a UNX

def forTim(x):
	fecha=datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S')
	x[2]=time.mktime(fecha.timetuple())	
	
	return x

#La funcion transformara los timestamps en bucket para separarlos por hora
def toHourBucket(x):
	horaEnSeg=60*60
	timeStamp=x[4]
	x[4]= timeStamp//horaEnSeg
	return x


###########################################################

#Inicio de Contexto Spark
conf = SparkConf().setMaster("spark://King:7077").setAppName("MTAFilter")

sc = SparkContext(conf = conf)

rdd = sc.textFile("hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08-01.txt")


#limpia ingresos repetidos
cleaned=rdd.distinct()


#descarta lineas corruptas que son aquellas que no cumplen las 26 comillas
discardCorrupted= cleaned.filter(lambda x: countTab(x)==10)


#va a mapear cada ingreso y le quitar las comillas
#myFile1 = discardCorrupted.map( lambda x: cleanEl(x))


#va a mapear cada ingreso y lo  partira en elementos a traves de la tabulacin
filteredT= discardCorrupted.map(lambda x:x.split('\t'))


#descarta las lineas que son headers
discardHeaders=filteredT.filter(lambda x : x[0]!="latitude" )

#formatea los tiempos a segundos en UNX
formatTime= discardHeaders.map(lambda x: forTim(x))

#solo mostrara los ingresos para una ruta
#getResultsFromOneRoute= formatTime.filter(lambda x: x[7]=='MTA NYCT_B63' )

#solo mostrara los ingresos para un bus
getResultsFromOneBus= formatTime.filter(lambda x: x[3]=='469')


#Comienzo de Tranformaciones para la Vista precomputada
#Formateo de Timestamp a Bucket de Hora
#HourBucket= NormalizeSpeedUndTime.map(lambda x: toHourBucket(x))


# mapear los resultados en llaves con dos valores para agruparlos luego. cada RDD tendra ((ID,BUCKET),SPEED)
#createKeys=HourBucket.map(lambda x: ((x[0],'h',x[4]),x[1]))


#Se agrupan los valores con todas las tuplas que le corresponden
#groupedKeys=createKeys.groupByKey()

#Se obtienen los valores maximos por hora
#maxPerHour= groupedKeys.mapValues(lambda x: max(x))

#Segunda parte Emision de granularidades para valores maximo por hora

#se genera un nuevo RDD con las granularidades por dia y el maximo por dia
#maxPerDay= maxPerHour.map(lambda (x,y): ((x[0],'d',x[2]//24),y)).groupByKey().mapValues(lambda x: max(x))

#se genera un nuevo RDD con las granularidades por semana y el maximo por semana
#maxPerWeek= maxPerDay.map(lambda (x,y): ((x[0],'s',x[2]//7),y)).groupByKey().mapValues(lambda x: max(x))

#se genera un nuevo RDD con las granularidades por mes y el maximo por mes

#REVISAR
#maxPerMonth= maxPerWeek.map(lambda (x,y): ((x[0],'m',x[2]//28),y)).groupByKey().mapValues(lambda x: max(x))


#allMaxRDD=maxPerHour.union(maxPerDay).union(maxPerWeek).union(maxPerMonth)

#allMaxRDD.saveAsTextFile("hdfs://King:9000/user/bdata/MHSPB_MasterDataset.txt")

#print("Emision de granularidades segun hora dia sem
for i in getResultsFromOneBus.take(100):
#for i in getResultsFromOneRoute.take(100):
        print(i)

