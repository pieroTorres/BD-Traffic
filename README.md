# BD-Traffic
Descargar base de datos en el link compartido

#Pendientes
##Clasificación de Timestamps  [ ]
Al clasificar por segmentos, aparecen todos los timestamps del día para cada bus. Sin embargo, se deben separar en grupos que competen ese momento en el tiempo.
encontrar la manera de clasificar o separar timestamps en bus_times.py
ideas: MLib? granularizacion? bucketing?

solución: 
  * [x] ordenar valores de menor a mayor. (probar algoritmo TimSort)
  * [x] hacer cadenas o listas enlazadas con valores sucesivos con la condición que entre cada valor sólo haya 40 segundos de diferencia.
  * [x] devolver sólo el par de timestamp mediano y el tiempo de recorrido (max-min).
  * [ ] encontrar el bucket de periodo para el timestamp mediano
  * [ ] mapear cada valor de lista con sus llaves iniciales para tener la tupla {BusID,"segmento","periodo",TiempoRecorrido}
  * [ ] la tupla deberá ser genérica para poder moldear los resultados según petición

##Generar csv/RDD para demo [ ]
##Generar tablas de segmentos/paraderos por ruta(intentar encontrar orden de paraderos) [ ]
##Generación de Periodos según granularización temporal (media hora, hora, dia, año) [ ]
##Generación de Periodos según granularización física (segmento, ruta) [ ]
