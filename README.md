# BD-Traffic
Descargar base de datos en el link compartido

#Pendientes
###Clasificación de Timestamps
Al clasificar por segmentos, aparecen todos los timestamps del día para cada bus. Sin embargo, se deben separar en grupos que competen ese momento en el tiempo.
encontrar la manera de clasificar o separar timestamps en bus_times.py
ideas: MLib? granularizacion? bucketing?
posible solución: 
  1) ordenar valores de menor a mayor. 
  2) hacer cadenas o listas enlazadas con valores sucesivos con la condición que entre cada valor sólo haya 30 segundos de diferencia.
  3)granularizar y devolver sólo el par de período y el tiempo de recorrido (max-min).
