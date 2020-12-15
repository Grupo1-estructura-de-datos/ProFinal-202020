"""
 * Copyright 2020, Departamento de sistemas y Computación
 * Universidad de Los Andes
 *
 *
 * Desarrolado para el curso ISIS1225 - Estructuras de Datos y Algoritmos
 *
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * Contribución de:
 *
 * Dario Correal
 *
 """

import config as cf
from App import model
import csv
from DISClib.ADT import queue as qe

"""
El controlador se encarga de mediar entre la vista y el modelo.
Existen algunas operaciones en las que se necesita invocar
el modelo varias veces o integrar varias de las respuestas
del modelo en una sola respuesta.  Esta responsabilidad
recae sobre el controlador.
"""

# ___________________________________________________
#  Inicializacion del catalogo
# ___________________________________________________

def init():
    """
    Llama la funcion de inicializacion  del modelo.
    """
    # analyzer es utilizado para interactuar con el modelo
    analyzer = model.newAnalyzer()
    return analyzer

# ___________________________________________________
#  Funciones para la carga de datos y almacenamiento
#  de datos en los modelos
# ___________________________________________________

def loadData(analyzer, filename):
    """
    Carga los datos de los archivos CSV en el modelo
    """
    filecsv = cf.data_dir + filename
    input_file = csv.DictReader(open(filecsv,encoding="utf-8"),delimiter=",")
    for data in input_file:
        model.addData(analyzer, data)
    return analyzer

# ___________________________________________________
#  Funciones para consultas
# ___________________________________________________

def QueueMenorAMayor(QMay,QMen):
    if not qe.isEmpty(QMen):
        D = True
        while D:
            qe.enqueue(QMay,qe.dequeue(QMen))
            if qe.isEmpty(QMen): D = False
    return QMay

def dataSize(analyzer):
    return model.dataSize(analyzer)

def f3(analyzer,M,N):
    cola = qe.newQueue()
    colas = model.f3(analyzer,M,N)
    cola = QueueMenorAMayor(cola,colas[0])
    qe.enqueue(cola,"")
    qe.enqueue(cola,"")
    qe.enqueue(cola,"A continuación, top compañias por cantidad de taxis afiliados, con su número respectivo de taxis: ")
    cola = QueueMenorAMayor(cola,colas[1])
    qe.enqueue(cola,"")
    qe.enqueue(cola,"")
    qe.enqueue(cola,"A continuación, top compañias por cantidad de servicios ofrecidos, con su número respectivo de servicios ofrecidos: ")
    cola = QueueMenorAMayor(cola,colas[2])
    return cola

def f4(analyzer,Ñ,FechaI,FechaF=None):
    if FechaF==None: cola = model.ColaTopÑAlfaFecha(analyzer,FechaI,Ñ)
    else: cola = model.ColaTopÑAlfaFechas(analyzer,FechaI,FechaF,Ñ)
    return cola

def f5(cont, CAI, CAF, HI, HF):
    cola = qe.newQueue()
    minimo = model.CostoMinimo(cont, CAI, CAF, HI, HF)
    qe.enqueue(cola,"El costo mínimo de tiempo en segundos entre las dos rutas es de: " + str(minimo[1]))
    qe.enqueue(cola,"Este se da saliendo de la estación de origen a las: " + str(minimo[2]))
    qe.enqueue(cola,"La ruta de este camino es: : " + str(minimo[0]))
    return cola