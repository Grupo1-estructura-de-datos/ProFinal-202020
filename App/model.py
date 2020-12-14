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
import config
from DISClib.ADT.graph import gr
from DISClib.ADT import map as m
from DISClib.ADT import orderedmap as om
from DISClib.ADT import indexminpq as iminpq
from DISClib.ADT import list as lt
from DISClib.ADT import queue as qe
from DISClib.DataStructures import listiterator as it
from DISClib.DataStructures import mapentry 
from DISClib.Algorithms.Graphs import scc
from DISClib.Algorithms.Graphs import dijsktra as djk
from DISClib.Utils import error as error
assert config
import datetime
import copy

"""
En este archivo definimos los TADs que vamos a usar y las operaciones
de creacion y consulta sobre las estructuras de datos.
"""

# -----------------------------------------------------
#                       API
# -----------------------------------------------------

def newAnalyzer():
    """ Inicializa el analizador
    Crea una lista vacia para guardar todos los crimenes
    Se crean indices (Maps) por los siguientes criterios:
    -Fechas
    Retorna el analizador inicializado.
    """
    analyzer = {'lst': None,
                'MaxPQTaxisAfiliados':None,
                'MaxPQServicios':None,
                'IDsTaxis':None,
                'NTaxis':0,
                'dateIndex':None
                }

    analyzer['lst'] = lt.newList("ARRAY_LIST", compareIds)
    analyzer['MaxPQTaxisAfiliados'] = iminpq.newIndexMinPQ(cmpfunction=compareRutas)
    analyzer['MaxPQServicios'] = iminpq.newIndexMinPQ(cmpfunction=compareRutas)
    analyzer["IDsTaxis"] = m.newMap(32233,100999001,'CHAINING',2,CompararCriterios)
    analyzer['dateIndex'] = om.newMap(omaptype='RBT', comparefunction=compareDates)
    return analyzer

# Funciones para agregar informacion

def addData(analyzer,data):
    if len(data["company"])<2:
        data["company"]=="Independent Owner"
    company = data["company"]
    idTaxi = data["taxi_id"]
    lt.addLast(analyzer["lst"],data)
    addCompany(analyzer, company, idTaxi)
    return analyzer

def addCompany(analyzer,company,idTaxi):
    if not iminpq.contains(analyzer["MaxPQTaxisAfiliados"],company):
            iminpq.insert(analyzer["MaxPQTaxisAfiliados"],company,999999)
            m.put(analyzer["IDsTaxis"],idTaxi,"")
            analyzer["NTaxis"] += 1
    else: 
            if not m.contains(analyzer["IDsTaxis"],idTaxi):
                m.put(analyzer["IDsTaxis"],idTaxi,"")
                val = m.get(analyzer["MaxPQTaxisAfiliados"]['qpMap'], company)
                valor = lt.getElement(analyzer["MaxPQTaxisAfiliados"]['elements'], val['value'])["index"]-1
                iminpq.decreaseKey(analyzer["MaxPQTaxisAfiliados"],company,valor)
                analyzer["NTaxis"] += 1
    if not iminpq.contains(analyzer["MaxPQServicios"],company):
            iminpq.insert(analyzer["MaxPQServicios"],company,999999)
    else: 
            val = m.get(analyzer["MaxPQServicios"]['qpMap'], company)
            valor = lt.getElement(analyzer["MaxPQServicios"]['elements'], val['value'])["index"]-1
            iminpq.decreaseKey(analyzer["MaxPQServicios"],company,valor)
    return analyzer

def addService(analyzer,company):
    pass

# ==============================
# Funciones de consulta
# ==============================

def dataSize(analyzer):
    return lt.size(analyzer["lst"])

def f3(analyzer,M,N):
    analyzer = copy.deepcopy(analyzer)
    texto = qe.newQueue()
    topAfiliados = qe.newQueue()
    topServicios = qe.newQueue()
    qe.enqueue(texto, "Hay " + str(analyzer["NTaxis"]) + " taxis para los servicios reportados")
    qe.enqueue(texto, "Hay " + str(iminpq.size(analyzer["MaxPQTaxisAfiliados"])) + " compañías con al menos un taxi inscrito")
    k = 0
    while k<M:
        kmin = iminpq.min(analyzer["MaxPQTaxisAfiliados"])
        kval = 1000000 - lt.getElement(analyzer["MaxPQTaxisAfiliados"]['elements'], m.get(analyzer["MaxPQTaxisAfiliados"]['qpMap'], kmin)['value'])["index"]
        qe.enqueue(topAfiliados,kmin + " - " + str(kval))
        iminpq.delMin(analyzer["MaxPQTaxisAfiliados"])
        k += 1
    l = 0
    while l<N:
        lmin = iminpq.min(analyzer["MaxPQServicios"])
        lval = 1000000 - lt.getElement(analyzer["MaxPQServicios"]['elements'], m.get(analyzer["MaxPQServicios"]['qpMap'], lmin)['value'])["index"]
        qe.enqueue(topServicios,lmin + " - " + str(lval))
        iminpq.delMin(analyzer["MaxPQServicios"])
        l += 1
    return (texto,topAfiliados,topServicios)

# ==============================
# Funciones Helper
# ==============================

def getDateTimeTaxiTrip(taxitrip):
    """
    Recibe la informacion de un servicio de taxi leido del archivo de datos (parametro).
    Retorna de forma separada la fecha (date) y el tiempo (time) del dato 'trip_start_timestamp'
    Los datos date se pueden comparar con <, >, <=, >=, ==, !=
    Los datos time se pueden comparar con <, >, <=, >=, ==, !=
    """
    tripstartdate = taxitrip['trip_start_timestamp']
    taxitripdatetime = datetime.datetime.strptime(tripstartdate, '%Y-%m-%dT%H:%M:%S.%f')
    return taxitripdatetime.date(), taxitripdatetime.time()

# ==============================
# Funciones de Comparacion
# ==============================

def compareIds(id1, id2):
    if (id1 == id2):
        return 0
    elif id1 > id2:
        return 1
    else:
        return -1

def CompararCriterios(keyname, criterio):
    authentry = mapentry.getKey(criterio)
    if (keyname == authentry):
        return 0
    elif (keyname > authentry):
        return 1
    else:
        return -1

def compareRutas(stop, keyvaluestop):
    stopcode = keyvaluestop['key']
    if (stop == stopcode):
        return 0
    elif (stop > stopcode):
        return 1
    else:
        return -1

def compareDates(date1, date2):
    if (date1 == date2):
        return 0
    elif (date1 > date2):
        return 1
    else:
        return -1