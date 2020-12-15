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
from DISClib.DataStructures import mapentry as me
from DISClib.ADT import orderedmap as om
from DISClib.ADT import indexminpq as iminpq
from DISClib.ADT import list as lt
from DISClib.ADT import queue as qe
from DISClib.DataStructures import listiterator as it
from DISClib.DataStructures import mapentry 
from DISClib.Algorithms.Sorting import mergesort as MeSo
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
                'NTaxis':None,
                'dateIndex':None,
                'CAs-HorasDeSalida':None,
                'graph':None
                }

    analyzer['lst'] = lt.newList("ARRAY_LIST", compareIds)
    analyzer['MaxPQTaxisAfiliados'] = iminpq.newIndexMinPQ(cmpfunction=compareRutas)
    analyzer['MaxPQServicios'] = iminpq.newIndexMinPQ(cmpfunction=compareRutas)
    analyzer["IDsTaxis"] = m.newMap(32233,100999001,'CHAINING',2,CompararCriterios)
    analyzer["NTaxis"] = 0
    analyzer['dateIndex'] = om.newMap(omaptype='RBT', comparefunction=compareDates)
    analyzer["CAs-HorasDeSalida"] = m.newMap(197,100999001,'CHAINING',2,CompararCriterios)
    analyzer['graph'] = gr.newGraph(datastructure='ADJ_LIST',directed=True,size=200,comparefunction=compareStations)
    analyzer['rutas'] = m.newMap(numelements=37813,maptype='PROBING',comparefunction=compareRutas)
    return analyzer

# Funciones para agregar informacion

def addData(analyzer,data):
    if len(data["company"])<2:
        data["company"]=="Independent Owner"
    company = data["company"]
    idTaxi = data["taxi_id"]
    instante = getDateTimeTaxiTrip(data["trip_start_timestamp"])
    fecha = instante[0]
    hora = instante[1]
    lt.addLast(analyzer["lst"],data)
    addCompany(analyzer, company, idTaxi)
    millasRecorridas = data["trip_miles"]
    dineroRecibido = data["trip_total"]
    origin = data['pickup_community_area']
    destination = data['dropoff_community_area']
    duration = data['trip_seconds']
    if len(millasRecorridas)>0 and len(dineroRecibido)>0:
        millasRecorridas = float(millasRecorridas)
        dineroRecibido = float(dineroRecibido)
        if millasRecorridas>0.0 and dineroRecibido>0.0: updateDateIndex(analyzer,idTaxi,fecha,millasRecorridas,dineroRecibido)
    if len(duration)>0: 
        duration = float(duration)
        if origin!=destination:
            addStation(analyzer,origin,hora)
            addStation(analyzer,destination,None)
            addConnection(analyzer, origin, destination, duration,hora)
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
                valor = lt.getElement(analyzer["MaxPQTaxisAfiliados"]['elements'], val['value'])["index"] - 1
                iminpq.decreaseKey(analyzer["MaxPQTaxisAfiliados"],company,valor)
                analyzer["NTaxis"] += 1
    if not iminpq.contains(analyzer["MaxPQServicios"],company):
            iminpq.insert(analyzer["MaxPQServicios"],company,999999)
    else: 
            val = m.get(analyzer["MaxPQServicios"]['qpMap'], company)
            valor = lt.getElement(analyzer["MaxPQServicios"]['elements'], val['value'])["index"] - 1
            iminpq.decreaseKey(analyzer["MaxPQServicios"],company,valor)
    return analyzer

def updateDateIndex(analyzer,idTaxi,fecha,millasRecorridas,dineroRecibido):
    entry = om.get(analyzer["dateIndex"], fecha)
    if entry is None:
        dataentry = m.newMap(numelements=30,
                             maptype='PROBING',
                             comparefunction=compareO)
        om.put(analyzer["dateIndex"], fecha, dataentry)
    else:
        dataentry = me.getValue(entry)
    addDateIndex(dataentry,idTaxi,millasRecorridas,dineroRecibido)
    return analyzer

def addDateIndex(dataentry,idTaxi,millasRecorridas,dineroRecibido):
    booleano = m.get(dataentry, idTaxi)
    if booleano==None:
        m.put(dataentry,idTaxi,{"totalMillas":millasRecorridas,"totalDinero":dineroRecibido,"servicios":1})
    else:
        valores = me.getValue(booleano)
        millasNuevas = valores["totalMillas"] + millasRecorridas
        dineroNuevo = valores["totalDinero"] + dineroRecibido
        serviciosNuevo = valores["servicios"] + 1
        m.remove(dataentry,idTaxi)
        m.put(dataentry,idTaxi,{"totalMillas":millasNuevas,"totalDinero":dineroNuevo,"servicios":serviciosNuevo})
    return dataentry

def addStation(analyzer,stationid,hora):
    if hora!=None:
        if not gr.containsVertex(analyzer["graph"], stationid + "-" + str(hora)): gr.insertVertex(analyzer["graph"], stationid + "-" + str(hora))
        if m.contains(analyzer["CAs-HorasDeSalida"],stationid):
            llaveValor = m.get(analyzer["CAs-HorasDeSalida"],stationid)
            lst = me.getValue(llaveValor)
            if lt.isPresent(lst,hora)==0: addHoraSalida(analyzer,stationid,hora)
        else:
            addStationSalida(analyzer,stationid)
            addHoraSalida(analyzer,stationid,hora)
    if not gr.containsVertex(analyzer["graph"], stationid): gr.insertVertex(analyzer["graph"], stationid)
    return analyzer

def addConnection(analyzer,origin,destination,duration,hora):
    N=addNRutas(analyzer["rutas"],origin,destination)
    edge = gr.getEdge(analyzer["graph"], origin, destination)
    if edge is None:
        gr.addEdge(analyzer["graph"], origin, destination, duration)
    else:
        EdgeWeightPonderado=edge["weight"]*(N-1)
        promedio = (EdgeWeightPonderado+duration)/N
        edge["weight"]=promedio
    edge2 = gr.getEdge(analyzer["graph"], origin + "-" + str(hora), destination)
    if edge2 is None:
        gr.addEdge(analyzer["graph"], origin + "-" + str(hora), destination, duration)
    else:
        EdgeWeightPonderado=edge2["weight"]*(N-1)
        promedio = (EdgeWeightPonderado+duration)/N
        edge2["weight"]=promedio
    return analyzer

def addNRutas(citibike,origin,destination):
    string = str(origin) + "-->" + str(destination)
    booleano = m.contains(citibike,string)
    if booleano:
        N = m.get(citibike,string)["value"]+1
        m.put(citibike,string,N)
        return N
    else: 
        m.put(citibike,string,1)
        return 1

def addStationSalida(analyzer,stationid):
    m.put(analyzer["CAs-HorasDeSalida"],stationid,lt.newList(cmpfunction=compareR))

def addHoraSalida(analyzer,stationid,hora):
    llaveValor = m.get(analyzer["CAs-HorasDeSalida"],stationid)
    lst = me.getValue(llaveValor)
    lt.addLast(lst,hora)

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

def ColaTopÑAlfaFecha(analyzer,FechaI,Ñ):
    cola = qe.newQueue()
    listaAlfas = topÑAlfaFecha(analyzer,FechaI)
    MeSo.mergesort(listaAlfas,compararAlfas)
    qe.enqueue(cola,"A continuación, los " + str(Ñ) + " taxis con más puntos para la fecha dada.")
    qe.enqueue(cola,"")
    return funcionQueue(listaAlfas,Ñ,cola)

def ColaTopÑAlfaFechas(analyzer,FechaI,FechaF,Ñ):
    mapaAlfaFinales = m.newMap(32233,100999001,'CHAINING',2,CompararCriterios)
    listaAlfasFinal = lt.newList()
    cola = qe.newQueue()
    FechaI = om.ceiling(analyzer["dateIndex"],FechaI)
    FechaF = om.floor(analyzer["dateIndex"],FechaF)
    qe.enqueue(cola,"hola")
    fechas = om.keys(analyzer["dateIndex"],FechaI,FechaF)
    iteradorFechas = it.newIterator(fechas)
    C = True
    while C:
        fecha = it.next(iteradorFechas)
        listaAlfas = topÑAlfaFecha(analyzer,fecha)
        concatenarAlfas(listaAlfas,mapaAlfaFinales)
        if not it.hasNext(iteradorFechas): C = False
    listaLlaves = m.keySet(mapaAlfaFinales)
    iteradorLlaves = it.newIterator(listaLlaves)
    Z = True
    while Z:
        llave = it.next(iteradorLlaves)
        pareja = m.get(mapaAlfaFinales,llave)
        diccionario = me.getValue(pareja)
        lt.addLast(listaAlfasFinal,{"Taxi":llave,"Alfa":(diccionario["Alfa"]/diccionario["N"])+1})
        if not it.hasNext(iteradorLlaves): Z = False
    MeSo.mergesort(listaAlfasFinal,compararAlfas)
    qe.enqueue(cola,"A continuación, los " + str(Ñ) + " taxis con más puntos para el rango de fechas dado.")
    qe.enqueue(cola,"")
    return funcionQueue(listaAlfasFinal,Ñ,cola)

def CostoMinimo(cont, CAI, CAF, HI, HF):
    pareja = m.get(cont['CAs-HorasDeSalida'],CAI)
    listaHoras = me.getValue(pareja)
    listaHorasFinal = lt.newList()
    MeSo.mergesort(listaHoras,compararhoras)
    C = True
    iteradorListaHoras = it.newIterator(listaHoras)
    while C:
        hora = it.next(iteradorListaHoras)
        if HI<=hora<=HF:
            lt.addLast(listaHorasFinal,hora)
        if not it.hasNext(iteradorListaHoras): C = False
    listaDeDiccionarios = lt.newList()
    D = True
    iteradorListaHorasFinal = it.newIterator(listaHorasFinal)
    caminoMinimo = iminpq.newIndexMinPQ(cmpfunction=compareRutas)
    while D:
        hora = it.next(iteradorListaHorasFinal)
        dijsktra = djk.Dijkstra(cont["graph"],CAI + "-" + str(hora))
        if djk.hasPathTo(dijsktra,CAF):
            costo = djk.distTo(dijsktra,CAF)
            iminpq.insert(caminoMinimo, hora, costo)
            lt.addLast(listaDeDiccionarios,{"Hora":hora,"Costo":costo})
        if not it.hasNext(iteradorListaHorasFinal): D = False
    horaMinima = iminpq.min(caminoMinimo)
    dijsktra = djk.Dijkstra(cont["graph"],CAI + "-" + str(horaMinima))
    ruta = djk.pathTo(dijsktra,CAF)
    gastoMinimo = djk.distTo(dijsktra,CAF)
    strRuta = ""
    iteradorFinal = it.newIterator(ruta)
    E = True
    while E:
        DIXX = it.next(iteradorFinal)
        if strRuta!="": strRuta += ","
        strRuta += DIXX["vertexA"] + "-->" + DIXX["vertexB"]
        if not it.hasNext(iteradorFinal): E = False
    rutacosto = (strRuta,gastoMinimo,horaMinima)
    return rutacosto

# ==============================
# Funciones Helper
# ==============================

def getDateTimeTaxiTrip(tripstartdate):
    """
    Recibe la informacion de un servicio de taxi leido del archivo de datos (parametro).
    Retorna de forma separada la fecha (date) y el tiempo (time) del dato 'trip_start_timestamp'
    Los datos date se pueden comparar con <, >, <=, >=, ==, !=
    Los datos time se pueden comparar con <, >, <=, >=, ==, !=
    """
    taxitripdatetime = datetime.datetime.strptime(tripstartdate, '%Y-%m-%dT%H:%M:%S.%f')
    return taxitripdatetime.date(), taxitripdatetime.time()

def alfa(totalMillas,totalDinero,totalServicios):
    return (totalMillas/totalDinero)*totalServicios

def topÑAlfaFecha(analyzer,FechaI):
    mapaValoresDia = me.getValue(om.get(analyzer["dateIndex"],FechaI))
    listaAlfas = lt.newList()
    IDsTaxi = m.keySet(mapaValoresDia)
    iteradorIDs = it.newIterator(IDsTaxi)
    C = True
    while C:
        llave = it.next(iteradorIDs)
        pareja = m.get(mapaValoresDia,llave)
        diccionario = me.getValue(pareja)
        lt.addLast(listaAlfas,{"Taxi":llave,"Alfa":alfa(diccionario["totalMillas"],diccionario["totalDinero"],diccionario["servicios"])})
        if not it.hasNext(iteradorIDs): C = False
    return listaAlfas

def concatenarAlfas(listaAlfas,mapaAlfaFinales):
    iterador = it.newIterator(listaAlfas)
    C = True
    while C:
        diccionario = it.next(iterador)
        if not m.contains(mapaAlfaFinales,diccionario["Taxi"]):
            diccionario["N"] = 1
        else: 
            Mapa = m.get(mapaAlfaFinales,diccionario["Taxi"])
            diccionarioMapa = me.getValue(Mapa)
            diccionario["N"] = diccionarioMapa["N"] + 1
            diccionario["Alfa"] = diccionarioMapa["Alfa"] + diccionario["Alfa"]
        m.put(mapaAlfaFinales,diccionario["Taxi"],diccionario)
        if not it.hasNext(iterador): C = False

def funcionQueue(listaAlfas,Ñ,cola):
    iterador = it.newIterator(listaAlfas)
    k = 0
    C = True
    while C:
        diccionario = it.next(iterador)
        texto = str(k+1) + ". El taxi con código que acaba en " + diccionario["Taxi"][-15:] + ". Con " + str(diccionario["Alfa"]) + " puntos"
        qe.enqueue(cola,texto)
        k += 1
        if k>=Ñ or (not it.hasNext(iterador)): C = False
    return cola

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

def compareO(o1, o2):
    o = me.getKey(o2)
    if (o1 == o):
        return 0
    elif (o1 > o):
        return 1
    else:
        return -1

def compararAlfas (Valor1, Valor2):
    return float(Valor1['Alfa']) > float(Valor2['Alfa'])

def compararhoras (Valor1, Valor2):
    return Valor1 < Valor2

def compareStations(stop, keyvaluestop):
    stopcode = keyvaluestop['key']
    if (stop == stopcode):
        return 0
    elif (stop > stopcode):
        return 1
    else:
        return -1

def compareR (A, B):
    if A == B:
        return 0
    elif A > B:
        return 1
    return -1