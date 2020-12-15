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


import sys
import config 
from App import controller
from DISClib.ADT import stack as sk
from DISClib.ADT import queue as qe
import timeit
import datetime
assert config

"""
La vista se encarga de la interacción con el usuario.
Presenta el menu de opciones  y  por cada seleccion
hace la solicitud al controlador para ejecutar la
operación seleccionada.
"""

# ___________________________________________________
#  Variables
# ___________________________________________________

recursionLimit = 20000
FechaF = None
large = "taxi-trips-wrvz-psew-subset-large.csv"
medium = "taxi-trips-wrvz-psew-subset-medium.csv"
small = "taxi-trips-wrvz-psew-subset-small.csv"

# ___________________________________________________
#  Funciones para imprimir la información de
#  respuesta.  La vista solo interactua con
#  el controlador.
# ___________________________________________________

def ImprimirEnConsola(cola, DatosAdicionales = None):
    if qe.isEmpty(cola)==False: 
        Centinela = True
        print("-"*100)
        while Centinela==True:
            print("", end=" "*10)
            k = qe.dequeue(cola)
            if not k=="": print("•" + k)
            else: print(k)
            if qe.isEmpty(cola)==True: Centinela=False
        print("-"*100)
    else: print("No se ha encontrado información para el criterio")
    if DatosAdicionales!=None:
        if qe.isEmpty(DatosAdicionales)==False:
            CentinelaAdicionales = True
            while CentinelaAdicionales==True:
                dato = qe.dequeue(DatosAdicionales)
                print(str(dato[0])+str(dato[1]))
                if qe.isEmpty(DatosAdicionales)==True: CentinelaAdicionales=False

# ___________________________________________________
#  Menu principal
# ___________________________________________________

def printMenu():
    print("\n")
    print("*"*43)
    print("Bienvenido")
    print("1- Crear estructuras de datos")
    print("2- Cargar información de taxis en Chicago")
    print("3- Reporte (Parte A)")
    print("4- Taxis con más puntos (Parte B)")
    print("5- Mejor horario para desplazarse entre dos Community Area (Parte C)")
    print("0- Salir")
    print("*"*43)

def optionOne():
    global cont
    print("\nInicializando....")
    cont = controller.init()

def optionTwo():
    print("\nCargando información de taxis en Chicago ....")
    sys.setrecursionlimit(recursionLimit)
    controller.loadData(cont, tamaño)
    print('Viajes cargados ' + str(controller.dataSize(cont)))

def optionThree():
    print("\nCargando reporte ....")
    reporte = controller.f3(cont,M,N)
    ImprimirEnConsola(reporte)

def optionFour():
    print("\nCargando taxis con más puntos ....")
    top = controller.f4(cont,Ñ,FechaI,FechaF)
    ImprimirEnConsola(top)

def optionFive():
    print("5- Cargando mejor horario para desplazarse entre dos Community Area")
    horario = controller.f5(cont, CAI, CAF, HI, HF)

while True:
    #try:
    printMenu()
    inputs = input('Seleccione una opción para continuar\n>')

    if int(inputs) == 0:
        print("\nHasta pronto!")
        break

    if int(inputs) == 1:
        executiontime = timeit.timeit(optionOne, number=1)
        print("Tiempo de ejecución: " + str(executiontime)+ " segundos")

    elif int(inputs) == 2:
        tamaño = input("Si desea trabajar con el archivo de datos grande escriba L, con el mediano M, y con el pequeño S: ").upper()
        if tamaño=="L": tamaño = large
        elif tamaño=="M": tamaño = medium
        elif tamaño=="S": tamaño = small
        executiontime = timeit.timeit(optionTwo, number=1)
        print("Tiempo de ejecución: " + str(executiontime)+ " segundos")

    elif int(inputs) == 3:
        M = int(input("Ingrese el número de compañías que quiere que se muestren por número de táxis afiliados: "))
        N = int(input("Ingrese el número de compañías que quiere que se muestren por número de servicios prestados: "))
        executiontime = timeit.timeit(optionThree, number=1)
        print("Tiempo de ejecución: " + str(executiontime)+ " segundos")

    elif int(inputs) == 4:
        print("¿Desea obtener el top de taxis usando la función alfa, para un solo día o para un rango de fechas?")
        booleano = int(input("""Ingrese "1" para un solo día o "2" para un rango de fechas: """))
        Ñ = int(input("Ingrese el número de táxis que quiere que se muestren en el top: "))
        if booleano==1:
            FechaI = input("Fecha (YYYY-MM-DD): ")
            FechaI = datetime.datetime.strptime(FechaI, '%Y-%m-%d').date()
        elif booleano==2:
            FechaI = input("Fecha inicial (YYYY-MM-DD): ")
            FechaI = datetime.datetime.strptime(FechaI, '%Y-%m-%d').date()
            FechaF = input("Fecha final (YYYY-MM-DD): ")
            FechaF = datetime.datetime.strptime(FechaF, '%Y-%m-%d').date()
        executiontime = timeit.timeit(optionFour, number=1)
        print("Tiempo de ejecución: " + str(executiontime)+ " segundos")
    
    elif int(inputs) == 5:
        CAI = str(float(input("Por favor introduzca el número de la Community Area de la cual quiere salir: ")))
        CAF = str(float(input("Por favor introduzca el número de la Community Area a la cual quiere llegar: ")))
        HI = input("Hora Inicial (HH:MM): ")
        HI = "1900-01-01 " + HI
        HF = input("Hora Final (HH:MM): ")
        HF = "1900-01-01 " + HF
        HI = datetime.datetime.strptime(HI, '%Y-%m-%d %H:%M').time()
        HF = datetime.datetime.strptime(HF, '%Y-%m-%d %H:%M').time()
        executiontime = timeit.timeit(optionFive, number=1)
        print("Tiempo de ejecución: " + str(executiontime)+ " segundos")

    #except:
     #   print("\nAlgo ocurrió mal, asegurese que todo esté bien e intente nuevamente: ")

sys.exit(0)