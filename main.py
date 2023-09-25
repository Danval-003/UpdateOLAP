import threading
from connection import connect

# Define las listas de datos como variables globales para que puedan ser compartidas entre hilos
galerasData = []
registerData = []
trabajadorData = []
timeData = []


def delete():
    conOLAP = connect(database='olap')
    cursorOlap = conOLAP.cursor()
    cursorOlap.execute("""
        DELETE FROM REGISTROS;
        DELETE FROM GALERAS_DIM;
        DELETE FROM TRABAJADORES_DIM;
        DELETE FROM TIEMPO_DIM;
    """)
    conOLAP.commit()


def trabajador():
    global trabajadorData  # Utiliza la variable global

    conOLTP = connect()
    cursorOLTP = conOLTP.cursor()
    cursorOLTP.execute("""
    select id_trabajador, nombre from trabajador;
    """)

    trabajadorData = cursorOLTP.fetchall()


def galeras():
    global galerasData  # Utiliza la variable global

    conOLTP = connect()
    cursorOLTP = conOLTP.cursor()
    cursorOLTP.execute("""
    select galeras.id_galera, no_galera, id_lote, fecha_inicio, fecha_salida, existencia_inicial from galeras 
    inner join galeras_info gi on galeras.id_galera = gi.id_galera;
    """)

    galerasData = cursorOLTP.fetchall()


def registros():
    global registerData, timeData  # Utiliza las variables globales

    conOLTP = connect()
    cursorOLTP = conOLTP.cursor()
    cursorOLTP.execute("""
    select id_galera, id_trabajador, date(fecha), decesos, ca, pesado, existencia_actual
    from registro;
    """)

    registerData = cursorOLTP.fetchall()

    for x in registerData:
        if x[2] not in timeData:
            timeData.append(x[2])


def makeTimeTable():
    global timeData
    conOLAP = connect(database='olap')
    cursorOlap = conOLAP.cursor()

    for x in timeData:
        cursorOlap.execute("""
        insert into tiempo_dim(fecha, anio, mes, mesnum, dia, trimestre_del_anio) VALUES (%s, %s, %s, %s, %s, %s)
        """, tuple([x, x.year, x.strftime('%B'), x.month, x.day, (x.month - 1) // 3 + 1]))
    conOLAP.commit()


def makeGalerasTable():
    global galerasData
    conOLAP = connect(database='olap')
    cursorOlap = conOLAP.cursor()

    for x in galerasData:
        cursorOlap.execute("""
        insert into galeras_dim(id_galera, numero_galera, id_lote, fecha_inicio, fecha_final, existencia_inicial) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """, tuple([j for j in x]))
    conOLAP.commit()


def makeTrabajadorTable():
    global trabajadorData
    conOLAP = connect(database='olap')
    cursorOlap = conOLAP.cursor()

    for x in trabajadorData:
        cursorOlap.execute("""
        insert into trabajadores_dim(id_trabajador, nombre) VALUES (%s, %s)
        """, tuple([j for j in x]))
    conOLAP.commit()


def makeRegistersTable():
    global registerData
    conOLAP = connect(database='olap')
    cursorOlap = conOLAP.cursor()

    for x in registerData:
        cursorOlap.execute("""
        insert into registros(id_galera, id_trabajador, fecha, decesos, ca, pesaje, existencia_recidual) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple([j for j in x]))
    conOLAP.commit()


# Crear hilos para ejecutar cada función
thread1 = threading.Thread(target=delete)
thread2 = threading.Thread(target=trabajador)
thread3 = threading.Thread(target=galeras)
thread4 = threading.Thread(target=registros)

# Iniciar los hilos
thread1.start()
thread2.start()
thread3.start()
thread4.start()

# Esperar a que todos los hilos terminen
thread1.join()
thread2.join()
thread3.join()
thread4.join()

thread5 = threading.Thread(target=makeTimeTable)
thread6 = threading.Thread(target=makeGalerasTable)
thread7 = threading.Thread(target=makeTrabajadorTable)

thread5.start()
thread6.start()
thread7.start()

thread5.join()
thread6.join()
thread7.join()

makeRegistersTable()
