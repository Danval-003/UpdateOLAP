import psycopg2


# Funcion que realiza la coneccion a la base de datos
def connect(user='postgres', password='OrtalizasDelUerto22', database="postgres"):
    # Se le manda la información para realizar la coneccion
    conn = psycopg2.connect(
        host="database-1.c5xesezrpxni.us-east-2.rds.amazonaws.com",
        database=database,
        user=user,
        password=password
    )
    return conn
