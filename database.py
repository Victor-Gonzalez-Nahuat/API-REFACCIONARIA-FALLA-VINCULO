import pymysql
import os
from datetime import date

def get_connection():
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT'))

    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT
    )

def obtenerOfertas():
    conn = get_connection()
    cursor = conn.cursor()

    hoy = date.today()
    hoy_aammdd = int(hoy.strftime("%y%m%d"))

    cursor.execute("""
        SELECT o.of_codigo, o.of_fecha, o.of_imagen, o.of_observaciones, p.id_descripcion, p.id_lista1
        FROM INAROF01 o
        JOIN INARMA01 p ON o.of_codigo = p.id_codigo
        WHERE o.of_fecha > %s
    """, (hoy_aammdd,))

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return None

    ofertas = [{
        "nombre": nombre,
        "precio": precio,
        "fecha": fecha,
        "imagen": imagen,
        "observaciones": observaciones,
        "codigo": codigo,

    } for codigo, fecha, imagen, observaciones, nombre, precio in resultados]

    return ofertas

def obtenerProductosPorNombre(nombre):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT codigo, descripcion 
            FROM INARMA01 
            WHERE descripcion LIKE %s
            ORDER BY descripcion ASC
            LIMIT 100
        """, (nombre + '%',))
        productos_encontrados = cursor.fetchall()

        if not productos_encontrados:
            return None

        return [{"codigo": codigo, "nombre": descripcion} for codigo, descripcion in productos_encontrados]
    finally:
        if conn:
            conn.close()


def obtenerProductosPorCodigo(codigo):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT codigo, descripcion, grupo, maximo, minimo, lista1, provee
            FROM INARMA01
            WHERE codigo = %s
        """, (codigo,))
        tearma01 = cursor.fetchone()

        if not tearma01:
            return None

        (codigo01, nombre01, grupo01, maximo01, minimo01, precio_lista01, id_proveedor01) = tearma01

        cursor.execute("""
            SELECT saldoinicial, entradas, salidas, ultimo_costo, ultima_venta, ultima_compra
            FROM INARAR01
            WHERE dt_codigo = %s
        """, (codigo,))
        inarar01 = cursor.fetchone()

        existencia01 = None
        ultimo_costo01 = None
        ultima_venta01 = None
        ultima_compra01 = None
        if inarar01:
            (saldo_inicial, entradas, salidas, ultimo_costo01, ultima_venta01, ultima_compra01) = inarar01
            existencia01 = (saldo_inicial or 0) + (entradas or 0) - (salidas or 0)

        cursor.execute("""
            SELECT dt_cliente
            FROM PRARMA01
            WHERE dt_codigoc = %s
        """, (id_proveedor01,))
        proveedor01 = cursor.fetchone()
        nombre_proveedor01 = proveedor01[0] if proveedor01 else "Desconocido"

        return {
            "codigo": codigo01,
            "nombre": nombre01,
            "grupo": grupo01,
            "maximo": maximo01,
            "minimo": minimo01,
            "precio": precio_lista01,
            "existencia": existencia01,
            "ultimo_costo": ultimo_costo01,
            "ultima_venta": ultima_venta01,
            "ultima_compra": ultima_compra01,
            "proveedor": nombre_proveedor01
        }

    finally:
        if conn:
            conn.close()


def obtenerLosPrimerosProductos(limit):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM INARMA01 LIMIT %s", (limit,))
    rows = cursor.fetchall()
    conn.close()
    productos = []
    for row in rows:
        productos.append({
            "codigo": row[1],
            "nombre": row[3]
        })
    return productos
