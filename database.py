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
        #Se buscan los productos en INARMA01
        cursor.execute("""
            SELECT id_codigo, id_descripcion 
            FROM INARMA01 
            WHERE id_descripcion LIKE %s
            ORDER BY id_descripcion ASC
            LIMIT 100
        """, (nombre + '%',))
        #Productos INARMA01
        INARMA01 = cursor.fetchall()

        #Se buscan los productos de INARMA02
        cursor.execute("""
            SELECT id_codigo, id_descripcion
            FROM INARMA02
            WHERE id_descripcion LIKE %s
            ORDER BY id_descripcion ASC
            LIMIT 100
        """, (nombre + '%',))

        #Productos INARMA02
        INARMA02 = cursor.fetchall()
        #Si no hay productos en ninguno retorna None
        if not INARMA01 and not INARMA02:
            return None

        #Se crean dos listas (para INARMA01 e INARMA02) con diccionarios para cada producto
        productos_INARMA01 = []
        productos_INARMA02 = []
        if INARMA01:
            productos_INARMA01 = [{"codigo": codigo, "nombre": descripcion} for codigo, descripcion in INARMA01]

        if INARMA02:
            productos_INARMA02 = [{"codigo": codigo, "nombre": descripcion} for codigo, descripcion in INARMA02]


        #Se crea una nueva lista compuesta por las listas de productos de ambas tablas
        productos_vinculo = [productos_INARMA01, productos_INARMA02]
        return productos_vinculo
    finally:
        if conn:
            conn.close()


def obtenerProductosPorCodigo(codigo):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id_codigo, id_descripcion, id_grupo, id_maximo, id_minimo, id_lista1, id_provee
            FROM INARMA01
            WHERE id_codigo = %s
        """, (codigo,))
        tearma01 = cursor.fetchone()

        cursor.execute("""
            SELECT id_codigo, id_descripcion, id_grupo, id_maximo, id_minimo, id_lista1, id_provee
            FROM INARMA02
            WHERE id_codigo = %s
        """, (codigo,))
        tearma02 = cursor.fetchone()

        if not tearma01 and not tearma02:
            return None

        producto01 = None
        if tearma01:
            (codigo01, nombre01, grupo01, maximo01, minimo01, precio_lista01, id_proveedor01) = tearma01

            cursor.execute("""
                SELECT dt_sadoinicial, dt_entradas, dt_salidas, dt_ultimo_costo, dt_ultima_venta, dt_ultima_compra
                FROM INARAR01
                WHERE dt_codigo = %s
            """, (codigo,))
            inarar01 = cursor.fetchone()

            existencia01 = None
            ultimo_costo01 = None
            ultima_venta01 = None
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

            producto01 = {
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

        producto02 = None
        if tearma02:
            (codigo02, nombre02, grupo02, maximo02, minimo02, precio_lista02, id_proveedor02) = tearma02

            cursor.execute("""
                SELECT dt_sadoinicial, dt_entradas, dt_salidas, dt_ultimo_costo, dt_ultima_venta, dt_ultima_compra
                FROM INARAR02
                WHERE dt_codigo = %s
            """, (codigo,))
            inarar02 = cursor.fetchone()

            existencia02 = None
            ultimo_costo02 = None
            ultima_venta02 = None
            if inarar02:
                (saldo_inicial, entradas, salidas, ultimo_costo02, ultima_venta02, ultima_compra02) = inarar02
                existencia02 = (saldo_inicial or 0) + (entradas or 0) - (salidas or 0)

            cursor.execute("""
                SELECT dt_cliente
                FROM PRARMA02
                WHERE dt_codigoc = %s
            """, (id_proveedor02,))
            proveedor02 = cursor.fetchone()
            nombre_proveedor02 = proveedor02[0] if proveedor02 else "Desconocido"

            producto02 = {
                "codigo": codigo02,
                "nombre": nombre02,
                "grupo": grupo02,
                "maximo": maximo02,
                "minimo": minimo02,
                "precio": precio_lista02,
                "existencia": existencia02,
                "ultimo_costo": ultimo_costo02,
                "ultima_venta": ultima_venta02,
                "ultima_compra": ultima_compra02,
                "proveedor": nombre_proveedor02
            }

        return [
            producto01 if producto01 else {},
            producto02 if producto02 else {}
        ]

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
