#app/database/core.py

import psycopg2
import psycopg2.pool
import psycopg2.extras
import os
import traceback
from dotenv import load_dotenv

# --- CONFIGURACIÓN DEL POOL GLOBAL ---
db_pool = None
DATABASE_URL = None

def init_db_pool():
    """
    Inicializa el pool de conexiones.
    """
    global db_pool, DATABASE_URL
    if db_pool:
        return 

    load_dotenv()
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if DATABASE_URL is None:
        raise ValueError("No se pudo conectar: DATABASE_URL no está configurada.")

    print(f"DEBUG URL: {repr(DATABASE_URL)}")

    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
        
        # Probar conexión
        conn = db_pool.getconn()
        if "localhost" in DATABASE_URL:
            print(" -> Pool de BD (Local) Creado.")
        else:
            print(" -> Pool de BD (Producción) Creado.")
        db_pool.putconn(conn)

    except psycopg2.OperationalError as e:
        print(f"!!! ERROR CRÍTICO AL CREAR EL POOL DE BD !!!\n{e}")
        traceback.print_exc()
        raise

def get_db_connection():
    """Helper para obtener una conexión raw del pool (para transacciones manuales)"""
    global db_pool
    if not db_pool:
        init_db_pool()
    return db_pool.getconn()

def return_db_connection(conn):
    """Helper para devolver conexión al pool"""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

def execute_query(query, params=(), fetchone=False, fetchall=False):
    global db_pool
    if not db_pool: init_db_pool()

    conn = None
    try:
        conn = db_pool.getconn() 
        conn.cursor_factory = psycopg2.extras.DictCursor 
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if fetchone: return cursor.fetchone()
            if fetchall: return cursor.fetchall()

    except Exception as e:
        print(f"Error lectura SQL: {e}")
        traceback.print_exc()
        raise e
    finally:
        if conn: db_pool.putconn(conn) 

def execute_commit_query(query, params=(), fetchone=False):
    global db_pool
    if not db_pool: init_db_pool()

    conn = None
    try:
        conn = db_pool.getconn() 
        conn.cursor_factory = psycopg2.extras.DictCursor
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            result = None
            if fetchone:
                result = cursor.fetchone()
            conn.commit() 
            
            if fetchone: return result
            return True 
            
    except Exception as e:
        print(f"Error escritura SQL: {e}")
        traceback.print_exc()
        if conn: conn.rollback() 
        raise e 
    finally:
        if conn: db_pool.putconn(conn)