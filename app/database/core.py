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
    [OPTIMIZADO] Usa ThreadedConnectionPool para concurrencia segura en FastAPI.
    """
    global db_pool, DATABASE_URL
    if db_pool:
        return 

    load_dotenv()
    DATABASE_URL = os.environ.get("DATABASE_URL")
    
    if DATABASE_URL is None:
        raise ValueError("No se pudo conectar: DATABASE_URL no está configurada.")

    try:
        # Definimos argumentos base
        pool_args = {
            "minconn": 1,
            "maxconn": 10, # Ajustar según el plan de Render/Supabase
            "dsn": DATABASE_URL
        }

        # --- CORRECCIÓN DE SEGURIDAD PARA RENDER/SUPABASE ---
        # Si NO estamos en localhost, forzamos SSL
        if "localhost" not in DATABASE_URL and "127.0.0.1" not in DATABASE_URL:
            print(" -> Forzando SSL para conexión remota...")
            pool_args["sslmode"] = "require"
        # ----------------------------------------------------

        # [MEJORA CRÍTICA] Usamos ThreadedConnectionPool
        # SimpleConnectionPool no es thread-safe para aplicaciones multihilo como FastAPI/Uvicorn
        db_pool = psycopg2.pool.ThreadedConnectionPool(**pool_args)
        
        # Probar conexión inmediatamente (Fail Fast)
        conn = db_pool.getconn()
        if "localhost" in DATABASE_URL:
            print(" -> Pool de BD (Local - Threaded) Creado.")
        else:
            print(" -> Pool de BD (Producción + SSL - Threaded) Creado.")
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
        try:
            db_pool.putconn(conn)
        except Exception:
            # Si la conexión ya estaba cerrada o el pool murió, no crasheamos
            pass

def execute_query(query, params=(), fetchone=False, fetchall=False):
    """
    Ejecuta una consulta de LECTURA (SELECT).
    Maneja la conexión y el retorno al pool automáticamente.
    """
    global db_pool
    if not db_pool: init_db_pool()

    conn = None
    try:
        conn = db_pool.getconn()
        # [MEJORA] Asignamos el factory directamente en el cursor para no ensuciar la conexión global
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, params)
            if fetchone: return cursor.fetchone()
            if fetchall: return cursor.fetchall()
            
    except Exception as e:
        print(f"Error lectura SQL: {e}")
        # No imprimimos traceback completo para errores de consulta comunes, solo el mensaje
        # traceback.print_exc() 
        raise e
    finally:
        if conn: db_pool.putconn(conn) 

def execute_commit_query(query, params=(), fetchone=False):
    """
    Ejecuta una consulta de ESCRITURA (INSERT/UPDATE/DELETE).
    Maneja Commit y Rollback automáticamente.
    """
    global db_pool
    if not db_pool: init_db_pool()

    conn = None
    try:
        conn = db_pool.getconn() 
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
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