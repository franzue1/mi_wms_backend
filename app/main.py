# app/main.py
from fastapi import FastAPI
from app import database as db
import traceback
import contextlib

#---------------- PARA PRODUCCION
@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Servidor iniciando, creando pool de conexiones... ---")
    try:
        db.init_db_pool()
        print("--- Pool de conexiones a Base de Datos creado. ---")

    except Exception as e:
        print(f"!!! ERROR FATAL DURANTE EL INICIO: {e}")
    
    yield
    
    print("--- Servidor apagándose. ---")

#-------------------------SOLO PARA CREACION INICIAL DE MODELO DE DATOS

#@contextlib.asynccontextmanager
#async def lifespan(app: FastAPI):
 #   print("--- Servidor iniciando, creando pool y verificando BD... ---")
  #  conn = None # Para asegurarnos de que podemos cerrarlo si algo falla
   # try:
    #    # 1. Llama a la función que crea el pool global
     #   db.init_db_pool()
      #  print("--- Pool de conexiones a Base de Datos creado. ---")
       # # 2. Obtener UNA conexión del pool para la configuración inicial
#        conn = db.db_pool.getconn() 
 #       # 3. Ejecutar la creación de tablas
  #      print("--- Creando/Verificando esquema de tablas... ---")
   #     db.create_schema(conn)
    #    # 4. Ejecutar la creación de datos iniciales
     #   print("--- Creando/Verificando datos iniciales... ---")
      #  db.create_initial_data(conn)
       # print("--- Base de datos verificada e inicializada. ---")
#    except Exception as e:
 #       print(f"!!! ERROR FATAL DURANTE EL INICIO: {e}")
  #      traceback.print_exc()
   #     if conn:
    #        conn.rollback() # Revertir cambios si la creación de datos falló
#    finally:
 #       # 5. Devolver la conexión de configuración al pool
  #      if conn:
   #         db.db_pool.putconn(conn)
#    yield
 #   print("--- Servidor apagándose. ---")

#------------------------------------------------------------------------

app = FastAPI(
    title="Mi WMS Backend API",
    description="La API backend para el sistema TheBoringWMS.",
    lifespan=lifespan # <-- Esta línea se queda igual
)

from app.api import (
    auth, products, warehouses, partners, locations, reports, 
    pickings, work_orders, adjustments, configuration, admin
)
app.include_router(auth.router, prefix="/auth", tags=["Autenticación"])
app.include_router(products.router, prefix="/products", tags=["Productos"])
app.include_router(warehouses.router, prefix="/warehouses", tags=["Almacenes"])
app.include_router(partners.router, prefix="/partners", tags=["Socios (Partners)"])
app.include_router(locations.router, prefix="/locations", tags=["Ubicaciones"])
app.include_router(reports.router, prefix="/reports", tags=["Reportes"])
app.include_router(pickings.router, prefix="/pickings", tags=["Operaciones (Pickings)"])
app.include_router(work_orders.router, prefix="/work-orders", tags=["Liquidaciones (OTs)"])
app.include_router(adjustments.router, prefix="/adjustments", tags=["Ajustes de Inventario"])
app.include_router(configuration.router, prefix="/config", tags=["Configuración"])
app.include_router(admin.router, prefix="/admin", tags=["Administración (RBAC)"])

@app.get("/")
async def read_root():
    return {"message": "Bienvenido a la API de TheBoringWMS"}