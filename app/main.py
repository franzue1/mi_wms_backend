# mi_wms_backend/app/main.py

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import traceback
from app import database as db
import os

# Importar los routers
from app.api import (
    auth, 
    products, 
    warehouses, 
    partners, 
    locations, 
    pickings, 
    admin,
    adjustments,
    work_orders,
    reports,
    configuration,
    projects,
    employees
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- Servidor iniciando, creando pool y verificando BD... ---")
    conn = None 
    try:
        # 1. Inicializar el Pool (Siempre necesario)
        db.init_db_pool()
        print("--- Pool de conexiones a Base de Datos creado. ---")
        
        # 2. Verificar si debemos inicializar la BD (Schema + Seed)
        should_init_db = os.getenv("INIT_DB", "False").lower() in ("true", "1", "yes")

        if should_init_db:
            print("--- [INIT_DB=True] Ejecutando creación de esquema y datos... ---")
            conn = db.get_db_connection()
            
            # Crear tablas
            db.create_schema(conn)
            # Crear datos base (admin, etc.)
            db.create_initial_data(conn)
            
            print("--- [INIT_DB=True] Inicialización completada. ---")
        else:
            print("--- [INIT_DB=False] Saltando creación de esquema/datos (Modo Producción). ---")
        
    except Exception as e:
        print(f"!!! ERROR FATAL DURANTE EL INICIO: {e}")
        traceback.print_exc()
        if conn:
            try: conn.rollback()
            except: pass
    finally:
        if conn:
            db.return_db_connection(conn)
            
    yield
    print("--- Servidor apagándose. ---")


# Crear la aplicación FastAPI con el lifespan
app = FastAPI(title="Mi WMS API", version="1.0.0", lifespan=lifespan)

# Configurar CORS (Permitir que Flet se conecte)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir los routers (endpoints)
app.include_router(auth.router, prefix="/auth", tags=["Auth"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(products.router, prefix="/products", tags=["Products"])
app.include_router(warehouses.router, prefix="/warehouses", tags=["Warehouses"])
app.include_router(locations.router, prefix="/locations", tags=["Locations"])
app.include_router(partners.router, prefix="/partners", tags=["Partners"])
app.include_router(pickings.router, prefix="/pickings", tags=["Pickings"])
app.include_router(adjustments.router, prefix="/adjustments", tags=["Adjustments"])
app.include_router(work_orders.router, prefix="/work-orders", tags=["Work Orders"])
app.include_router(configuration.router, prefix="/config", tags=["Configuration"])
app.include_router(reports.router, prefix="/reports", tags=["Reports"])
app.include_router(projects.router, prefix="/projects", tags=["Projects"])
app.include_router(employees.router, prefix="/employees", tags=["Employees"])

@app.get("/")
def read_root():
    return {"message": "Bienvenido a la API de Mi WMS"}