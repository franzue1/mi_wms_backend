# app/main.py
from fastapi import FastAPI
from app import database as db
import traceback
import contextlib

# En mi_wms_backend/app/main.py

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # --- VERSIÓN DE INICIALIZACIÓN ---
    # Esto creará las tablas y los datos iniciales
    print("--- Servidor iniciando, verificando base de datos... ---")
    try:
        conn = db.connect_db()
        db.create_schema(conn)      # <-- Esta línea es la clave
        db.create_initial_data(conn) # <-- Y esta
        conn.close()
        print("--- Base de datos verificada y/o inicializada. ---")
    except Exception as e:
        print(f"!!! ERROR FATAL DURANTE EL INICIO: No se pudo inicializar la BD. {e}")
        traceback.print_exc()
    
    yield
    print("--- Servidor apagándose. ---")

app = FastAPI(
    title="Mi WMS Backend API",
    description="La API backend para el sistema TheBoringWMS.",
    lifespan=lifespan # <-- Esta línea se queda igual
)

# ... (El resto de tus imports de app.api y app.include_router) ...
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