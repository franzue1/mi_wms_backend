# app/main.py
from fastapi import FastAPI
from app import database as db
import traceback
import contextlib
# (Tus otras importaciones de 'app.api' están más abajo)

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # El servidor se inicia CADA VEZ. Solo queremos verificar la conexión.
    # Las tablas y datos ya fueron creados la PRIMERA VEZ.
    print("--- Servidor iniciando, verificando conexión a BD... ---")
    try:
        conn = db.connect_db()
        # Hacemos una consulta simple para verificar que la conexión funciona
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        print("--- Conexión a Base de Datos exitosa. ---")
    except Exception as e:
        print(f"!!! ERROR FATAL DURANTE EL INICIO: No se pudo conectar a la BD. {e}")
        traceback.print_exc()
    
    yield
    # Código que se ejecuta cuando la app se apaga
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