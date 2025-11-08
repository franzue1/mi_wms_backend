# app/main.py
from fastapi import FastAPI
# 1. Importa el nuevo router
from app.api import (
    auth, products, warehouses, partners, locations, reports, 
    pickings, work_orders, adjustments, configuration, admin)

app = FastAPI(
    title="Mi WMS Backend API",
    description="La API backend para el sistema TheBoringWMS."
)

# Incluimos los routers
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
app.include_router(admin.router, prefix="/admin", tags=["Administración (RBAC)"]) # <-- 2. Añade

@app.get("/")
async def read_root():
    return {"message": "Bienvenido a la API de TheBoringWMS"}