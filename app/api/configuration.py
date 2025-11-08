# app/api/configuration.py
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper de Permisos ---
def check_config_permission(auth: AuthDependency):
    if "nav.config.view" not in auth.permissions: # Usamos el permiso genérico de config
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

# --- Categorías de Producto ---

@router.get("/product-categories", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_product_categories():
    data = db.get_product_categories()
    return [dict(row) for row in data]

@router.post("/product-categories", response_model=schemas.ConfigResponse, status_code=201, dependencies=[Depends(check_config_permission)])
async def create_product_category(category: schemas.ConfigCreate):
    try:
        new_id = db.create_product_category(category.name)
        return {"id": new_id, "name": category.name}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/product-categories/{category_id}", response_model=schemas.ConfigResponse, dependencies=[Depends(check_config_permission)])
async def update_product_category(category_id: int, category: schemas.ConfigCreate):
    try:
        db.update_product_category(category_id, category.name)
        return {"id": category_id, "name": category.name}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.delete("/product-categories/{category_id}", status_code=200, dependencies=[Depends(check_config_permission)])
async def delete_product_category(category_id: int):
    success, message = db.delete_product_category(category_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Unidades de Medida (UoM) ---

@router.get("/uoms", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_uoms():
    data = db.get_uoms()
    return [dict(row) for row in data]

@router.post("/uoms", response_model=schemas.ConfigResponse, status_code=201, dependencies=[Depends(check_config_permission)])
async def create_uom(uom: schemas.ConfigCreate):
    try:
        new_id = db.create_uom(uom.name)
        return {"id": new_id, "name": uom.name}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/uoms/{uom_id}", response_model=schemas.ConfigResponse, dependencies=[Depends(check_config_permission)])
async def update_uom(uom_id: int, uom: schemas.ConfigCreate):
    try:
        db.update_uom(uom_id, uom.name)
        return {"id": uom_id, "name": uom.name}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.delete("/uoms/{uom_id}", status_code=200, dependencies=[Depends(check_config_permission)])
async def delete_uom(uom_id: int):
    success, message = db.delete_uom(uom_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Categorías de Almacén ---

@router.get("/warehouse-categories", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_warehouse_categories():
    data = db.get_warehouse_categories()
    return [dict(row) for row in data]

@router.post("/warehouse-categories", response_model=schemas.ConfigResponse, status_code=201, dependencies=[Depends(check_config_permission)])
async def create_warehouse_category(category: schemas.ConfigCreate):
    try:
        new_id = db.create_warehouse_category(category.name)
        return {"id": new_id, "name": category.name}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/warehouse-categories/{category_id}", response_model=schemas.ConfigResponse, dependencies=[Depends(check_config_permission)])
async def update_warehouse_category(category_id: int, category: schemas.ConfigCreate):
    try:
        db.update_warehouse_category(category_id, category.name)
        return {"id": category_id, "name": category.name}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.delete("/warehouse-categories/{category_id}", status_code=200, dependencies=[Depends(check_config_permission)])
async def delete_warehouse_category(category_id: int):
    success, message = db.delete_warehouse_category(category_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Categorías de Socio (Partner) ---

@router.get("/partner-categories", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_partner_categories():
    data = db.get_partner_categories()
    return [dict(row) for row in data]

# (Las funciones POST, PUT, DELETE para partner_categories no están en tu database.py,
# pero si las necesitaras, seguirían el mismo patrón de arriba)