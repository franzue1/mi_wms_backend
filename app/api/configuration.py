# app/api/configuration.py
"""
Endpoints de Configuración.
Delega lógica de validación al ConfigService.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.config_service import ConfigService
from app.exceptions import ValidationError
import asyncio
import traceback

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper de Permisos ---
def check_config_permission(auth: AuthDependency):
    if "nav.config.view" not in auth.permissions: 
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

# --- Categorías de Producto ---
# (Estos endpoints estaban BIEN, los mantenemos igual)

@router.get("/product-categories", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_product_categories(company_id: int = Query(...)):
    data = await asyncio.to_thread(db.get_product_categories, company_id)
    return [dict(row) for row in data]

@router.post("/product-categories", response_model=schemas.ConfigResponse, status_code=201, dependencies=[Depends(check_config_permission)])
async def create_product_category(category: schemas.ConfigCreate, company_id: int = Query(...)):
    """Crea una nueva categoría de producto. Usa ConfigService para validación."""
    try:
        # Validar datos usando el servicio
        validated = ConfigService.validate_product_category(category.name, company_id)

        new_item = await asyncio.to_thread(
            db.create_product_category,
            validated["name"],
            validated["company_id"]
        )
        return ConfigService.build_category_response(dict(new_item))
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.put("/product-categories/{category_id}", response_model=schemas.ConfigResponse, dependencies=[Depends(check_config_permission)])
async def update_product_category(category_id: int, category: schemas.ConfigCreate, company_id: int = Query(...)):
    """Actualiza una categoría de producto. Usa ConfigService para validación."""
    try:
        # Validar datos usando el servicio
        validated = ConfigService.validate_product_category(category.name, company_id)

        updated_item = await asyncio.to_thread(
            db.update_product_category,
            category_id,
            validated["name"],
            validated["company_id"]
        )
        return ConfigService.build_category_response(dict(updated_item))
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.delete("/product-categories/{category_id}", status_code=200, dependencies=[Depends(check_config_permission)])
async def delete_product_category(category_id: int, company_id: int = Query(...)):
    success, message = await asyncio.to_thread(db.delete_product_category, category_id, company_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Unidades de Medida (UoM) - REFACTORIZADO A MULTI-COMPAÑÍA ---

@router.get("/uoms", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_uoms(company_id: int = Query(...)): # [CORREGIDO] Agregado company_id
    data = await asyncio.to_thread(db.get_uoms, company_id)
    return [dict(row) for row in data]

@router.post("/uoms", response_model=schemas.ConfigResponse, status_code=201, dependencies=[Depends(check_config_permission)])
async def create_uom(uom: schemas.ConfigCreate, company_id: int = Query(...)):
    """Crea una nueva unidad de medida. Usa ConfigService para validación."""
    try:
        # Validar datos usando el servicio
        validated = ConfigService.validate_uom(uom.name, company_id)

        new_id = await asyncio.to_thread(
            db.create_uom,
            validated["name"],
            validated["company_id"]
        )
        return {"id": new_id, "name": validated["name"]}
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/uoms/{uom_id}", response_model=schemas.ConfigResponse, dependencies=[Depends(check_config_permission)])
async def update_uom(uom_id: int, uom: schemas.ConfigCreate, company_id: int = Query(...)):
    """Actualiza una unidad de medida. Usa ConfigService para validación."""
    try:
        # Validar datos usando el servicio
        validated = ConfigService.validate_uom(uom.name, company_id)

        await asyncio.to_thread(
            db.update_uom,
            uom_id,
            validated["name"],
            validated["company_id"]
        )
        return {"id": uom_id, "name": validated["name"]}
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.delete("/uoms/{uom_id}", status_code=200, dependencies=[Depends(check_config_permission)])
async def delete_uom(uom_id: int, company_id: int = Query(...)): # [CORREGIDO] Agregado company_id
    success, message = await asyncio.to_thread(db.delete_uom, uom_id, company_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Categorías de Almacén ---
# (Estos ya estaban bien, los mantenemos igual)

@router.get("/warehouse-categories", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_warehouse_categories(company_id: int = Query(...)):
    data = await asyncio.to_thread(db.get_warehouse_categories, company_id)
    return [dict(row) for row in data]

@router.post("/warehouse-categories", response_model=schemas.ConfigResponse, status_code=201, dependencies=[Depends(check_config_permission)])
async def create_warehouse_category(category: schemas.ConfigCreate, company_id: int = Query(...)):
    """Crea una nueva categoría de almacén. Usa ConfigService para validación."""
    try:
        # Validar datos usando el servicio
        validated = ConfigService.validate_warehouse_category(category.name, company_id)

        new_item = await asyncio.to_thread(
            db.create_warehouse_category,
            validated["name"],
            validated["company_id"]
        )
        return ConfigService.build_category_response(dict(new_item))
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/warehouse-categories/{category_id}", response_model=schemas.ConfigResponse, dependencies=[Depends(check_config_permission)])
async def update_warehouse_category(category_id: int, category: schemas.ConfigCreate, company_id: int = Query(...)):
    """Actualiza una categoría de almacén. Usa ConfigService para validación."""
    try:
        # Validar datos usando el servicio
        validated = ConfigService.validate_warehouse_category(category.name, company_id)

        updated_item = await asyncio.to_thread(
            db.update_warehouse_category,
            category_id,
            validated["name"],
            validated["company_id"]
        )
        return ConfigService.build_category_response(dict(updated_item))
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.delete("/warehouse-categories/{category_id}", status_code=200, dependencies=[Depends(check_config_permission)])
async def delete_warehouse_category(category_id: int, company_id: int = Query(...)):
    success, message = await asyncio.to_thread(db.delete_warehouse_category, category_id, company_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Categorías de Socio (Partner) ---

@router.get("/partner-categories", response_model=List[schemas.ConfigResponse], dependencies=[Depends(check_config_permission)])
async def get_partner_categories(company_id: int = Query(...)):
    data = await asyncio.to_thread(db.get_partner_categories, company_id)
    return [dict(row) for row in data]