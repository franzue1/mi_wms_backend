# app/api/locations.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.LocationResponse])
async def get_all_locations(
    auth: AuthDependency,
    company_id: int = 1,
    skip: int = 0,
    limit: int = 100,
    
    # --- ¡PARÁMETROS DE FILTRO Y ORDEN AÑADIDOS! ---
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    path: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    warehouse_name: Optional[str] = Query(None),
    warehouse_status: Optional[str] = Query(None) # Para filtrar por estado de almacén
):
    """ Obtiene una lista de ubicaciones filtrada y paginada. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    # 1. Construir dict de filtros
    filters = {
        "path": path, "type": type, "warehouse_name": warehouse_name,
        "warehouse_status": warehouse_status
    }
    # 2. Limpiar Nones
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

    # 3. Llamar a la BD
    locations_raw = db.get_locations_filtered_sorted(
        company_id, 
        filters=clean_filters, 
        sort_by=sort_by, 
        ascending=ascending, 
        limit=limit, 
        offset=skip
    )
    return [dict(loc) for loc in locations_raw]

# --- ¡NUEVO ENDPOINT DE CONTEO! ---
@router.get("/count", response_model=int)
async def get_locations_count(
    auth: AuthDependency,
    company_id: int = 1,
    
    # Mismos filtros que get_all_locations
    path: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    warehouse_name: Optional[str] = Query(None),
    warehouse_status: Optional[str] = Query(None)
):
    """ Obtiene el conteo total de ubicaciones para la paginación. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = {
        "path": path, "type": type, "warehouse_name": warehouse_name,
        "warehouse_status": warehouse_status
    }
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    count = db.get_locations_count(company_id, filters=clean_filters)
    return count

@router.get("/{location_id}", response_model=schemas.LocationResponse)
async def get_location(location_id: int, auth: AuthDependency):
    """ Obtiene una ubicación por su ID. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    loc = db.get_location_details_by_id(location_id)
    if not loc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ubicación no encontrada")
    return dict(loc)

@router.post("/", response_model=schemas.LocationResponse, status_code=status.HTTP_201_CREATED)
async def create_location(
    location: schemas.LocationCreate,
    auth: AuthDependency,
    company_id: int = 1
):
    """ Crea una nueva ubicación. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_loc_id = db.create_location(
            company_id=company_id,
            name=location.name,
            path=location.path,
            type=location.type,
            category=location.category,
            warehouse_id=location.warehouse_id
        )
        created_loc = db.get_location_details_by_id(new_loc_id)
        return dict(created_loc)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.put("/{location_id}", response_model=schemas.LocationResponse)
async def update_location(
    location_id: int,
    location: schemas.LocationUpdate,
    auth: AuthDependency,
    company_id: int = 1
):
    """ Actualiza una ubicación existente. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        current_data = db.get_location_details_by_id(location_id)
        if not current_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ubicación no encontrada")
        
        update_data = current_data.copy()
        update_data.update(location.dict(exclude_unset=True))
        
        db.update_location(
            location_id=location_id,
            company_id=company_id,
            name=update_data['name'],
            path=update_data['path'],
            type=update_data['type'],
            category=update_data['category'],
            warehouse_id=update_data['warehouse_id']
        )
        
        updated_loc = db.get_location_details_by_id(location_id)
        return dict(updated_loc)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.delete("/{location_id}", status_code=status.HTTP_200_OK)
async def delete_location(location_id: int, auth: AuthDependency):
    """ Elimina una ubicación (si no está en uso). """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    success, message = db.delete_location(location_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return {"message": message}