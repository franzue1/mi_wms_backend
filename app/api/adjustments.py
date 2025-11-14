# app/api/adjustments.py
from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import List, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper para parsear filtros de Ajuste ---
def _parse_adjustment_filters(request: Request) -> dict:
    """Lee los query params y los convierte en un dict de filtros."""
    filters = {}
    
    # Lista de claves de filtro que SÍ aceptamos (de tu database.py)
    KNOWN_FILTER_KEYS = {
        'name', 'state', 'responsible_user',
        'adjustment_reason', 'src_path', 'dest_path'
    }
    
    RESERVED_KEYS = {'company_id', 'skip', 'limit', 'sort_by', 'ascending', 'token'}

    for key, value in request.query_params.items():
        if key not in RESERVED_KEYS and key in KNOWN_FILTER_KEYS and value:
            filters[key] = value
    
    return filters

# --- Endpoint de Lista de Ajustes (con paginación) ---
@router.get("/", response_model=List[schemas.AdjustmentListResponse])
async def get_all_adjustments(
    auth: AuthDependency,
    company_id: int, 
    request: Request,
    skip: int = 0,
    limit: int = 50,
    sort_by: str = 'id',
    ascending: bool = False
):
    """ Obtiene la lista paginada de Ajustes de Inventario. """
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    try:
        filters = _parse_adjustment_filters(request)
        
        adj_raw = await asyncio.to_thread(
            db.get_adjustments_filtered_sorted,
            company_id=company_id, 
            filters=filters,
            sort_by=sort_by, 
            ascending=ascending, 
            limit=limit, 
            offset=skip
        )
        return [dict(adj) for adj in adj_raw]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener ajustes: {e}")

# --- Endpoint de Conteo de Ajustes ---
@router.get("/count", response_model=int)
async def get_adjustments_count(
    auth: AuthDependency,
    company_id: int,
    request: Request
):
    """ Obtiene el CONTEO TOTAL de Ajustes de Inventario. """
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    try:
        filters = _parse_adjustment_filters(request)
        count = await asyncio.to_thread(
            db.get_adjustments_count, company_id, filters=filters
        )
        return count
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al contar ajustes: {e}")