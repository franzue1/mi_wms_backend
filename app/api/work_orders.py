# app/api/work_orders.py
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- ¡NUEVO! Helper para parsear filtros ---
def _parse_filters_from_request(request: Request) -> dict:
    """
    Lee los query params de la URL y los convierte en un dict de filtros
    para la base de datos, ignorando los parámetros de paginación/API.
    """
    filters = {}
    
    # Lista de claves de filtro que SÍ aceptamos (de tu 'filter_map' en database.py)
    KNOWN_FILTER_KEYS = {
        'id', 'ot_number', 'service_type', 'job_type', 
        'customer_name', 'address', 'phase', 'warehouse_name', 
        'location_src_path', 'service_act_number'
    }
    
    # Ignorar claves que no son de filtrado
    RESERVED_KEYS = {'company_id', 'skip', 'limit', 'sort_by', 'ascending', 'token'}

    for key, value in request.query_params.items():
        if key not in RESERVED_KEYS and key in KNOWN_FILTER_KEYS and value:
            filters[key] = value
    
    return filters
# --- Fin del Helper ---


@router.get("/", response_model=List[schemas.WorkOrderResponse])
async def get_all_work_orders(
    auth: AuthDependency,
    company_id: int, 
    request: Request, # <-- ¡CAMBIO! Aceptamos el Request
    skip: int = 0,
    limit: int = 50,
    sort_by: str = 'id',
    ascending: bool = False
):
    """ 
    Obtiene la lista paginada de Órdenes de Trabajo (Liquidaciones).
    ¡AHORA ACEPTA FILTROS DINÁMICOS!
    """
    if "liquidaciones.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    try:
        # ¡CAMBIO! Parseamos los filtros desde la URL
        filters = _parse_filters_from_request(request)
        
        wo_raw = db.get_work_orders_filtered_sorted(
            company_id=company_id, 
            filters=filters, # <-- ¡CAMBIO! Pasamos los filtros reales
            sort_by=sort_by, 
            ascending=ascending, 
            limit=limit, 
            offset=skip
        )
        return [dict(wo) for wo in wo_raw]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener OTs: {e}")

@router.get("/count", response_model=int)
async def get_work_orders_count(
    auth: AuthDependency,
    company_id: int,
    request: Request # <-- ¡CAMBIO! Aceptamos el Request
):
    """ 
    Obtiene el CONTEO TOTAL de Órdenes de Trabajo (Liquidaciones).
    ¡AHORA ACEPTA FILTROS DINÁMICOS!
    """
    if "liquidaciones.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    try:
        # ¡CAMBIO! Parseamos los filtros desde la URL
        filters = _parse_filters_from_request(request)
        
        count = db.get_work_orders_count(company_id, filters=filters) # <-- ¡CAMBIO!
        return count
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al contar OTs: {e}")

# --- El resto de tus funciones (get_work_order_details_combo, create_work_order, etc.) ---
# --- permanecen exactamente iguales. ---

@router.get("/{wo_id}", response_model=schemas.LiquidationDetailsResponse)
async def get_work_order_details_combo(
    wo_id: int, 
    auth: AuthDependency, 
    company_id: int = Query(...) # <-- ¡Este es el cambio!
):
    """ 
    [COMBO] Obtiene TODOS los datos necesarios para la
    vista de detalle de Liquidación en una sola llamada.
    """
    if "liquidaciones.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        combo_data, error = await asyncio.to_thread(
            db.get_liquidation_details_combo, wo_id, company_id
        )
        
        if error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error)
        
        return combo_data

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener detalle de OT: {e}")

@router.post("/", response_model=schemas.WorkOrderResponse, status_code=status.HTTP_201_CREATED)
async def create_work_order(
    work_order: schemas.WorkOrderCreate,
    auth: AuthDependency,
    company_id: int = 1 # Fijo por ahora
):
    """ Crea una nueva Orden de Trabajo. """
    if "liquidaciones.can_create" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_wo_id = db.create_work_order(
            company_id=company_id,
            ot_number=work_order.ot_number,
            customer=work_order.customer_name,
            address=work_order.address,
            service=work_order.service_type,
            job_type=work_order.job_type
        )
        
        wo_raw = db.get_work_orders_filtered_sorted(
            company_id=company_id,
            filters={'id': new_wo_id},
            limit=1,
            offset=0
        )
        if not wo_raw:
             raise HTTPException(status_code=404, detail="Se creó la OT pero no se pudo encontrar.")

        return dict(wo_raw[0])

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.put("/{wo_id}/save", status_code=status.HTTP_200_OK)
async def save_liquidation_progress(
    wo_id: int,
    data: schemas.WorkOrderSaveRequest,
    auth: AuthDependency,
    # --- ¡CORRECCIÓN AQUÍ! ---
    # Ya no es un default, ahora es un Query parameter obligatorio
    company_id: int = Query(...) 
):
    """
    Guarda el progreso de una liquidación (actualiza la OT y los pickings 'draft').
    """
    if "liquidaciones.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        user_name = auth.username
        
        consumo_data_dict = data.consumo_data.dict()
        retiro_data_dict = data.retiro_data.dict() if data.retiro_data else None
        
        # ¡Ahora 'company_id' viene de la URL!
        success, message = db.save_liquidation_progress(
            wo_id=wo_id,
            wo_updates=data.wo_updates,
            consumo_data=consumo_data_dict,
            retiro_data=retiro_data_dict,
            company_id=company_id, # <-- ¡Pasa el ID correcto!
            user_name=user_name
        )
        if not success:
            raise HTTPException(status_code=400, detail=message)
        return {"message": message}
        
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.post("/{wo_id}/liquidate", status_code=status.HTTP_200_OK)
async def liquidate_work_order(
    wo_id: int,
    data: schemas.WorkOrderSaveRequest, # Reusa el mismo schema de guardado
    auth: AuthDependency,
    company_id: int = 1 # Fijo por ahora
):
    """
    Valida y Liquida una OT. 
    Primero guarda el progreso (como /save) y luego valida los pickings.
    """
    if "liquidaciones.can_liquidate" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        user_name = auth.username
        
        consumo_data_dict = data.consumo_data.dict()
        retiro_data_dict = data.retiro_data.dict() if data.retiro_data else None
        
        consumptions = consumo_data_dict.get('lines_data', [])
        retiros = retiro_data_dict.get('lines_data', []) if retiro_data_dict else []
        
        success, message = db.process_full_liquidation(
            wo_id=wo_id,
            consumptions=consumptions, # Pasa la lista de líneas
            retiros=retiros, # Pasa la lista de líneas
            service_act_number=data.consumo_data.service_act_number,
            date_attended_db=data.consumo_data.date_attended_db,
            current_ui_location_id=data.consumo_data.location_src_id,
            user_name=user_name
        )
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
        return {"message": message}
        
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")