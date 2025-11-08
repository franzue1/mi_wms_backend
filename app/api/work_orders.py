# app/api/work_orders.py
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.WorkOrderResponse])
async def get_all_work_orders(
    auth: AuthDependency,
    company_id: int = 1, # Fijo por ahora
    skip: int = 0,
    limit: int = 25
):
    """ Obtiene la lista de Órdenes de Trabajo (Liquidaciones). """
    if "liquidaciones.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    # TODO: Implementar filtros y sort completos
    wo_raw = db.get_work_orders_filtered_sorted(
        company_id, 
        filters={}, 
        sort_by='id', 
        ascending=False, 
        limit=limit, 
        offset=skip
    )
    return [dict(wo) for wo in wo_raw]

@router.get("/{wo_id}", response_model=schemas.WorkOrderResponse)
async def get_work_order_details(wo_id: int, auth: AuthDependency):
    """ Obtiene los detalles de una Orden de Trabajo (Liquidación) por su ID. """
    if "liquidaciones.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usamos company_id=1 (fijo) en lugar de auth.company_id
    company_id = 1 

    wo_raw = db.get_work_orders_filtered_sorted(
        company_id=company_id,
        filters={'id': wo_id}, # Filtramos por el ID
        limit=1,
        offset=0
    )
    
    if not wo_raw:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Orden de Trabajo no encontrada")
    
    return dict(wo_raw[0])

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
        created_wo = db.get_work_order_details(new_wo_id) # Usamos la consulta simple
        # Convertimos la fila a dict y añadimos campos dummy que espera la response
        # (Esto es temporal hasta que db.get_work_order_details sea tan completa como get_work_orders_filtered_sorted)
        response_data = dict(created_wo)
        response_data.update({
            'warehouse_name': None, 'location_src_path': None, 
            'service_act_number': None, 'attention_date_str': None
        })
        return response_data

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.put("/{wo_id}/save", status_code=status.HTTP_200_OK)
async def save_liquidation_progress(
    wo_id: int,
    data: schemas.WorkOrderSaveRequest,
    auth: AuthDependency,
    company_id: int = 1 # Fijo por ahora
):
    """
    Guarda el progreso de una liquidación (actualiza la OT y los pickings 'draft').
    """
    if "liquidaciones.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        user_name = auth.username
        
        # Convertir Pydantic models a dicts que la DB espera
        consumo_data_dict = data.consumo_data.dict()
        retiro_data_dict = data.retiro_data.dict() if data.retiro_data else None
        
        success, message = db.save_liquidation_progress(
            wo_id=wo_id,
            wo_updates=data.wo_updates,
            consumo_data=consumo_data_dict,
            retiro_data=retiro_data_dict,
            company_id=company_id,
            user_name=user_name
        )
        if not success:
            raise HTTPException(status_code=400, detail=message)
        return {"message": message}
        
    except Exception as e:
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
        
        # Convertir Pydantic models a dicts que la DB espera
        consumo_data_dict = data.consumo_data.dict()
        retiro_data_dict = data.retiro_data.dict() if data.retiro_data else None
        
        # Extraer los datos necesarios para la validación final
        consumptions = [line['product_id'] for line in consumo_data_dict.get('lines_data', [])]
        retiros = [line['product_id'] for line in retiro_data_dict.get('lines_data', [])]
        
        # Simplificamos: La lógica de la UI debe garantizar que los datos en 'data' son correctos.
        # La validación real (check_stock, etc.) ocurre dentro de process_full_liquidation.
        
        success, message = db.process_full_liquidation(
            wo_id=wo_id,
            consumptions=consumo_data_dict['lines_data'], # Pasa la lista de líneas
            retiros=retiro_data_dict['lines_data'] if retiro_data_dict else [], # Pasa la lista de líneas
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