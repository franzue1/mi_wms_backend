# app/api/pickings.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel # <-- AÑADIR ESTA LÍNEA
from datetime import date # <-- AÑADIR ESTA LÍNEA
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Función Helper de Filtros (Movida al inicio) ---
def _build_picking_filters(type_code: str, filters_in: dict):
    filter_map = {
        'name': 'p.name', 'purchase_order': 'p.purchase_order',
        'src_path': 'src_path_display', 'dest_path': 'dest_path_display',
        'warehouse_src_name': 'w_src.name', 'warehouse_dest_name': 'w_dest.name',
        'state': 'p.state', 'custom_operation_type': 'p.custom_operation_type',
        'partner_ref': 'p.partner_ref', 'responsible_user': 'p.responsible_user',
        'date_transfer_from': 'date_transfer_from', 'date_transfer_to': 'date_transfer_to'
    }
    clean_filters = {}
    for api_key, db_key in filter_map.items():
        if filters_in.get(api_key):
            clean_filters[db_key] = filters_in[api_key]
    return clean_filters

# --- Endpoints de Lista (Lectura) ---

@router.get("/", response_model=List[dict])
async def get_all_pickings(
    auth: AuthDependency, type_code: str, company_id: int = 1, skip: int = 0, limit: int = 25,
    sort_by: Optional[str] = Query(None), ascending: bool = Query(False),
    name: Optional[str] = Query(None), purchase_order: Optional[str] = Query(None),
    src_path: Optional[str] = Query(None), dest_path: Optional[str] = Query(None),
    warehouse_src_name: Optional[str] = Query(None), warehouse_dest_name: Optional[str] = Query(None),
    state: Optional[str] = Query(None), custom_operation_type: Optional[str] = Query(None),
    partner_ref: Optional[str] = Query(None), responsible_user: Optional[str] = Query(None),
    date_transfer_from: Optional[str] = Query(None), date_transfer_to: Optional[str] = Query(None)
):
    if "operations.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    filters_dict = locals()
    clean_filters = _build_picking_filters(type_code, filters_dict)
    try:
        pickings_raw = db.get_pickings_by_type(
            picking_type_code=type_code, company_id=company_id, filters=clean_filters,
            sort_by=sort_by or 'id', ascending=ascending, limit=limit, offset=skip
        )
        return [dict(p) for p in pickings_raw]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener pickings: {e}")

@router.get("/count", response_model=int)
async def get_pickings_count(
    auth: AuthDependency, type_code: str, company_id: int = 1,
    name: Optional[str] = Query(None), purchase_order: Optional[str] = Query(None),
    src_path: Optional[str] = Query(None), dest_path: Optional[str] = Query(None),
    warehouse_src_name: Optional[str] = Query(None), warehouse_dest_name: Optional[str] = Query(None),
    state: Optional[str] = Query(None), custom_operation_type: Optional[str] = Query(None),
    partner_ref: Optional[str] = Query(None), responsible_user: Optional[str] = Query(None),
    date_transfer_from: Optional[str] = Query(None), date_transfer_to: Optional[str] = Query(None)
):
    if "operations.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    filters_dict = locals()
    clean_filters = _build_picking_filters(type_code, filters_dict)
    try:
        count = db.get_pickings_count(type_code, company_id, filters=clean_filters)
        return count
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al contar pickings: {e}")

@router.get("/{picking_id}", response_model=schemas.PickingResponse)
async def get_picking_details(picking_id: int, auth: AuthDependency):
    if "operations.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    picking_header, picking_moves_raw = db.get_picking_details(picking_id)
    if not picking_header:
        raise HTTPException(status_code=404, detail="Albarán no encontrado")
    response_data = dict(picking_header)
    response_data["moves"] = [dict(move) for move in picking_moves_raw]
    return response_data

@router.get("/{picking_id}/serials", response_model=Dict[int, Dict[str, float]])
async def get_picking_serials(picking_id: int, auth: AuthDependency):
    """
    Obtiene las series/lotes ya guardados para un albarán (stock_move_lines).
    Devuelve un mapa: {move_id: {"serial_name": qty, ...}}
    """
    if "operations.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        serials_data = db.get_serials_for_picking(picking_id)
        return serials_data
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener series: {e}")

class PickingCreateRequest(BaseModel):
    picking_type_id: int
    company_id: int
    responsible_user: str

@router.post("/create-draft", response_model=schemas.PickingResponse, status_code=201)
async def create_draft_picking(data: PickingCreateRequest, auth: AuthDependency):
    """ Crea un nuevo albarán en estado 'borrador'. """
    if "operations.can_create" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")
    try:
        pt_details = db.get_picking_type_details(data.picking_type_id)
        if not pt_details:
            raise HTTPException(status_code=404, detail="Tipo de operación no encontrado")
        new_name = db.get_next_picking_name(data.picking_type_id)
        
        new_picking_id = db.create_picking(
            name=new_name,
            picking_type_id=data.picking_type_id,
            location_src_id=pt_details['default_location_src_id'],
            location_dest_id=pt_details['default_location_dest_id'],
            company_id=data.company_id,
            responsible_user=data.responsible_user
        )
        # Devolvemos el picking recién creado
        return await get_picking_details(new_picking_id, auth)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al crear albarán: {e}")

@router.post("/{picking_id}/mark-ready", status_code=200)
async def mark_picking_ready(picking_id: int, auth: AuthDependency):
    if "operations.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    success, message = db.check_stock_for_picking(picking_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    rows_affected = db.mark_picking_as_ready(picking_id)
    if rows_affected == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar (quizás no estaba en 'draft')")
    return {"message": "Albarán marcado como 'listo'. Stock reservado."}

@router.post("/{picking_id}/validate", status_code=200)
async def validate_picking(picking_id: int, tracking_data: schemas.ValidateRequest, auth: AuthDependency):
    if "operations.can_validate" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    success, message = db.process_picking_validation(picking_id, tracking_data.moves_with_tracking)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return {"message": message}

@router.post("/{picking_id}/return-to-draft", status_code=200)
async def return_picking_to_draft(picking_id: int, auth: AuthDependency):
    if "operations.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    success, message = db.return_picking_to_draft(picking_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

@router.delete("/{picking_id}", status_code=200)
async def cancel_picking(picking_id: int, auth: AuthDependency):
    """ Cancela un albarán (pasa a estado 'cancelled'). """
    if "operations.can_edit" not in auth.permissions: # Asumimos que editar permite cancelar
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    success, message = db.cancel_picking(picking_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}

# --- Endpoints para editar la Cabecera y Líneas ---

class PickingHeaderUpdate(BaseModel):
    location_src_id: Optional[int] = None
    location_dest_id: Optional[int] = None
    partner_ref: Optional[str] = None
    date_transfer: Optional[date] = None
    purchase_order: Optional[str] = None
    custom_operation_type: Optional[str] = None
    partner_id: Optional[int] = None

@router.put("/{picking_id}/header", status_code=200)
async def update_picking_header(picking_id: int, data: PickingHeaderUpdate, auth: AuthDependency):
    """ Actualiza campos específicos de la cabecera de un albarán. """
    if "operations.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        db.update_picking_header(
            pid=picking_id,
            src_id=data.location_src_id,
            dest_id=data.location_dest_id,
            ref=data.partner_ref,
            date_transfer=data.date_transfer,
            purchase_order=data.purchase_order,
            custom_op_type=data.custom_operation_type,
            partner_id=data.partner_id
        )
        return {"message": "Cabecera actualizada."}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al actualizar cabecera: {e}")

class StockMoveCreate(BaseModel):
    product_id: int
    qty: float
    loc_src_id: Optional[int] = None # <-- CAMBIO
    loc_dest_id: Optional[int] = None # <-- CAMBIO
    price_unit: float = 0
    partner_id: Optional[int] = None

@router.post("/{picking_id}/moves", response_model=dict, status_code=201)
async def add_stock_move(picking_id: int, move_data: StockMoveCreate, auth: AuthDependency):
    """ Añade una nueva línea (stock_move) a un albarán 'draft'. """
    if "operations.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        new_move_id = db.add_stock_move_to_picking(
            picking_id=picking_id,
            product_id=move_data.product_id,
            qty=move_data.qty,
            loc_src_id=move_data.loc_src_id,
            loc_dest_id=move_data.loc_dest_id,
            price_unit=move_data.price_unit,
            partner_id=move_data.partner_id
        )
        return {"id": new_move_id}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al añadir línea: {e}")

@router.delete("/moves/{move_id}", status_code=200)
async def delete_stock_move(move_id: int, auth: AuthDependency):
    """ Elimina una línea (stock_move) de un albarán 'draft'. """
    if "operations.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        db.delete_stock_move(move_id)
        return {"message": "Línea eliminada."}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al eliminar línea: {e}")

class MoveQuantityUpdate(BaseModel):
    quantity: float

@router.put("/moves/{move_id}/quantity", status_code=200)
async def update_move_quantity(move_id: int, data: MoveQuantityUpdate, auth: AuthDependency):
    """ Actualiza la cantidad de una línea 'draft'. """
    if "operations.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        db.update_move_quantity_done(move_id, data.quantity)
        return {"message": "Cantidad actualizada."}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al actualizar cantidad: {e}")

@router.put("/moves/{move_id}/tracking", status_code=200)
async def save_move_lines(move_id: int, tracking_data: Dict[str, float], auth: AuthDependency):
    """ Guarda/actualiza las series/lotes para una línea de movimiento. """
    if "operations.can_validate" not in auth.permissions: # Requiere permiso de validación
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        success, message = db.save_move_lines_for_move(move_id, tracking_data)
        if not success:
            raise HTTPException(status_code=400, detail=message)
        return {"message": message}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al guardar series: {e}")

# --- Endpoints de Stock (Vivos) ---
# (Estos endpoints son necesarios para la UI de detalle)

@router.get("/stock/available", response_model=float)
async def get_real_available_stock(
    auth: AuthDependency,
    product_id: int,
    location_id: int
):
    """ Obtiene el stock disponible real (físico - reservado). """
    try:
        stock = db.get_real_available_stock(product_id, location_id)
        return stock
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stock/available-serials", response_model=List[dict])
async def get_available_serials(
    auth: AuthDependency,
    product_id: int,
    location_id: int
):
    """ Obtiene las series disponibles (físicas - reservadas). """
    try:
        serials_raw = db.get_available_serials_at_location(product_id, location_id)
        return [dict(s) for s in serials_raw]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Endpoints de Soporte (Helpers para Dropdowns) ---

@router.get("/helpers/operation-types", response_model=List[dict])
async def get_operation_types(auth: AuthDependency, code: str):
    data = db.get_operation_types_by_code(code)
    return [dict(row) for row in data]

@router.get("/helpers/operation-rule", response_model=dict)
async def get_operation_rule(auth: AuthDependency, name: str):
    data = db.get_operation_type_details_by_name(name)
    if not data:
        raise HTTPException(status_code=404, detail="Regla no encontrada")
    return dict(data)

@router.get("/helpers/warehouses-by-category", response_model=List[dict])
async def get_warehouses_by_category(
    auth: AuthDependency, 
    categories: Optional[List[str]] = Query(None)  # <-- 1. Hazlo Opcional
):
    company_id = 1 # TODO: Obtener de la sesión/token
    
    # 2. Añade esta comprobación:
    if not categories:
        return [] # Devuelve una lista vacía si no se proporcionan categorías
        
    data = db.get_warehouses_by_categories(company_id, categories)
    return [dict(row) for row in data]

@router.get("/helpers/locations-by-warehouse", response_model=List[dict])
async def get_locations_by_warehouse(auth: AuthDependency, warehouse_id: int):
    data = db.get_locations_by_warehouse(warehouse_id)
    return [dict(row) for row in data]

@router.get("/helpers/partners-by-category", response_model=List[dict])
async def get_partners_by_category(auth: AuthDependency, category_name: str):
    company_id = 1 # TODO: Obtener de la sesión/token
    data = db.get_partners(company_id, category_name)
    return [dict(row) for row in data]

@router.get("/helpers/picking-type-details", response_model=dict)
async def get_picking_type_details(auth: AuthDependency, pt_id: int):
    data = db.get_picking_type_details(pt_id)
    if not data:
        raise HTTPException(status_code=404, detail="Tipo de Picking no encontrado")
    return dict(data)

@router.get("/helpers/location-details", response_model=dict)
async def get_location_details(auth: AuthDependency, loc_id: int):
    data = db.get_location_name_details(loc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Ubicación no encontrada")
    return dict(data)