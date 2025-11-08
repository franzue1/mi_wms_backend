# app/api/adjustments.py
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.AdjustmentListResponse])
async def get_all_adjustments(
    auth: AuthDependency,
    company_id: int = 1,
    skip: int = 0,
    limit: int = 25
):
    """ Obtiene la lista de Ajustes de Inventario. """
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    adjustments_raw = db.get_adjustments_filtered_sorted(
        company_id, filters={}, sort_by='id', ascending=False, limit=limit, offset=skip
    )
    return [dict(adj) for adj in adjustments_raw]

@router.post("/", response_model=schemas.PickingResponse, status_code=status.HTTP_201_CREATED)
async def create_adjustment(auth: AuthDependency, company_id: int = 1):
    """ Crea un nuevo borrador de ajuste de inventario. """
    if "adjustments.can_create" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_picking_id = db.create_draft_adjustment(company_id, auth.username)
        if not new_picking_id:
            raise ValueError("No se pudo crear el borrador (verifique config de picking_types y locations 'ADJ')")
        
        # Devolvemos el picking completo (cabecera y líneas vacías)
        picking_header, picking_moves = db.get_picking_details(new_picking_id)
        response_data = dict(picking_header)
        response_data["moves"] = [dict(move) for move in picking_moves]
        return response_data

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.get("/{adjustment_id}", response_model=schemas.PickingResponse)
async def get_adjustment_details(adjustment_id: int, auth: AuthDependency):
    """ Obtiene la cabecera y líneas de un ajuste (que es un picking). """
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    picking_header, picking_moves = db.get_picking_details(adjustment_id)
    if not picking_header:
        raise HTTPException(status_code=404, detail="Ajuste no encontrado")
        
    response_data = dict(picking_header)
    response_data["moves"] = [dict(move) for move in picking_moves]
    return response_data

@router.put("/{adjustment_id}", status_code=status.HTTP_200_OK)
async def save_adjustment(
    adjustment_id: int,
    data: schemas.AdjustmentSaveRequest,
    auth: AuthDependency
):
    """ Guarda el progreso (cabecera y líneas) de un ajuste en borrador. """
    if "adjustments.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    success, msg, _ = db.save_adjustment_draft(adjustment_id, data.header_data, data.lines_data)
    if not success:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg}

@router.post("/{adjustment_id}/mark-ready", status_code=status.HTTP_200_OK)
async def mark_adjustment_ready(adjustment_id: int, auth: AuthDependency):
    """ Pasa un ajuste a 'listo' (valida stock FÍSICO). """
    if "adjustments.can_edit" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    success, message = db.check_stock_for_picking(adjustment_id)
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    rows_affected = db.mark_picking_as_ready(adjustment_id)
    if rows_affected == 0:
        raise HTTPException(status_code=400, detail="No se pudo actualizar (quizás no estaba en 'draft')")

    return {"message": "Ajuste marcado como 'listo'."}

@router.post("/{adjustment_id}/validate", status_code=status.HTTP_200_OK)
async def validate_adjustment(
    adjustment_id: int, 
    tracking_data: schemas.ValidateRequest,
    auth: AuthDependency
):
    """ Valida un ajuste ('listo' -> 'hecho') y mueve el stock físico. """
    if "adjustments.can_validate" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    success, message = db.process_picking_validation(
        adjustment_id, 
        tracking_data.moves_with_tracking
    )
    if not success:
        raise HTTPException(status_code=400, detail=message)
    return {"message": message}