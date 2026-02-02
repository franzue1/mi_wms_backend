# app/api/work_orders.py
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, UploadFile, File
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData, verify_company_access
from app.services.work_order_service import WorkOrderService
from app.exceptions import ValidationError, BusinessRuleError, NotFoundError
import traceback
import asyncio
from fastapi.responses import StreamingResponse

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper para parsear filtros ---
def _parse_filters_from_request(request: Request) -> dict:
    """
    Lee los query params de la URL y los convierte en un dict de filtros.
    Usa WorkOrderService.build_wo_filter_dict para normalización.
    """
    params = dict(request.query_params)

    return WorkOrderService.build_wo_filter_dict(
        ot_number=params.get('ot_number'),
        customer_name=params.get('customer_name'),
        address=params.get('address'),
        service_type=params.get('service_type'),
        job_type=params.get('job_type'),
        phase=params.get('phase'),
        warehouse_name=params.get('warehouse_name'),
        location_src_path=params.get('location_src_path'),
        service_act_number=params.get('service_act_number'),
        project_name=params.get('project_name'),
    )
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
    verify_company_access(auth, company_id) # <--- BLINDAJE DE SEGURIDAD
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
    verify_company_access(auth, company_id) # <--- BLINDAJE
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

@router.get("/{wo_id}", response_model=schemas.LiquidationDetailsResponse)
async def get_work_order_details_combo(
    wo_id: int, 
    auth: AuthDependency, 
    company_id: int = Query(...) # <-- ¡Este es el cambio!
):
    verify_company_access(auth, company_id) # <--- BLINDAJE
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
    company_id: int = Query(...)
):
    """ Crea una nueva Orden de Trabajo vinculada a un Proyecto. """
    verify_company_access(auth, company_id)

    if "liquidaciones.can_create" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        # Validar datos usando el servicio
        WorkOrderService.validate_ot_number(work_order.ot_number)
        WorkOrderService.validate_customer_name(work_order.customer_name)

        new_wo_id = db.create_work_order(
            company_id=company_id,
            ot_number=work_order.ot_number,
            customer=work_order.customer_name,
            address=work_order.address,
            service=work_order.service_type,
            job_type=work_order.job_type,
            project_id=work_order.project_id
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

    except ValidationError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=ve.message)
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
    company_id: int = Query(...) 
):
    verify_company_access(auth, company_id) # <--- BLINDAJE
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
    data: schemas.WorkOrderSaveRequest,
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """
    Valida y Liquida una OT.
    Primero guarda el progreso (como /save) y luego valida los pickings.
    """
    verify_company_access(auth, company_id)

    if "liquidaciones.can_liquidate" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        user_name = auth.username
        consumo_data_dict = data.consumo_data.dict()
        retiro_data_dict = data.retiro_data.dict() if data.retiro_data else None

        consumptions = consumo_data_dict.get('lines_data', [])
        retiros = retiro_data_dict.get('lines_data', []) if retiro_data_dict else []

        # Validar datos usando el servicio
        WorkOrderService.validate_liquidation_has_lines(consumptions, retiros)

        success, message = db.process_full_liquidation(
            wo_id=wo_id,
            company_id=company_id,
            consumptions=consumptions,
            retiros=retiros,
            service_act_number=data.consumo_data.service_act_number,
            date_attended_db=data.consumo_data.date_attended_db,
            current_ui_location_id=data.consumo_data.location_src_id,
            user_name=user_name
        )

        if not success:
            raise HTTPException(status_code=400, detail=message)
        return {"message": message}

    except ValidationError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=ve.message)
    except BusinessRuleError as bre:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=bre.message)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.get("/export/csv", response_class=StreamingResponse)
async def export_work_orders_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
):
    """
    Genera y transmite un archivo CSV de las Órdenes de Trabajo.
    Usa WorkOrderService para generación de CSV.
    """
    verify_company_access(auth, company_id)

    if "liquidaciones.can_import_export" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        filters = {}
        wo_data_raw = db.get_work_orders_for_export(company_id, filters=filters)

        if not wo_data_raw:
            raise NotFoundError("No hay datos para exportar", "EXPORT_NO_DATA")

        # Usar el servicio para generar el CSV
        csv_content = WorkOrderService.generate_csv_content(wo_data_raw)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=ordenes_de_trabajo.csv"}
        )

    except NotFoundError as nfe:
        raise HTTPException(status_code=404, detail=nfe.message)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")

@router.post("/import/csv", response_model=dict)
async def import_work_orders_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """
    Importa OTs desde CSV.
    Usa WorkOrderService para parsing y validación.
    """
    verify_company_access(auth, company_id)

    if "liquidaciones.can_import_export" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        content = await file.read()

        # Usar el servicio para parsear el CSV
        rows, headers = WorkOrderService.parse_csv_file(content)

        # Validar headers usando el servicio
        WorkOrderService.validate_csv_headers(headers)

        created_count, updated_count, error_list = 0, 0, []

        for i, row in enumerate(rows):
            row_num = i + 2

            try:
                # Usar el servicio para procesar la fila
                processed = WorkOrderService.process_csv_row(row, row_num)

                # Llamada al repositorio
                result = db.upsert_work_order_from_import(company_id, processed)

                if result == "created":
                    created_count += 1
                elif result == "updated":
                    updated_count += 1

            except ValidationError as ve:
                error_list.append(ve.message)
            except Exception as e:
                ot = row.get('ot_number', 'N/A')
                error_list.append(f"Fila {row_num} (OT: {ot}): {str(e)}")

        if error_list:
            detail_msg = "Errores en la importación:\n- " + "\n- ".join(error_list[:10])
            if len(error_list) > 10:
                detail_msg += f"\n... y {len(error_list) - 10} errores más."
            raise HTTPException(status_code=400, detail=detail_msg)

        return {"created": created_count, "updated": updated_count, "errors": 0}

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico al procesar CSV: {e}")

