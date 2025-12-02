# app/api/work_orders.py
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, UploadFile, File
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData, verify_company_access
import traceback
import asyncio
import io
import csv
from fastapi.responses import StreamingResponse

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper para parsear filtros ---
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
        'location_src_path', 'service_act_number', 'project_name'
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
    verify_company_access(auth, company_id) # <--- BLINDAJE

    if "liquidaciones.can_create" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
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
    data: schemas.WorkOrderSaveRequest, # Reusa el mismo schema de guardado
    auth: AuthDependency,
    company_id: int = Query(...)
):
    verify_company_access(auth, company_id) # <--- BLINDAJE
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
            company_id=company_id, # <-- ¡CORRECCIÓN 2!
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

@router.get("/export/csv", response_class=StreamingResponse)
async def export_work_orders_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    # (Opcional: puedes añadir los mismos filtros que 'get_all_work_orders' aquí si lo deseas)
    # name: Optional[str] = Query(None), 
    # phase: Optional[str] = Query(None),
):
    verify_company_access(auth, company_id)
    """
    [NUEVO] Genera y transmite un archivo CSV de las Órdenes de Trabajo.
    """
    if "liquidaciones.can_import_export" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        # 1. Por ahora, exportamos todo. 
        # (Podríamos añadir filtros aquí, pero empecemos simple)
        filters = {} 
        
        # 2. Llamar a una función de BD que obtenga *todos* los datos (sin paginación)
        #    (Asumiremos que 'get_work_orders_for_export' existe, la crearemos en el Paso 2)
        wo_data_raw = db.get_work_orders_for_export(company_id, filters=filters)

        if not wo_data_raw:
            raise HTTPException(status_code=404, detail="No hay datos para exportar.")

        # 3. Crear CSV en memoria
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        # 4. Escribir cabeceras (las mismas que tu frontend estaba usando)
        headers = ["ot_number", "customer_name", "address", "service_type", "job_type", "phase"]
        writer.writerow(headers)

        # 5. Escribir datos
        for wo_row in wo_data_raw:
            wo_dict = dict(wo_row)
            writer.writerow([wo_dict.get(h, '') for h in headers])

        output.seek(0)
        
        # 6. Devolver el archivo
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=ordenes_de_trabajo.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")

@router.post("/import/csv", response_model=dict)
async def import_work_orders_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    verify_company_access(auth, company_id)
    """
    [ACTUALIZADO] Importa OTs leyendo la columna de PROYECTO.
    Soporta: 'project', 'proyecto', 'project_name', 'obra'.
    """
    if "liquidaciones.can_import_export" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        content = await file.read()
        # Intentamos decodificar con utf-8-sig para quitar el BOM de Excel
        content_decoded = content.decode('utf-8-sig') 
        file_io = io.StringIO(content_decoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al leer el archivo: {e}")

    # Detectar delimitador automáticamente (Excel usa ; en regiones latinas, , en US)
    dialect = csv.Sniffer().sniff(content_decoded[:1024], delimiters=";,")
    file_io.seek(0)
    reader = csv.DictReader(file_io, dialect=dialect)

    try:
        rows_to_process = list(reader)
        if not rows_to_process: raise ValueError("El archivo CSV está vacío.")
        
        # Validar cabeceras mínimas
        headers = {h.lower().strip() for h in reader.fieldnames or []}
        required_headers = {"ot_number", "customer_name"} 
        if not required_headers.issubset(headers):
            missing = required_headers - headers
            raise ValueError(f"Faltan columnas obligatorias: {', '.join(sorted(list(missing)))}")

        created_count, updated_count, error_list = 0, 0, []
        
        for i, row in enumerate(rows_to_process):
            row_num = i + 2
            # Limpieza de claves y valores
            clean_row = {k.lower().strip(): v.strip() for k, v in row.items() if k}
            
            ot = clean_row.get('ot_number')
            cust = clean_row.get('customer_name')
            
            if not ot or not cust:
                error_list.append(f"Fila {row_num}: Faltan datos (OT o Cliente).")
                continue
            
            # --- CAPTURA INTELIGENTE DEL PROYECTO ---
            # Buscamos variaciones comunes del nombre de la columna
            project_name = (
                clean_row.get('project_name') or 
                clean_row.get('project') or 
                clean_row.get('proyecto') or 
                clean_row.get('obra')
            )
            # ----------------------------------------

            try:
                payload = {
                    "ot_number": ot,
                    "customer_name": cust,
                    "address": clean_row.get('address', ''),
                    "service_type": clean_row.get('service_type'),
                    "job_type": clean_row.get('job_type'),
                    "project_name": project_name # <-- Pasamos el nombre al repo
                }
                
                # Llamada al repositorio inteligente
                result = db.upsert_work_order_from_import(company_id, payload)
                
                if result == "created": created_count += 1
                elif result == "updated": updated_count += 1
                    
            except Exception as e:
                error_list.append(f"Fila {row_num} (OT: {ot}): {str(e)}")

        if error_list:
            # Si hay pocos errores, los mostramos. Si son muchos, mostramos resumen.
            detail_msg = "Errores en la importación:\n- " + "\n- ".join(error_list[:10])
            if len(error_list) > 10: detail_msg += f"\n... y {len(error_list)-10} errores más."
            
            raise HTTPException(status_code=400, detail=detail_msg)

        return {"created": created_count, "updated": updated_count, "errors": 0}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico al procesar CSV: {e}")

