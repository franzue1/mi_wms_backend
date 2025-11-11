# app/api/pickings.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel
from datetime import date, datetime
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import io
import csv
from fastapi.responses import StreamingResponse
import asyncio
from collections import defaultdict

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

@router.get("/{picking_id}/ui-details", response_model=dict)
async def get_picking_ui_details(picking_id: int, auth: AuthDependency, company_id: int = 1):
    """
    [COMBO-OPTIMIZADO-JSON] Obtiene la mayoría de los datos en una
    sola consulta a la BD y luego obtiene los almacenes dinámicos.
    """
    if "operations.can_view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    print(f"\n[API-COMBO-JSON] Obteniendo UI-Details para Picking ID: {picking_id}")

    try:
        # --- 1. ¡UNA SOLA LLAMADA PARA CASI TODO! ---
        # Usamos to_thread porque la consulta JSON es pesada y es mejor
        # no bloquear el bucle de eventos principal.
        ui_data, error = await asyncio.to_thread(
            db.get_picking_ui_details_optimized, picking_id, company_id
        )
        if error:
            raise Exception(error)
        
        # --- 2. Lógica Dinámica de Almacenes (que sigue en Python) ---
        op_rule = ui_data.get("op_rule")
        wh_origin_list, wh_dest_list = [], []

        if op_rule:
            source_type = op_rule.get('source_location_category')
            dest_type = op_rule.get('destination_location_category')
            op_name = op_rule.get('name')
            
            # (Pega tu lógica de 'allowed_origin_wh_categories' aquí)
            allowed_origin_wh_categories = []
            if source_type == 'internal':
                if op_name == "Transferencia entre Almacenes": allowed_origin_wh_categories = ["ALMACEN PRINCIPAL"]
                # ... (todas tus otras reglas 'elif') ...
                else: allowed_origin_wh_categories = ["ALMACEN PRINCIPAL", "CONTRATISTA"]
            
            allowed_dest_wh_categories = []
            if dest_type == 'internal':
                if op_name == "Compra Nacional": allowed_dest_wh_categories = ["ALMACEN PRINCIPAL"]
                # ... (todas tus otras reglas 'elif') ...
                else: allowed_dest_wh_categories = ["ALMACEN PRINCIPAL", "CONTRATISTA"]

            # --- 3. Consultas 2 y 3 (Ligeras y en Paralelo) ---
            tasks = []
            if allowed_origin_wh_categories:
                tasks.append(asyncio.to_thread(db.get_warehouses_by_categories, company_id, allowed_origin_wh_categories))
            else:
                tasks.append(asyncio.to_thread(lambda: [])) # Placeholder
                
            if allowed_dest_wh_categories:
                tasks.append(asyncio.to_thread(db.get_warehouses_by_categories, company_id, allowed_dest_wh_categories))
            else:
                tasks.append(asyncio.to_thread(lambda: [])) # Placeholder

            results = await asyncio.gather(*tasks)
            wh_origin_list = [dict(w) for w in results[0]]
            wh_dest_list = [dict(w) for w in results[1]]
        
        # --- 4. Añadir los almacenes al JSON y devolver ---
        ui_data["dropdown_options"]["warehouses_origin"] = wh_origin_list
        ui_data["dropdown_options"]["warehouses_dest"] = wh_dest_list
        
        return ui_data

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener UI-Details: {e}")

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

@router.get("/export/csv", response_class=StreamingResponse)
async def export_pickings_csv(
    auth: AuthDependency,
    company_id: int = 1,
    export_type: str = Query("headers", enum=["headers", "full"])
):
    """
    [MIGRADO] Genera y transmite un archivo CSV de las operaciones.
    Llama a la función de BD 'get_data_for_export'.
    """
    if "operations.can_import_export" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        # 1. Obtener los datos de la misma función que usaba Flet
        data_to_export = db.get_data_for_export(company_id, export_type)
        
        if not data_to_export:
            raise HTTPException(status_code=404, detail="No hay datos para exportar.")

        # 2. Definir las cabeceras (la misma lógica que tenías en Flet)
        headers = []
        if export_type == 'headers':
            headers = [
                'picking_name', 'picking_type_code', 'state', 'custom_operation_type',
                'almacen_origen', 'ubicacion_origen',
                'almacen_destino', 'ubicacion_destino',
                'partner_ref', 'purchase_order',
                'date_transfer', 'responsible_user'
            ]
        elif export_type == 'full':
            headers = [
                'picking_name', 'picking_type_code', 'state', 'custom_operation_type',
                'almacen_origen', 'ubicacion_origen',
                'almacen_destino', 'ubicacion_destino',
                'partner_ref', 'purchase_order',
                'date_transfer', 'responsible_user',
                'product_sku', 'product_name', 'quantity', 'price_unit', 'serial'
            ]
        
        # 3. Crear el CSV en memoria
        output = io.StringIO(newline='')
        writer = csv.DictWriter(output, fieldnames=headers, delimiter=';', extrasaction='ignore')
        
        writer.writeheader()
        writer.writerows(data_to_export) # Escribir todas las filas

        output.seek(0)
        
        # 4. Devolver el archivo
        filename = f"operaciones_{export_type}.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")

@router.post("/import/csv", response_model=dict)
async def import_pickings_csv(
    auth: AuthDependency,
    import_type: str = Query(..., enum=["headers", "full"]),
    company_id: int = 1,
    file: UploadFile = File(...)
):
    """
    [MIGRADO] Importa operaciones (cabeceras o completo) desde un CSV.
    [CORREGIDO] Llama a las funciones de 'db' sin pasar el cursor.
    """
    if "operations.can_import_export" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    print(f"\n--- Iniciando Importación (API) '{import_type}' (Todo o Nada) ---")

    try:
        content = await file.read()
        content_decoded = content.decode('utf-8-sig')
        file_io = io.StringIO(content_decoded)

        sniffer = csv.Sniffer()
        try: dialect = sniffer.sniff(content_decoded[:2048], delimiters=';,')
        except csv.Error: dialect = csv.excel; dialect.delimiter = ';'

        file_io.seek(0)
        reader = csv.DictReader(file_io, dialect=dialect)

        headers_csv = [h.lower().strip() for h in reader.fieldnames or []]
        rows_to_process = [{k.lower().strip() if k else k : v.strip() if v else v for k, v in row_raw.items()} for row_raw in reader]

        if not rows_to_process:
            raise ValueError("El archivo CSV está vacío.")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al leer el archivo: {e}")


    all_errors = []
    validated_data = []
    responsible_user = auth.username

    try:
        # ============================================
        # --- FASE 1: VALIDACIÓN (SIN BD WRITES) ---
        # ============================================
        print("--- Fase 1: Iniciando Validación ---")

        if import_type == 'headers':
            required_headers = {'custom_operation_type', 'ubicacion_origen', 'ubicacion_destino', 'date_transfer'}
            if not required_headers.issubset(set(headers_csv)):
                missing = required_headers - set(headers_csv); raise ValueError(f"Faltan columnas: {', '.join(missing)}")

            for i, row in enumerate(rows_to_process):
                row_num = i + 2; current_errors = []
                validated_row_data = {'row_num': row_num, 'original_data': row}
                try:
                    op_type_name = row.get('custom_operation_type')
                    almacen_origen_csv = row.get('almacen_origen'); ubicacion_origen_csv = row.get('ubicacion_origen')
                    almacen_destino_csv = row.get('almacen_destino'); ubicacion_destino_csv = row.get('ubicacion_destino')
                    date_str = row.get('date_transfer')
                    partner_ref = row.get('partner_ref', ''); purchase_order = row.get('purchase_order', '')

                    if not all([op_type_name, ubicacion_origen_csv, ubicacion_destino_csv, date_str]):
                        current_errors.append("Faltan datos (Tipo Op, Ubicacion Origen/Destino o Fecha).")
                    else:
                        op_rule = db.get_operation_type_details_by_name(op_type_name)
                        if not op_rule: current_errors.append(f"Tipo op '{op_type_name}' no encontrado.")
                        else:
                            validated_row_data['op_rule'] = op_rule
                            op_code = op_rule['code']; expected_source_type = op_rule['source_location_category']; expected_dest_type = op_rule['destination_location_category']
                            src_loc_id, dest_loc_id, partner_id, wh_id_for_pt = None, None, None, None

                            # Validar Origen
                            if expected_source_type == 'vendor':
                                partner_info = db.get_partner_id_by_name(ubicacion_origen_csv, company_id)
                                if partner_info is None or partner_info['category_name'] != 'Proveedor Externo': current_errors.append(f"Proveedor Origen '{ubicacion_origen_csv}' inválido.")
                                else: partner_id = partner_info['id']
                            elif expected_source_type == 'customer':
                                partner_info = db.get_partner_id_by_name(ubicacion_origen_csv, company_id)
                                if partner_info is None or partner_info['category_name'] != 'Proveedor Cliente': current_errors.append(f"Cliente Origen '{ubicacion_origen_csv}' inválido.")
                                else: partner_id = partner_info['id']
                            elif expected_source_type == 'internal':
                                if not almacen_origen_csv: current_errors.append("Falta Almacén Origen.")
                                else:
                                    source_loc_details = db.get_location_details_by_names(company_id, almacen_origen_csv, ubicacion_origen_csv)
                                    if source_loc_details is None: current_errors.append(f"Ubicación Origen '{almacen_origen_csv}/{ubicacion_origen_csv}' inválida.")
                                    else: src_loc_id = source_loc_details['id']; wh_id_for_pt = source_loc_details['warehouse_id']

                            # Validar Destino
                            if expected_dest_type == 'vendor':
                                partner_info = db.get_partner_id_by_name(ubicacion_destino_csv, company_id)
                                if partner_info is None or partner_info['category_name'] != 'Proveedor Externo': current_errors.append(f"Proveedor Destino '{ubicacion_destino_csv}' inválido.")
                                else: partner_id = partner_info['id']
                            elif expected_dest_type == 'customer':
                                partner_info = db.get_partner_id_by_name(ubicacion_destino_csv, company_id)
                                if partner_info is None or partner_info['category_name'] != 'Proveedor Cliente': current_errors.append(f"Cliente Destino '{ubicacion_destino_csv}' inválido.")
                                else: partner_id = partner_info['id']
                            elif expected_dest_type == 'internal':
                                if not almacen_destino_csv: current_errors.append("Falta Almacén Destino.")
                                else:
                                    dest_loc_details = db.get_location_details_by_names(company_id, almacen_destino_csv, ubicacion_destino_csv)
                                    if dest_loc_details is None: current_errors.append(f"Ubicación Destino '{almacen_destino_csv}/{ubicacion_destino_csv}' inválida.")
                                    else:
                                        dest_loc_id = dest_loc_details['id']
                                        if wh_id_for_pt is None: wh_id_for_pt = dest_loc_details['warehouse_id']

                            if not current_errors:
                                picking_type_id = db.find_picking_type_id(company_id, op_code, wh_id_for_pt)
                                if not picking_type_id: current_errors.append(f"No se encontró tipo albarán '{op_code}' para almacén ID '{wh_id_for_pt}'.")
                                else: validated_row_data['picking_type_id'] = picking_type_id
                            try: date_transfer_db = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d"); validated_row_data['date_transfer_db'] = date_transfer_db
                            except ValueError: current_errors.append(f"Formato fecha '{date_str}' incorrecto (DD/MM/YYYY).")

                    validated_row_data['src_loc_id'] = src_loc_id; validated_row_data['dest_loc_id'] = dest_loc_id
                    validated_row_data['partner_id'] = partner_id; validated_row_data['partner_ref'] = partner_ref
                    validated_row_data['purchase_order'] = purchase_order
                except Exception as val_err:
                    current_errors.append(f"Error inesperado: {val_err}")
                if current_errors: all_errors.extend([f"Fila {row_num}: {err}" for err in current_errors])
                else: validated_data.append(validated_row_data)

        elif import_type == 'full':
            required_headers = {'documento_origen', 'custom_operation_type', 'ubicacion_origen', 'ubicacion_destino', 'date_transfer', 'product_sku', 'quantity'}
            if not required_headers.issubset(set(headers_csv)):
                missing = required_headers - set(headers_csv); raise ValueError(f"Faltan columnas: {', '.join(missing)}")

            grouped_rows = defaultdict(list)
            for row_num_idx, row in enumerate(rows_to_process):
                doc_origen = row.get('documento_origen'); line_num = row_num_idx + 2
                if not doc_origen: all_errors.append(f"Fila {line_num}: Sin 'documento_origen'."); continue
                grouped_rows[doc_origen].append({'data': row, 'line_num': line_num})

            for doc_origen, lines_with_nums in grouped_rows.items():
                validated_group_data = {'doc_origen': doc_origen, 'header': {}, 'lines': []}
                first_line_info = lines_with_nums[0]; first_line = first_line_info['data']; first_line_num = first_line_info['line_num']
                group_errors = []
                try:
                    op_type_name = first_line.get('custom_operation_type'); almacen_origen_csv = first_line.get('almacen_origen'); ubicacion_origen_csv = first_line.get('ubicacion_origen')
                    almacen_destino_csv = first_line.get('almacen_destino'); ubicacion_destino_csv = first_line.get('ubicacion_destino'); date_str = first_line.get('date_transfer')
                    partner_ref = first_line.get('partner_ref', ''); purchase_order = first_line.get('purchase_order', '')
                    if not all([op_type_name, ubicacion_origen_csv, ubicacion_destino_csv, date_str]): raise ValueError("Faltan datos cabecera.")
                    op_rule = db.get_operation_type_details_by_name(op_type_name);
                    if not op_rule: raise ValueError(f"Tipo op '{op_type_name}' no encontrado.")
                    op_code = op_rule['code']; expected_source_type = op_rule['source_location_category']; expected_dest_type = op_rule['destination_location_category']
                    src_loc_id, dest_loc_id, partner_id, wh_id_for_pt = None, None, None, None
                    if expected_source_type == 'vendor':
                        partner_info = db.get_partner_id_by_name(ubicacion_origen_csv, company_id)
                        if partner_info is None or partner_info['category_name'] != 'Proveedor Externo': group_errors.append(f"Proveedor Origen '{ubicacion_origen_csv}' inválido.")
                        else: partner_id = partner_info['id']
                    elif expected_source_type == 'customer':
                            partner_info = db.get_partner_id_by_name(ubicacion_origen_csv, company_id)
                            if partner_info is None or partner_info['category_name'] != 'Proveedor Cliente': group_errors.append(f"Cliente Origen '{ubicacion_origen_csv}' inválido.")
                            else: partner_id = partner_info['id']
                    elif expected_source_type == 'internal':
                        if not almacen_origen_csv: group_errors.append("Falta Almacén Origen.")
                        else:
                            source_loc_details = db.get_location_details_by_names(company_id, almacen_origen_csv, ubicacion_origen_csv)
                            if source_loc_details is None: group_errors.append(f"Ubicación Origen '{almacen_origen_csv}/{ubicacion_origen_csv}' inválida.")
                            else: src_loc_id = source_loc_details['id']; wh_id_for_pt = source_loc_details['warehouse_id']
                    
                    # --- ¡INICIO DE CORRECCIÓN! Lógica de Destino Faltante ---
                    if expected_dest_type == 'vendor':
                        partner_info = db.get_partner_id_by_name(ubicacion_destino_csv, company_id)
                        if partner_info is None or partner_info['category_name'] != 'Proveedor Externo': group_errors.append(f"Proveedor Destino '{ubicacion_destino_csv}' inválido.")
                        else: partner_id = partner_info['id']
                    elif expected_dest_type == 'customer':
                        partner_info = db.get_partner_id_by_name(ubicacion_destino_csv, company_id)
                        if partner_info is None or partner_info['category_name'] != 'Proveedor Cliente': group_errors.append(f"Cliente Destino '{ubicacion_destino_csv}' inválido.")
                        else: partner_id = partner_info['id']
                    elif expected_dest_type == 'internal':
                        if not almacen_destino_csv: group_errors.append("Falta Almacén Destino.")
                        else:
                            dest_loc_details = db.get_location_details_by_names(company_id, almacen_destino_csv, ubicacion_destino_csv)
                            if dest_loc_details is None: group_errors.append(f"Ubicación Destino '{almacen_destino_csv}/{ubicacion_destino_csv}' inválida.")
                            else:
                                dest_loc_id = dest_loc_details['id']
                                if wh_id_for_pt is None: wh_id_for_pt = dest_loc_details['warehouse_id']
                    # --- ¡FIN DE CORRECCIÓN! ---


                    if not group_errors:
                        picking_type_id = db.find_picking_type_id(company_id, op_code, wh_id_for_pt)
                        if not picking_type_id: group_errors.append(f"No se encontró tipo albarán '{op_code}' para almacén ID '{wh_id_for_pt}'.")
                        try: date_transfer_db = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                        except ValueError: group_errors.append(f"Formato fecha '{date_str}' incorrecto (DD/MM/YYYY).")
                    if group_errors: raise ValueError("; ".join(group_errors))

                    validated_group_data['header'] = {'picking_type_id': picking_type_id, 'src_loc_id': src_loc_id, 'dest_loc_id': dest_loc_id, 'op_type_name': op_type_name, 'partner_id': partner_id, 'date_transfer_db': date_transfer_db, 'partner_ref': partner_ref, 'purchase_order': purchase_order}

                    product_aggregation = defaultdict(lambda: {'total_quantity': 0.0, 'serials': set(), 'prices': [], 'lines_involved': []})
                    for line_info in lines_with_nums:
                        line_row = line_info['data']; line_num = line_info['line_num']
                        sku = line_row.get('product_sku'); qty_str = line_row.get('quantity'); price_str = line_row.get('price_unit', '0'); serials_str = line_row.get('serial', '')
                        if not sku or not qty_str: raise ValueError(f"Línea {line_num}: Faltan SKU o Qty.");
                        try: qty = float(qty_str); assert qty > 0
                        except: raise ValueError(f"Línea {line_num}: Qty '{qty_str}' inválida.")
                        try: price = float(price_str.replace(',', '.')) if price_str else 0.0
                        except ValueError: raise ValueError(f"Línea {line_num}: Precio '{price_str}' inválido.")
                        if sku not in product_aggregation or 'details' not in product_aggregation[sku]:
                            product_details = db.get_product_details_by_sku(sku, company_id)
                            if not product_details: raise ValueError(f"Línea {line_num}: SKU '{sku}' no encontrado.")
                            product_aggregation[sku]['details'] = product_details
                        agg = product_aggregation[sku]; agg['total_quantity'] += qty; agg['prices'].append(price);
                        agg['serials'].update({s.strip() for s in serials_str.split(',') if s.strip()}); agg['lines_involved'].append(line_num)

                    for sku, agg_data in product_aggregation.items():
                        product_details = agg_data['details']; product_tracking = product_details['tracking']; total_qty = agg_data['total_quantity']; collected_serials = list(agg_data['serials']); num_serials = len(collected_serials); line_numbers_str = ", ".join(map(str, sorted(agg_data['lines_involved'])))
                        avg_price = sum(agg_data['prices']) / len(agg_data['prices']) if agg_data['prices'] else 0.0
                        final_price = avg_price if avg_price > 0 else (product_details['standard_price'] or 0.0)
                        final_qty = total_qty
                        if product_tracking != 'none':
                            if num_serials > 0 and abs(total_qty - num_serials) > 0.001: raise ValueError(f"SKU '{sku}' (L.{line_numbers_str}): Qty({total_qty}) != #Series({num_serials}).")
                            elif num_serials == 0 and total_qty > 0: raise ValueError(f"SKU '{sku}' (L.{line_numbers_str}): Requiere series no provistas.")
                            elif num_serials > 0: final_qty = float(num_serials)
                            else: collected_serials = []
                        elif collected_serials: collected_serials = []
                        if final_qty > 0:
                            validated_group_data['lines'].append({ 'product_id': product_details['id'], 'final_qty': final_qty, 'final_price': final_price, 'serials': collected_serials })
                    if validated_group_data['lines']: validated_data.append(validated_group_data)
                    else: print(f"  [WARN] Doc '{doc_origen}': Omitido (sin líneas válidas).")
                except Exception as group_val_err:
                    error_msg = f"Doc '{doc_origen}' (Línea ~{first_line_num}): {group_val_err}"; all_errors.append(error_msg)
                    print(f"  -> ERROR Validación Doc '{doc_origen}': {group_val_err}")

        # ==========================================
        # --- FIN FASE 1: Revisar Errores ---
        # ==========================================
        if all_errors:
            raise HTTPException(status_code=400, detail="Validación fallida:\n- " + "\n- ".join(all_errors[:20]))

        # ============================================
        # --- FASE 2: EJECUCIÓN (SI NO HAY ERRORES) ---
        # ============================================
        print("--- Fase 2: Iniciando Ejecución (Creación en BD) ---")
        created_headers = 0; created_pickings = 0

        if import_type == 'headers':
            # --- ¡CORRECCIÓN! No necesitamos conn_exec ni cursor_exec ---
            try:
                for valid_row in validated_data:
                    row_num = valid_row['row_num']; picking_type_id = valid_row['picking_type_id']
                    try:
                        new_name = db.get_next_picking_name(picking_type_id)
                        # Llamar a las funciones de 'db' directamente
                        new_picking_id = db.create_picking(
                            new_name, picking_type_id, valid_row['src_loc_id'], 
                            valid_row['dest_loc_id'], company_id, responsible_user
                        )
                        db.update_picking_header(
                            new_picking_id, valid_row['src_loc_id'], valid_row['dest_loc_id'],
                            valid_row['partner_ref'], valid_row['date_transfer_db'],
                            valid_row['purchase_order'], valid_row['op_rule']['name'],
                            valid_row['partner_id']
                        )
                        created_headers += 1
                    except Exception as exec_row_err: all_errors.append(f"Fila {row_num}: Error DB ejecución: {exec_row_err}"); raise exec_row_err
            except Exception as exec_err:
                if not all_errors: all_errors.append(f"Error crítico durante ejecución: {exec_err}");

        elif import_type == 'full':
            for group_data in validated_data:
                doc_origen = group_data['doc_origen']; header_data = group_data['header']; lines_data = group_data['lines']
                try:
                    new_name = db.get_next_picking_name(header_data['picking_type_id'])

                    # --- ¡CORRECCIÓN! Llamar a las funciones de 'db' directamente ---
                    new_picking_id = db.create_picking(
                        new_name, header_data['picking_type_id'], header_data['src_loc_id'], 
                        header_data['dest_loc_id'], company_id, responsible_user
                    )
                    db.update_picking_header(
                        new_picking_id, header_data['src_loc_id'], header_data['dest_loc_id'],
                        header_data['partner_ref'], header_data['date_transfer_db'],
                        header_data['purchase_order'], header_data['op_type_name'],
                        header_data['partner_id']
                    )

                    for line in lines_data:
                        move_id = db.add_stock_move_to_picking(
                            new_picking_id, line['product_id'], line['final_qty'],
                            header_data['src_loc_id'], header_data['dest_loc_id'],
                            line['final_price'], header_data['partner_id']
                        )
                        if line['serials']:
                            tracking_data = {s: 1 for s in line['serials']}
                            db.save_move_lines_for_move(move_id, tracking_data)

                    created_pickings += 1
                except Exception as exec_group_err:
                    all_errors.append(f"Doc '{doc_origen}': Error DB ejecución: {exec_group_err}"); break

        if all_errors:
            raise HTTPException(status_code=500, detail="Falló la Fase 2 (Ejecución):\n- " + "\n- ".join(all_errors[:20]))

        return {"created": created_headers or created_pickings, "updated": 0, "errors": 0}

    except ValueError as ve: # Captura errores de Fase 1
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico al procesar CSV: {e}")


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
# --- ¡CORREGIDO! ---
        await asyncio.to_thread(
            db.update_picking_header,
            pid=picking_id,
            src_id=data.location_src_id,
            dest_id=data.location_dest_id,
            ref=data.partner_ref,
            date_transfer=data.date_transfer,
            purchase_order=data.purchase_order,
            custom_op_type=data.custom_operation_type,
            partner_id=data.partner_id
        )
        # --- FIN CORRECCIÓN! ---
        
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
    data = await asyncio.to_thread(db.get_picking_type_details, pt_id) # <-- ¡CORREGIDO!
    if not data:
        raise HTTPException(status_code=404, detail="Tipo de Picking no encontrado")
    return dict(data)

@router.get("/helpers/location-details", response_model=dict)
async def get_location_details(auth: AuthDependency, loc_id: int):
    data = db.get_location_name_details(loc_id)
    if not data:
        raise HTTPException(status_code=404, detail="Ubicación no encontrada")
    return dict(data)

@router.get("/helpers/picking-types-summary", response_model=List[dict])
async def get_picking_types_summary(auth: AuthDependency, company_id: int = 1):
    """
    Obtiene una lista simple de todos los tipos de albarán (para las pestañas).
    """
    data = db.get_picking_types(company_id)
    return [dict(row) for row in data]

@router.get("/helpers/op-type-change-data", response_model=dict)
async def get_op_type_change_data(
    auth: AuthDependency,
    op_type_name: str,
    company_id: int = 1
):
    """
    [COMBO-V2-OPTIMIZADO] Obtiene solo los datos de REGLA y DROPDOWNS
    cuando el usuario cambia el 'Tipo de Operación'.
    La lista de productos ya no se envía.
    """
    print(f"\n[API-COMBO-V2] Obteniendo datos (solo dropdowns) para Tipo Op: {op_type_name}")
    
    try:
        # --- 1. Obtener la Regla de Operación ---
        op_rule = await asyncio.to_thread(db.get_operation_type_details_by_name, op_type_name) # <-- ¡CORREGIDO!
        if not op_rule:
            raise HTTPException(status_code=404, detail="Regla de operación no encontrada.")
            
        source_type = op_rule['source_location_category']
        dest_type = op_rule['destination_location_category']
        op_name = op_rule['name']

        # --- 2. Definir Filtros de Almacén (Lógica de negocio) ---
        allowed_origin_wh_categories = []
        if source_type == 'internal':
            if op_name == "Transferencia entre Almacenes": allowed_origin_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Consignación Entregada": allowed_origin_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Devolución de Contrata": allowed_origin_wh_categories = ["CONTRATISTA"]
            elif op_name == "Transferencia entre Contratas": allowed_origin_wh_categories = ["CONTRATISTA"]
            elif op_name == "Devolución a Proveedor": allowed_origin_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Devolución a Cliente": allowed_origin_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Traspaso Contrata Cliente": allowed_origin_wh_categories = ["ALMACEN PRINCIPAL", "CONTRATISTA"]
            elif op_name == "Liquidación por OT": allowed_origin_wh_categories = ["CONTRATISTA"]
            else: allowed_origin_wh_categories = ["ALMACEN PRINCIPAL", "CONTRATISTA"]
        
        allowed_dest_wh_categories = []
        if dest_type == 'internal':
            if op_name == "Compra Nacional": allowed_dest_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Consignación Recibida": allowed_dest_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Transferencia entre Almacenes": allowed_dest_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Consignación Entregada": allowed_dest_wh_categories = ["CONTRATISTA"]
            elif op_name == "Devolución de Contrata": allowed_dest_wh_categories = ["ALMACEN PRINCIPAL"]
            elif op_name == "Transferencia entre Contratas": allowed_dest_wh_categories = ["CONTRATISTA"]
            else: allowed_dest_wh_categories = ["ALMACEN PRINCIPAL", "CONTRATISTA"]

        # --- 3. Ejecutar consultas de BD en paralelo (SIN PRODUCTOS) ---
        tasks = {
            "warehouses_origin": asyncio.to_thread(db.get_warehouses_by_categories, company_id, allowed_origin_wh_categories),
            "warehouses_dest": asyncio.to_thread(db.get_warehouses_by_categories, company_id, allowed_dest_wh_categories),
            "partners_vendor": asyncio.to_thread(db.get_partners, company_id, category_name="Proveedor Externo"),
            "partners_customer": asyncio.to_thread(db.get_partners, company_id, category_name="Proveedor Cliente"),
        }
        
        results = await asyncio.gather(*tasks.values())
        
        # --- 4. Construir la Respuesta JSON (SIN PRODUCTOS) ---
        response = {
            "op_rule": dict(op_rule),
            "dropdown_options": {
                "warehouses_origin": [dict(r) for r in results[0]],
                "warehouses_dest": [dict(r) for r in results[1]],
                "partners_vendor": [dict(r) for r in results[2]],
                "partners_customer": [dict(r) for r in results[3]]
            },
            "products": [] # <-- DEVOLVER LISTA VACÍA (ya no se usa)
        }
        
        print(f"[API-COMBO-V2] Datos (solo dropdowns) generados.")
        return response

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error en op-type-change-data: {e}")
    

    