# app/api/reports.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional, Dict
from app import database as db
from app import schemas, security
from app.security import TokenData
from datetime import date, datetime # Asegúrate de que datetime esté importado
import traceback # Importa traceback
import csv
import io
import asyncio
from fastapi.responses import StreamingResponse
from decimal import Decimal, ROUND_HALF_UP, getcontext
getcontext().prec = 28

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/dashboard-kpis", response_model=schemas.DashboardResponse)
async def get_dashboard_kpis(
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """ 
    Obtiene TODOS los KPIs. 
    [ESTABILIZADO] Ejecuta consultas en BLOQUES secuenciales para no saturar 
    el pool de conexiones de la base de datos (evita error SSL connection closed).
    """
    if "nav.dashboard.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        # --- BLOQUE 1: KPIs Numéricos Ligeros (Rápidos) ---
        # Ejecutamos estos 3 en paralelo porque consumen poco recurso
        batch_1 = await asyncio.gather(
            asyncio.to_thread(db.get_dashboard_kpis, company_id),           # Conteos
            asyncio.to_thread(db.execute_query, "SELECT COUNT(*) as c FROM work_orders WHERE phase != 'Liquidado' AND company_id=%s", (company_id,), fetchone=True), # OTs
            asyncio.to_thread(db.get_total_liquidated_value_global, company_id) # Liquidado Global
        )
        kpis_counts = batch_1[0]
        ots_pendientes = batch_1[1]['c'] if batch_1[1] else 0
        total_liquidated = batch_1[2]

        # --- BLOQUE 2: Análisis Financiero y Gráficos (Pesados) ---
        # Ejecutamos secuencialmente o en un grupo pequeño
        inv_values = await asyncio.to_thread(db.get_inventory_value_kpis, company_id)
        ownership_stats = await asyncio.to_thread(db.get_ownership_distribution, company_id)
        
        batch_2_charts = await asyncio.gather(
            asyncio.to_thread(db.get_operations_throughput, company_id),
            asyncio.to_thread(db.get_inventory_aging, company_id),
            asyncio.to_thread(db.get_material_flow_series, company_id, 30) # Default 30 days
        )
        throughput = batch_2_charts[0]
        aging = batch_2_charts[1]
        flow_data = batch_2_charts[2]

        # --- BLOQUE 3: Rankings y Listas (Complejos) ---
        # Estos suelen tener JOINs y GROUP BYs grandes
        batch_3 = await asyncio.gather(
            asyncio.to_thread(db.get_top_projects_statistics, company_id),
            asyncio.to_thread(db.get_value_by_region, company_id),
            asyncio.to_thread(db.get_top_products_by_value, company_id),
            asyncio.to_thread(db.get_value_by_category, company_id),
            asyncio.to_thread(db.get_warehouse_ranking_by_category, company_id, "ALMACEN PRINCIPAL", 5),
            asyncio.to_thread(db.get_warehouse_ranking_by_category, company_id, "CONTRATISTA", 10),
            asyncio.to_thread(db.get_abc_stats, company_id),
            asyncio.to_thread(db.get_reverse_logistics_rate, company_id)
        )
        
        top_projects = batch_3[0]
        geo_data = batch_3[1]
        top_products = batch_3[2]
        val_by_cat = batch_3[3]
        top_wh = batch_3[4]
        top_cont = batch_3[5]
        abc_data = batch_3[6]
        ret_rate = batch_3[7]

        # --- Procesamiento Final (En memoria) ---
        own_val = sum(x['value'] for x in ownership_stats if x['type'] == 'Propio')
        cons_val = sum(x['value'] for x in ownership_stats if x['type'] == 'Consignado')

        response = schemas.DashboardResponse(
            # Financiero
            total_inventory_value=inv_values['total'],
            own_inventory_value=own_val,
            consigned_inventory_value=cons_val,
            total_liquidated_value=total_liquidated,
            value_kpis=inv_values, 

            # Operativo
            pending_receptions=kpis_counts.get('IN', 0),
            pending_transfers=kpis_counts.get('INT', 0),
            pending_liquidations=ots_pendientes,

            # Gráficos
            throughput_chart=[{"day": day.strftime("%a"), "count": count} for day, count in throughput],
            aging_chart=aging,
            material_flow=flow_data,
            
            # Listas
            top_projects=top_projects,
            ownership_chart=ownership_stats,
            top_products=top_products,
            value_by_category=val_by_cat,
            geo_heatmap=geo_data,
            top_warehouses=top_wh,
            top_contractors=top_cont,
            abc_stats=abc_data,
            return_rate=ret_rate
        )
        return response
        
    except Exception as e:
        print(f"ERROR DASHBOARD: {e}") # Log para Render
        traceback.print_exc()
        # Devolvemos un error 500 limpio pero informativo
        raise HTTPException(status_code=500, detail=f"Error de DB: {str(e)}")

@router.get("/stock-summary", response_model=List[schemas.StockReportResponse])
async def get_stock_summary_report(
    auth: AuthDependency,
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None,
    location_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None
):
    """ Obtiene el reporte de stock resumido (agrupado por producto/ubicación). """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    filters = { "warehouse_id": warehouse_id,"location_id": location_id, "sku": sku, "product_name": product_name, "category_name": category_name }
    filters = {k: v for k, v in filters.items() if v is not None}

    try:
        stock_data = db.get_stock_summary_filtered_sorted(company_id=company_id, filters=filters)
        return [dict(row) for row in stock_data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al generar reporte de stock: {e}")


@router.get("/aging", response_model=List[schemas.AgingDetailResponse])
async def get_aging_report(
    auth: AuthDependency,
    company_id: int = Query(...),
    product_filter: Optional[str] = Query(None, alias="product"),
    warehouse_id: Optional[int] = Query(None),
    bucket: Optional[str] = Query(None)
):
    """ Obtiene el reporte detallado de antigüedad de inventario. """
    if "reports.aging.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    filters = { "product": product_filter, "warehouse_id": warehouse_id, "bucket": bucket }
    filters = {k: v for k, v in filters.items() if v is not None}

    try:
        aging_data = db.get_inventory_aging_details(company_id, filters)
        return [dict(row) for row in aging_data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al generar reporte de antigüedad: {e}")

@router.get("/coverage", response_model=List[schemas.CoverageReportResponse])
async def get_coverage_report(
    auth: AuthDependency,
    company_id: int = Query(...),
    history_days: int = 90,
    product_filter: Optional[str] = Query(None)
):
    """ Obtiene el reporte de cobertura de stock. """
    if "reports.coverage.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        coverage_data = db.get_stock_coverage_report(company_id, history_days, product_filter)
        return [dict(row) for row in coverage_data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al generar reporte de cobertura: {e}")

@router.get("/reservations/{product_id}/{location_id}", response_model=List[dict])
async def get_product_reservations(
    product_id: int,
    location_id: int,
    auth: AuthDependency,
    lot_id: Optional[int] = None
):
    """ Obtiene el detalle de albaranes 'listo' que reservan stock. """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        reservations = db.get_product_reservations(product_id, location_id, lot_id)
        return [dict(row) for row in reservations]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener reservas: {e}")

# --- ¡NUEVOS ENDPOINTS PARA KARDEX! ---

@router.get("/kardex-summary", response_model=List[schemas.KardexSummaryResponse])
async def get_kardex_summary(
    auth: AuthDependency,
    date_from: date, # FastAPI convierte "YYYY-MM-DD" en un objeto date
    date_to: date,
    company_id: int = Query(...),
    product_filter: Optional[str] = Query(None),
    warehouse_id: Optional[str] = Query(None) # Puede ser 'all' o un ID
):
    """ Obtiene el reporte resumen de Kardex Valorizado. """
    if "reports.kardex.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")
    
    try:
        # Convertir fechas a strings YYYY-MM-DD que espera la BD
        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")
        
        kardex_data = db.get_kardex_summary(
            company_id, date_from_str, date_to_str, product_filter, warehouse_id
        )
        return [dict(row) for row in kardex_data]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar kardex: {e}")

@router.get("/kardex-detail", response_model=List[schemas.KardexDetailResponse])
async def get_kardex_detail(
    auth: AuthDependency,
    product_id: int,
    date_from: date,
    date_to: date,
    company_id: int = Query(...),
    warehouse_id: Optional[str] = Query(None) # Puede ser 'all' o un ID
):
    """ Obtiene el detalle de movimientos de Kardex para un producto. """
    if "reports.kardex.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")
    
    try:
        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")
        
        detail_data = db.get_product_kardex(
            company_id, product_id, date_from_str, date_to_str, warehouse_id
        )
        return [dict(row) for row in detail_data]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar detalle kardex: {e}")

def _process_kardex_export_data_sync(company_id, date_from_db, date_to_db, warehouse_id, product_filter, date_from_str_display):
    """
    Lógica de cálculo de exportación de Kardex.
    Esta es la función pesada, ahora se ejecuta en el servidor.
    """
    D = Decimal; TWO_PLACES = D('0.01'); FOUR_PLACES = D('0.0001')
    
    # 1. Obtener saldo inicial de TODOS los productos
    summary_initial = db.get_kardex_summary(company_id, '1900-01-01', date_from_db, product_filter, warehouse_id)
    product_states = {}
    for item_row in summary_initial:
        item = dict(item_row)
        product_states[item['product_id']] = {
            'qty': D(str(item.get('final_balance', 0.0) or 0.0)), 
            'val': D(str(item.get('final_value', 0.0) or 0.0)),
            'sku': item['sku'], 'name': item['product_name'],
            'category_name': item.get('category_name')
        }
    
    # 2. Obtener TODOS los movimientos en el rango
    raw_moves = db.get_full_product_kardex_data(company_id, date_from_db, date_to_db, warehouse_id, product_filter)
    
    final_data = []; group_id_counter = 0; current_product_id = None; state = {}

    # Si no hay movimientos, exportar solo los saldos iniciales
    if not raw_moves:
        for group_id, (pid, state_data) in enumerate(product_states.items(), 1):
            if state_data['qty'] != D('0') or state_data['val'] != D('0'):
                final_data.append({
                    'GroupID': group_id, 'SKU': state_data['sku'], 'Producto': state_data['name'], 
                    'Categoría': state_data.get('category_name') or '',
                    'Fecha': date_from_str_display, 'Referencia': 'SALDO INICIAL',
                    'Saldo Cant': state_data['qty'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
                    'Saldo Valorizado': state_data['val'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                })
        return final_data

    # 3. Procesar movimientos y calcular saldos
    for move_row in raw_moves:
        move = dict(move_row); p_id = move['product_id']
        
        if p_id != current_product_id:
            current_product_id = p_id; group_id_counter += 1
            state = product_states.get(p_id, {'qty': D('0'), 'val': D('0'), 'sku': move['product_sku'], 'name': move['product_name'], 'category_name': move.get('category_name')})
            
            if state['qty'] != D('0') or state['val'] != D('0'):
                final_data.append({
                    'GroupID': group_id_counter, 'SKU': state['sku'], 'Producto': state['name'], 
                    'Categoría': state.get('category_name') or '',
                    'Fecha': date_from_str_display, 'Referencia': 'SALDO INICIAL',
                    'Saldo Cant': state['qty'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
                    'Saldo Valorizado': state['val'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                })
        
        current_qty = state['qty'] ; current_val = state['val']
        quantity_in = D(str(move.get('quantity_in', 0.0) or 0.0)); quantity_out = D(str(move.get('quantity_out', 0.0) or 0.0))
        cost_at_adjustment_raw = move.get('cost_at_adjustment')
        cost_at_adjustment = D(str(cost_at_adjustment_raw)) if cost_at_adjustment_raw is not None else None
        
        valor_entrada_calc = D('0'); valor_salida_calc = D('0'); precio_unit_salida = D('0'); precio_unit_entrada = D('0')
        current_avg_cost = (current_val / current_qty) if current_qty > D('0') else D('0')
        
        if quantity_out > D('0'):
            if cost_at_adjustment is not None and cost_at_adjustment > D('0'):
                precio_unit_salida = cost_at_adjustment.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                valor_salida_calc = (quantity_out * precio_unit_salida).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            else:
                precio_unit_salida = current_avg_cost.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                valor_salida_calc = (quantity_out * precio_unit_salida).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            current_qty -= quantity_out; current_val -= valor_salida_calc
        
        elif quantity_in > D('0'):
            price_unit_in_raw = move.get('price_unit')
            if price_unit_in_raw is not None and D(str(price_unit_in_raw)) > D('0'):
                precio_unit_entrada = D(str(price_unit_in_raw)).quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                valor_entrada_calc = (quantity_in * precio_unit_entrada).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            else:
                precio_unit_entrada = current_avg_cost.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                valor_entrada_calc = (quantity_in * precio_unit_entrada).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
            current_qty += quantity_in; current_val += valor_entrada_calc
        
        if current_qty.compare(D('0.005')) < 0:
            current_qty = D('0'); current_val = D('0')
            
        state['qty'] = current_qty ; state['val'] = current_val
        
        final_data.append({
            'GroupID': group_id_counter, 'SKU': move['product_sku'], 'Producto': move['product_name'], 'Categoría': move.get('category_name') or '',
            'Fecha': move['date'].strftime("%d/%m/%Y %H:%M") if move.get('date') else '', # <-- CORREGIDO
            'Fecha Traslado': move['date_transfer'].strftime("%d/%m/%Y") if move.get('date_transfer') else '', # <-- CORREGIDO
            'Referencia': move['operation_ref'], 'Tipo Operacion': move['custom_operation_type'], 
            'Almacen Origen': move.get('almacen_origen') or (move.get('partner_name') if move.get('type_code') == 'IN' else "-"),
            'Ubicacion Origen': move.get('ubicacion_origen') or "-",
            'Almacen Destino': move.get('almacen_destino') or (move.get('partner_name') if move.get('type_code') == 'OUT' else "-"),
            'Ubicacion Destino': move.get('ubicacion_destino') or "-",
            'Razón Ajuste': move.get('adjustment_reason') or '', 'Almacen Afectado': move.get('affected_warehouse') or '',
            'Guia Remision / Acta': move.get('partner_ref') or '', 'Proveedor / Cliente / OT': move.get('partner_name') or '',
            'Orden de Compra': move.get('purchase_order') or '',
            'Entrada Cant': quantity_in.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if quantity_in > D('0') else '',
            'Precio Unit. Entrada': precio_unit_entrada.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP) if quantity_in > D('0') else '',
            'Valor Entrada': valor_entrada_calc.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if valor_entrada_calc > D('0') else '',
            'Salida Cant': quantity_out.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if quantity_out > D('0') else '',
            'Precio Unit. Salida': precio_unit_salida.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP) if quantity_out > D('0') else '',
            'Valor Salida': valor_salida_calc.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if valor_salida_calc > D('0') else '',
            'Saldo Cant': current_qty.quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
            'Saldo Valorizado': current_val.quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
        })
    return final_data

@router.get("/kardex-export/csv", response_class=StreamingResponse)
async def export_kardex_detail_csv(
    auth: AuthDependency,
    date_from: date, # Recibe 'YYYY-MM-DD'
    date_to: date,
    company_id: int = Query(...),
    product_filter: Optional[str] = Query(None),
    warehouse_id: Optional[str] = Query(None),
    date_from_display: str = Query(...) # Recibe 'DD/MM/YYYY' para el 'SALDO INICIAL'
):
    """
    Genera y transmite el reporte de Kardex detallado completo como CSV.
    Toda la lógica pesada de 'Decimal' se ejecuta aquí, en el servidor.
    """
    if "reports.kardex.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    try:
        # 1. Ejecutar la función de procesamiento de datos
        processed_data = _process_kardex_export_data_sync(
            company_id, 
            date_from.strftime("%Y-%m-%d"), 
            date_to.strftime("%Y-%m-%d"), 
            warehouse_id, 
            product_filter,
            date_from_display
        )
        
        if not processed_data:
            raise HTTPException(status_code=404, detail="No se encontraron movimientos para exportar.")

        # 2. Definir cabeceras
        headers = [
            'GroupID', 'SKU', 'Producto', 'Categoría', 'Fecha', 'Fecha Traslado', 'Referencia',
            'Tipo Operacion', 'Almacen Origen', 'Ubicacion Origen', 'Almacen Destino', 'Ubicacion Destino',
            'Razón Ajuste', 'Almacen Afectado', 'Guia Remision / Acta', 
            'Proveedor / Cliente / OT', 'Orden de Compra',
            'Entrada Cant', 'Precio Unit. Entrada', 'Valor Entrada',
            'Salida Cant', 'Precio Unit. Salida', 'Valor Salida',
            'Saldo Cant', 'Saldo Valorizado'
        ]

        # 3. Generar CSV en memoria
        output = io.StringIO(newline='')
        writer = csv.DictWriter(output, fieldnames=headers, delimiter=';', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(processed_data)
        
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=kardex_detalle_completo.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar exportación de kardex: {e}")

@router.get("/stock-detail", response_model=List[schemas.StockDetailResponse])
async def get_stock_detail_report(
    auth: AuthDependency,
    company_id: int = Query(...),
    # Filtros
    warehouse_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    location_id: Optional[int] = None # Filtro extra de ubicación
):
    """ 
    Obtiene el reporte de stock detallado (por serie/lote).
    Corresponde a la Pestaña 2 (db.get_stock_on_hand_filtered_sorted).
    """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    # Adaptamos los filtros del frontend a los de la función de BD
    filters = { 
        "warehouse_id": warehouse_id, 
        "location_id": location_id,
        "sku": sku, 
        "product_name": product_name, 
        "category_name": category_name 
    }
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}

    try:
        stock_data = await asyncio.to_thread(
            db.get_stock_on_hand_filtered_sorted, 
            company_id=company_id, 
            filters=filters
        )
        
        # --- 🕵️‍♂️ INICIO DEL ESPÍA ---
        if stock_data:
            print(f"\n[DEBUG API] Primera fila cruda de DB: {dict(stock_data[0])}")
            # Verificamos si las claves existen
            row = dict(stock_data[0])
            print(f"   -> Tiene 'lot_name'? {row.get('lot_name')}")
            print(f"   -> Tiene 'project_name'? {row.get('project_name')}")
        else:
            print("\n[DEBUG API] La DB no devolvió resultados.")
        # --- FIN DEL ESPÍA ---

        return [dict(row) for row in stock_data]

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar reporte detallado: {e}")


# --- ENDPOINTS FALTANTES PARA EXPORTAR CSV ---

def _generate_csv_response(data: List[dict], headers_map: dict, filename: str) -> StreamingResponse:
    """Helper genérico para crear un CSV en memoria y devolverlo como StreamingResponse."""
    if not data:
        raise HTTPException(status_code=404, detail="No hay datos para exportar.")
        
    output = io.StringIO(newline='')
    writer = csv.writer(output, delimiter=';')
    
    # Escribir cabeceras (usando las keys del map como el orden)
    writer.writerow(headers_map.values())

    # Escribir datos
    for row_dict in data:
        # Construir la fila en el orden de las cabeceras
        csv_row = [row_dict.get(key, '') for key in headers_map.keys()]
        writer.writerow(csv_row)
            
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@router.get("/stock-summary/export/csv", response_class=StreamingResponse)
async def export_stock_summary_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    # Reutilizamos los mismos filtros que el reporte
    warehouse_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None
):
    """ Exporta el reporte de stock resumido a CSV. """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = { "warehouse_id": warehouse_id, "sku": sku, "product_name": product_name, "category_name": category_name }
    filters = {k: v for k, v in filters.items() if v is not None}
    
    try:
        # 1. Obtener los datos (igual que el endpoint /stock-summary)
        stock_data_raw = db.get_stock_summary_filtered_sorted(company_id=company_id, filters=filters)
        stock_data = [dict(row) for row in stock_data_raw]

        # 2. Definir cabeceras (key_db: "Header CSV")
        headers_map = {
            'warehouse_name': "Almacen", 'location_name': "Ubicacion",
            'sku': "SKU", 'product_name': "Producto", 'category_name': "Categoria",
            'physical_quantity': "Fisico", 'reserved_quantity': "Reservado",
            'available_quantity': "Disponible", 'uom_name': "UdM"
        }
        
        # 3. Generar y devolver CSV
        return _generate_csv_response(stock_data, headers_map, "stock_resumen.csv")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al exportar resumen: {e}")

@router.get("/stock-detail/export/csv", response_class=StreamingResponse)
async def export_stock_detail_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    # Reutilizamos los mismos filtros
    warehouse_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    location_id: Optional[int] = None
):
    """ Exporta el reporte de stock detallado (Series/Lotes) a CSV. """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = { "sku": sku, "product_name": product_name, "category_name": category_name, "location_id": location_id }
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    try:
        # 1. Obtener los datos (igual que el endpoint /stock-detail)
        stock_data_raw = db.get_stock_on_hand_filtered_sorted(
            company_id=company_id, warehouse_id=warehouse_id, filters=filters
        )
        stock_data = [dict(row) for row in stock_data_raw]

        # 2. Definir cabeceras
        headers_map = {
            'warehouse_name': "Almacen", 'location_name': "Ubicacion",
            'sku': "SKU", 'product_name': "Producto", 'category_name': "Categoria",
            'lot_name': "Serie_Lote",
            'physical_quantity': "Fisico", 'reserved_quantity': "Reservado",
            'available_quantity': "Disponible", 'uom_name': "UdM"
        }
        
        # 3. Generar y devolver CSV
        return _generate_csv_response(stock_data, headers_map, "stock_detalle_series.csv")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al exportar detalle: {e}")

@router.post("/stock-for-products", response_model=Dict[int, float])
async def get_stock_for_products_list(
    auth: AuthDependency,
    request_data: schemas.StockCheckRequest
):
    """
    [NUEVO] Obtiene el stock físico (quants) para una lista de IDs de
    productos en una ubicación específica.
    """
    try:
        # Usamos to_thread porque la función de BD es síncrona
        stock_map = await asyncio.to_thread(
            db.get_stock_for_multiple_products,
            request_data.location_id,
            request_data.product_ids
        )
        return stock_map
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al consultar stock múltiple: {e}")
    
@router.get("/project-kardex/{project_id}")
def get_project_kardex_report(project_id: int, auth: AuthDependency, company_id: int = Query(...)):
    """
    Devuelve el resumen de stock y consumo de un proyecto específico.
    """
    try:
        data = db.get_project_kardex(company_id, project_id)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generando reporte: {str(e)}")

@router.get("/warehouses-kpi", response_model=List[dict])
async def get_warehouses_kpi(
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """ Obtiene resumen de almacenes para el Hub de Inventario. """
    try:
        data = await asyncio.to_thread(db.get_warehouses_kpi_summary, company_id)
        return [dict(row) for row in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo KPIs de almacenes: {e}")
    
@router.get("/chart/flow", response_model=List[schemas.FlowDataPoint])
async def get_flow_chart_data(
    auth: AuthDependency,
    company_id: int = Query(...),
    days: int = Query(30) # Parámetro nuevo
):
    """Endpoint específico para refrescar el gráfico de líneas."""
    if "nav.dashboard.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")
    
    try:
        data = await asyncio.to_thread(db.get_material_flow_series, company_id, days)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
