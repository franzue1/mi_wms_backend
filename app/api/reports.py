# app/api/reports.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional, Dict
from app import database as db
from app import schemas, security
from app.security import TokenData
from datetime import date, datetime
import traceback
import csv
import io
import asyncio
from fastapi.responses import StreamingResponse
from decimal import Decimal, ROUND_HALF_UP, getcontext
from app.database.repositories import operation_repo
from app.services.report_service import ReportService
from app.exceptions import NotFoundError, ValidationError
getcontext().prec = 28

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/dashboard-kpis", response_model=schemas.DashboardResponse)
async def get_dashboard_kpis(
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """ 
    [MODO SEGURO] Ejecución ESTRICTAMENTE SECUENCIAL.
    Se eliminó asyncio.gather para evitar abrir múltiples conexiones SSL
    simultáneas que saturan el Pooler de Supabase/Render.
    """
    if "nav.dashboard.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        # Ejecutamos UNA consulta a la vez (await explicito paso a paso)
        
        # 1. Conteos Básicos
        kpis_counts = await asyncio.to_thread(db.get_dashboard_kpis, company_id)
        
        # 2. OTs Pendientes
        ots_res = await asyncio.to_thread(db.execute_query, "SELECT COUNT(*) as c FROM work_orders WHERE phase != 'Liquidado' AND company_id=%s", (company_id,), fetchone=True)
        ots_pendientes = ots_res['c'] if ots_res else 0

        # 3. Valor Liquidado Global
        total_liquidated = await asyncio.to_thread(db.get_total_liquidated_value_global, company_id)

        # 4. Valor Inventario (Financiero)
        inv_values = await asyncio.to_thread(db.get_inventory_value_kpis, company_id)

        # 5. Distribución Propiedad
        ownership_stats = await asyncio.to_thread(db.get_ownership_distribution, company_id)

        # 6. Gráfico Rendimiento
        throughput = await asyncio.to_thread(db.get_operations_throughput, company_id)

        # 7. Aging
        aging = await asyncio.to_thread(db.get_inventory_aging, company_id)

        # 8. Flujo Materiales
        flow_data = await asyncio.to_thread(db.get_material_flow_series, company_id, 30)

        # 9. Top Proyectos
        top_projects = await asyncio.to_thread(db.get_top_projects_statistics, company_id)

        # 10. Geo Data
        geo_data = await asyncio.to_thread(db.get_value_by_region, company_id)

        # 11. Top Productos
        top_products = await asyncio.to_thread(db.get_top_products_by_value, company_id)

        # 12. Por Categoría
        val_by_cat = await asyncio.to_thread(db.get_value_by_category, company_id)

        # 13. Top Almacenes
        top_wh = await asyncio.to_thread(db.get_warehouse_ranking_by_category, company_id, "ALMACEN PRINCIPAL", 5)

        # 14. Top Contratistas
        top_cont = await asyncio.to_thread(db.get_warehouse_ranking_by_category, company_id, "CONTRATISTA", 10)

        # 15. Estadísticas ABC
        abc_data = await asyncio.to_thread(db.get_abc_stats, company_id)

        # 16. Tasa Devolución
        ret_rate = await asyncio.to_thread(db.get_reverse_logistics_rate, company_id)

        # --- Procesamiento Final ---
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
        print(f"ERROR DASHBOARD: {e}") 
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error de DB: {str(e)}")

@router.get("/stock-summary", response_model=List[schemas.StockReportResponse])
async def get_stock_summary_report(
    auth: AuthDependency,
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None,
    location_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    # [NUEVO] Agregar location_name
    location_name: Optional[str] = None,
    
    # Paginación
    skip: int = 0,
    limit: int = 50,
    sort_by: str = 'sku',
    ascending: bool = True
):
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    filters = { 
        "warehouse_id": warehouse_id,
        "location_id": location_id,
        "sku": sku, 
        "product_name": product_name, 
        "category_name": category_name,
        # [NUEVO] Mapear el filtro
        "location_name": location_name
    }
    filters = {k: v for k, v in filters.items() if v is not None}

    try:
        stock_data = await asyncio.to_thread(
            db.get_stock_summary_filtered_sorted, 
            company_id=company_id, 
            filters=filters,
            sort_by=sort_by, 
            ascending=ascending, 
            limit=limit, 
            offset=skip
        )
        return [dict(row) for row in stock_data]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar reporte de stock: {e}")

@router.get("/stock-summary/count", response_model=int)
async def get_stock_summary_count(
    auth: AuthDependency,
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None,
    location_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    # [NUEVO]
    location_name: Optional[str] = None
):
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    filters = { 
        "warehouse_id": warehouse_id, 
        "location_id": location_id, 
        "sku": sku, 
        "product_name": product_name, 
        "category_name": category_name,
        # [NUEVO]
        "location_name": location_name
    }
    filters = {k: v for k, v in filters.items() if v is not None}

    try:
        return await asyncio.to_thread(db.get_stock_summary_count, company_id, filters)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al contar resumen: {e}")

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

@router.get("/incoming/{product_id}/{location_id}", response_model=List[dict])
async def get_product_incoming(
    product_id: int,
    location_id: int,
    auth: AuthDependency
):
    """ Obtiene el detalle de albaranes 'listo' que están trayendo stock (En Tránsito). """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        # Llamamos a la nueva función del repo
        incoming_data = db.get_product_incoming(product_id, location_id)
        return [dict(row) for row in incoming_data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener tránsito: {e}")

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

# La función _process_kardex_export_data_sync ha sido movida a ReportService.process_kardex_export_data

@router.get("/kardex-export/csv", response_class=StreamingResponse)
async def export_kardex_detail_csv(
    auth: AuthDependency,
    date_from: date,
    date_to: date,
    company_id: int = Query(...),
    product_filter: Optional[str] = Query(None),
    warehouse_id: Optional[str] = Query(None),
    date_from_display: str = Query(...)
):
    """
    Genera y transmite el reporte de Kardex detallado completo como CSV.
    Delega al ReportService para el procesamiento de datos.
    """
    if "reports.kardex.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    try:
        # 1. Procesar datos usando el servicio
        processed_data = await asyncio.to_thread(
            ReportService.process_kardex_export_data,
            company_id,
            date_from.strftime("%Y-%m-%d"),
            date_to.strftime("%Y-%m-%d"),
            warehouse_id,
            product_filter,
            date_from_display
        )

        # 2. Generar CSV usando el servicio
        csv_content = ReportService.generate_kardex_csv_content(processed_data)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=kardex_detalle_completo.csv"}
        )

    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=e.message)
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
    location_id: Optional[int] = None,
    # [NUEVO] Filtros de texto adicionales
    location_name: Optional[str] = None,
    lot_name: Optional[str] = None,
    project_name: Optional[str] = None,

    # Paginación y Orden
    skip: int = 0,
    limit: int = 50,
    sort_by: str = 'sku',
    ascending: bool = True
):
    """ 
    Obtiene el reporte de stock detallado PAGINADO.
    """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    filters = { 
        "warehouse_id": warehouse_id, 
        "location_id": location_id,
        "sku": sku, 
        "product_name": product_name, 
        "category_name": category_name,
        # [NUEVO]
        "location_name": location_name,
        "lot_name": lot_name,
        "project_name": project_name
    }
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}

    try:
        stock_data = await asyncio.to_thread(
            db.get_stock_on_hand_filtered_sorted, 
            company_id=company_id, 
            filters=filters,
            sort_by=sort_by,
            ascending=ascending,
            limit=limit,
            offset=skip
        )
        return [dict(row) for row in stock_data]

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar reporte detallado: {e}")

@router.get("/stock-detail/count", response_model=int)
async def get_stock_detail_count(
    auth: AuthDependency,
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    location_id: Optional[int] = None,
    # [NUEVO]
    location_name: Optional[str] = None,
    lot_name: Optional[str] = None,
    project_name: Optional[str] = None
):
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    filters = { 
        "warehouse_id": warehouse_id, "location_id": location_id,
        "sku": sku, "product_name": product_name, "category_name": category_name,
        # [NUEVO]
        "location_name": location_name,
        "lot_name": lot_name,
        "project_name": project_name
    }
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}

    try:
        count = await asyncio.to_thread(
            db.get_stock_on_hand_count, company_id, warehouse_id, filters
        )
        return count
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al contar stock: {e}")

# --- ENDPOINTS FALTANTES PARA EXPORTAR CSV ---

def _generate_csv_response(data: List[dict], headers_map: dict, filename: str) -> StreamingResponse:
    """Helper genérico para crear un CSV usando ReportService."""
    try:
        csv_content = ReportService.generate_stock_summary_csv_content(data, headers_map)
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=e.message)

@router.get("/stock-summary/export/csv", response_class=StreamingResponse)
async def export_stock_summary_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None,
    location_id: Optional[int] = None, # Agregado
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    # [NUEVO] Filtro de texto
    location_name: Optional[str] = None
):
    """ Exporta el reporte de stock resumido a CSV. """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = { 
        "warehouse_id": warehouse_id, "location_id": location_id,
        "sku": sku, "product_name": product_name, "category_name": category_name,
        "location_name": location_name
    }
    filters = {k: v for k, v in filters.items() if v is not None}
    
    try:
        # 1. Obtener datos (Usamos la misma función del reporte, sin paginación)
        stock_data_raw = db.get_stock_summary_filtered_sorted(company_id=company_id, filters=filters)
        stock_data = [dict(row) for row in stock_data_raw]

        # 2. Headers
        headers_map = {
            'warehouse_name': "Almacen", 'location_name': "Ubicacion",
            'sku': "SKU", 'product_name': "Producto", 'category_name': "Categoria",
            'physical_quantity': "Fisico", 'reserved_quantity': "Reservado",
            'incoming_quantity': "En Transito",
            'available_quantity': "Disponible", 'uom_name': "UdM",
            'notes': "Observaciones"
        }
        
        return _generate_csv_response(stock_data, headers_map, "stock_resumen.csv")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al exportar resumen: {e}")

@router.get("/stock-detail/export/csv", response_class=StreamingResponse)
async def export_stock_detail_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None,
    location_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None,
    # Filtros de texto
    location_name: Optional[str] = None,
    lot_name: Optional[str] = None,
    project_name: Optional[str] = None,
    # [NUEVO] Flags de columnas dinámicas
    include_series: bool = True,
    include_project: bool = True
):
    """ Exporta el reporte de stock detallado a CSV con columnas dinámicas. """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = { 
        "sku": sku, "product_name": product_name, "category_name": category_name, 
        "location_id": location_id, "warehouse_id": warehouse_id,
        "location_name": location_name, "lot_name": lot_name, "project_name": project_name
    }
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    try:
        # 1. Obtener datos
        stock_data_raw = db.get_stock_on_hand_filtered_sorted(
            company_id=company_id, filters=filters
        )
        stock_data = [dict(row) for row in stock_data_raw]

        # 2. Construir Headers Dinámicos
        # Definimos las columnas fijas
        headers_map = {
            'warehouse_name': "Almacen", 
            'location_name': "Ubicacion",
            'sku': "SKU", 
            'product_name': "Producto", 
            'category_name': "Categoria"
        }

        # Agregamos dinámicamente según lo que pidió el frontend
        if include_series:
            headers_map['lot_name'] = "Serie_Lote"
        
        if include_project:
            headers_map['project_name'] = "Obra"

        # Agregamos las columnas numéricas finales
        headers_map.update({
            'physical_quantity': "Fisico", 
            'reserved_quantity': "Reservado",
            'incoming_quantity': "En Transito", # <--- ¡AGREGADO AQUÍ!
            'available_quantity': "Disponible", 
            'uom_name': "UdM",
            'notes': "Observaciones"
        })
        
        return _generate_csv_response(stock_data, headers_map, "stock_detalle.csv")

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

@router.get("/warehouses-kpi", response_model=Dict) # Cambiado return type a Dict para devolver {data, total}
async def get_warehouses_kpi(
    auth: AuthDependency,
    company_id: int = Query(...),
    search: Optional[str] = None,
    limit: int = 12,
    skip: int = 0
):
    """ [PAGINADO] Obtiene resumen de almacenes para el Hub. """
    try:
        user_id = auth.user_id
        # Obtener rol si no está en token
        role_name = auth.role_name
        if not hasattr(auth, 'role_name') or not role_name:
             user_data = await asyncio.to_thread(db.execute_query, 
                "SELECT r.name FROM users u JOIN roles r ON u.role_id = r.id WHERE u.id=%s", 
                (user_id,), fetchone=True)
             role_name = user_data['name'] if user_data else 'Usuario'

        # Ejecutar en paralelo count y data
        results = await asyncio.gather(
            asyncio.to_thread(db.get_warehouses_kpi_paginated, company_id, user_id, role_name, search, limit, skip),
            asyncio.to_thread(db.get_warehouses_kpi_count, company_id, user_id, role_name, search)
        )
        
        data = results[0]
        total = results[1]

        return {
            "items": [dict(row) for row in data],
            "total": total
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error obteniendo KPIs: {e}")

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

@router.get("/filter-options", response_model=List[str])
async def get_report_filter_options(
    auth: AuthDependency,
    field: str = Query(..., regex="^(location_name|category_name)$"),
    company_id: int = Query(...),
    warehouse_id: Optional[int] = None
):
    """ Devuelve lista de valores únicos para llenar dropdowns de filtro. """
    try:
        return await asyncio.to_thread(db.get_distinct_filter_values, company_id, field, warehouse_id)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error obteniendo opciones: {e}")

@router.put("/stock-note")
async def update_stock_note(
    auth: AuthDependency,
    note_data: schemas.StockNoteUpdate
):
    """ Actualiza la nota de un stock quant específico. """
    if "reports.stock.edit_notes" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No tienes permiso para editar notas de stock.")

    try:
        await asyncio.to_thread(
            operation_repo.update_stock_quant_notes,
            product_id=note_data.product_id,
            location_id=note_data.location_id,
            notes=note_data.notes,
            lot_id=note_data.lot_id,
            project_id=note_data.project_id,
            apply_to_group=note_data.apply_to_group
        )
        return {"message": "Nota actualizada correctamente"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))