# app/api/reports.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData
from datetime import date, datetime # Asegúrate de que datetime esté importado
import traceback # Importa traceback

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/dashboard-kpis", response_model=schemas.DashboardResponse)
async def get_dashboard_kpis(
    auth: AuthDependency,
    company_id: int = 1
):
    """ Obtiene TODOS los KPIs y datos de gráficos para el Dashboard. """
    if "nav.dashboard.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        pending_kpis_raw = db.get_dashboard_kpis(company_id)
        value_kpis_raw = db.get_inventory_value_kpis(company_id)
        
        ots_pendientes_result = db.execute_query(
            "SELECT COUNT(*) as count FROM work_orders WHERE phase != 'Liquidado' AND company_id = %s",
            (company_id,), fetchone=True
        )
        ots_pendientes = ots_pendientes_result['count'] if ots_pendientes_result else 0

        throughput_raw = db.get_operations_throughput(company_id)
        throughput_chart = [{"day": day.strftime("%a"), "count": count} for day, count in throughput_raw]

        aging_chart = db.get_inventory_aging(company_id, tracked_only=True)

        response = schemas.DashboardResponse(
            pending_kpis=schemas.DashboardKPIs(**pending_kpis_raw),
            value_kpis=schemas.InventoryValueKPIs(**value_kpis_raw),
            pending_ots=ots_pendientes,
            throughput_chart=throughput_chart,
            aging_chart=aging_chart
        )
        return response
        
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar KPIs: {e}")

@router.get("/stock-summary", response_model=List[schemas.StockReportResponse])
async def get_stock_summary_report(
    auth: AuthDependency,
    company_id: int = 1,
    warehouse_id: Optional[int] = None,
    sku: Optional[str] = None,
    product_name: Optional[str] = None,
    category_name: Optional[str] = None
):
    """ Obtiene el reporte de stock resumido (agrupado por producto/ubicación). """
    if "reports.stock.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    filters = { "warehouse_id": warehouse_id, "sku": sku, "product_name": product_name, "category_name": category_name }
    filters = {k: v for k, v in filters.items() if v is not None}

    try:
        stock_data = db.get_stock_summary_filtered_sorted(company_id=company_id, filters=filters)
        return [dict(row) for row in stock_data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al generar reporte de stock: {e}")

@router.get("/aging", response_model=List[schemas.AgingDetailResponse])
async def get_aging_report(
    auth: AuthDependency,
    company_id: int = 1,
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
    company_id: int = 1,
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
    company_id: int = 1,
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
    company_id: int = 1,
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