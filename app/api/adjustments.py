#app/api/adjustments.py
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import asyncio
import io
import csv

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper de Filtros ---
def _parse_adjustment_filters(request: Request) -> dict:
    filters = {}
    KNOWN_FILTER_KEYS = {'name', 'state', 'responsible_user', 'adjustment_reason', 'src_path', 'dest_path'}
    RESERVED_KEYS = {'company_id', 'skip', 'limit', 'sort_by', 'ascending', 'token'}
    for key, value in request.query_params.items():
        if key not in RESERVED_KEYS and key in KNOWN_FILTER_KEYS and value:
            filters[key] = value
    return filters

# --- Endpoints de Listado ---
@router.get("/", response_model=List[schemas.AdjustmentListResponse])
async def get_all_adjustments(
    auth: AuthDependency, company_id: int, request: Request,
    skip: int = 0, limit: int = 50, sort_by: str = 'id', ascending: bool = False
):
    if "adjustments.can_view" not in auth.permissions: raise HTTPException(403, "No autorizado")
    try:
        filters = _parse_adjustment_filters(request)
        adj_raw = await asyncio.to_thread(db.get_adjustments_filtered_sorted, company_id, filters, sort_by, ascending, limit, skip)
        return [dict(adj) for adj in adj_raw]
    except Exception as e:
        traceback.print_exc(); raise HTTPException(500, f"Error: {e}")

@router.get("/count", response_model=int)
async def get_adjustments_count(auth: AuthDependency, company_id: int, request: Request):
    if "adjustments.can_view" not in auth.permissions: raise HTTPException(403, "No autorizado")
    try:
        filters = _parse_adjustment_filters(request)
        return await asyncio.to_thread(db.get_adjustments_count, company_id, filters)
    except Exception as e:
        traceback.print_exc(); raise HTTPException(500, f"Error: {e}")

# ==========================================
# --- NUEVOS ENDPOINTS IMPORT/EXPORT ---
# ==========================================

@router.get("/export/csv", response_class=StreamingResponse)
async def export_adjustments_csv(auth: AuthDependency, company_id: int = Query(...)):
    """ Descarga CSV con todos los ajustes y sus líneas. """
    if "adjustments.can_view" not in auth.permissions: raise HTTPException(403, "No autorizado")
    
    try:
        data = await asyncio.to_thread(db.get_adjustments_for_export, company_id)
        if not data: raise HTTPException(404, "No hay datos para exportar.")

        output = io.StringIO(newline='')
        
        # [CORRECCIÓN] Agregamos 'series' a las columnas para evitar ValueError
        fieldnames = ['referencia', 'razon', 'fecha', 'usuario', 'notas', 'estado', 
                      'ubicacion', 'sku', 'producto', 'cantidad', 'costo_unitario', 'series']
        
        # Usamos extrasaction='ignore' por seguridad, aunque con el campo agregado ya debería funcionar
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
        writer.writeheader()
        
        writer.writerows([dict(row) for row in data])
        
        output.seek(0)
        filename = f"ajustes_inventario_{company_id}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        traceback.print_exc(); raise HTTPException(500, f"Error exportando: {e}")

@router.post("/import/csv", status_code=201)
async def import_adjustments_csv(
    auth: AuthDependency, 
    company_id: int = Query(...), 
    file: UploadFile = File(...)
):
    """
    Importa ajustes masivos.
    """
    if "adjustments.can_create" not in auth.permissions: raise HTTPException(403, "No autorizado")

    try:
        content = await file.read()
        content_decoded = content.decode('utf-8-sig') 
        file_io = io.StringIO(content_decoded)
        
        sniffer = csv.Sniffer()
        try: dialect = sniffer.sniff(content_decoded[:2048], delimiters=';,')
        except: dialect = csv.excel; dialect.delimiter = ';'
        
        file_io.seek(0)
        reader = csv.DictReader(file_io, dialect=dialect)
        
        rows = [{k.lower().strip(): v.strip() for k, v in row.items() if k} for row in reader]
        
        if not rows: raise ValueError("Archivo vacío")
        
        required = {'sku', 'cantidad', 'ubicacion'}
        headers = set(rows[0].keys())
        if not required.issubset(headers):
            raise ValueError(f"Faltan columnas obligatorias: {required - headers}")

        count = await asyncio.to_thread(db.import_smart_adjustments_transaction, company_id, auth.username, rows)
        
        return {"message": f"Se crearon {count} documentos de ajuste correctamente."}

    except ValueError as ve: raise HTTPException(400, str(ve))
    except Exception as e:
        traceback.print_exc(); raise HTTPException(500, f"Error importando: {e}")