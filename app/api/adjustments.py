# app/api/adjustments.py
"""
Endpoints de Ajustes de Inventario (Conteos Cíclicos).
Delega lógica de negocio al AdjustmentService.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.adjustment_service import AdjustmentService
from app.exceptions import ValidationError, NotFoundError, ErrorCodes
import traceback
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]


# --- Helper de Filtros ---
def _parse_adjustment_filters(request: Request) -> dict:
    """Parsea filtros de query params para ajustes."""
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
    """Obtiene listado paginado de ajustes de inventario."""
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(403, "No autorizado")
    try:
        filters = _parse_adjustment_filters(request)
        adj_raw = await asyncio.to_thread(
            db.get_adjustments_filtered_sorted,
            company_id, filters, sort_by, ascending, limit, skip
        )
        return [dict(adj) for adj in adj_raw]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Error: {e}")


@router.get("/count", response_model=int)
async def get_adjustments_count(auth: AuthDependency, company_id: int, request: Request):
    """Obtiene el conteo total de ajustes para paginación."""
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(403, "No autorizado")
    try:
        filters = _parse_adjustment_filters(request)
        return await asyncio.to_thread(db.get_adjustments_count, company_id, filters)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Error: {e}")


# ==========================================
# --- ENDPOINTS IMPORT/EXPORT ---
# ==========================================

@router.get("/export/csv", response_class=StreamingResponse)
async def export_adjustments_csv(auth: AuthDependency, company_id: int = Query(...)):
    """
    Descarga CSV con todos los ajustes y sus líneas.
    Usa AdjustmentService para generar el contenido.
    """
    if "adjustments.can_view" not in auth.permissions:
        raise HTTPException(403, "No autorizado")

    try:
        # Obtener datos del repositorio
        data = await asyncio.to_thread(db.get_adjustments_for_export, company_id)

        if not data:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": ErrorCodes.EXPORT_NO_DATA,
                    "message": "No hay datos para exportar."
                }
            )

        # Generar CSV usando el servicio
        csv_content = AdjustmentService.generate_export_csv_content(data)

        filename = f"ajustes_inventario_{company_id}.csv"

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Error exportando: {e}")


@router.post("/import/csv", status_code=201)
async def import_adjustments_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """
    Importa ajustes masivos desde CSV.
    Usa AdjustmentService para parsear y validar el CSV.
    """
    if "adjustments.can_create" not in auth.permissions:
        raise HTTPException(403, "No autorizado")

    try:
        # 1. Leer contenido del archivo
        content = await file.read()

        # 2. Parsear CSV usando el servicio
        rows, headers = AdjustmentService.parse_adjustment_csv(content)

        # 3. Validar headers usando el servicio
        AdjustmentService.validate_adjustment_csv_headers(headers)

        # 4. Normalizar filas
        normalized_rows = [
            AdjustmentService.normalize_csv_row(row)
            for row in rows
        ]

        if not normalized_rows:
            raise ValidationError(
                "Archivo vacío",
                ErrorCodes.CSV_EMPTY_FILE
            )

        # 5. Ejecutar importación en el repositorio
        count = await asyncio.to_thread(
            db.import_smart_adjustments_transaction,
            company_id,
            auth.username,
            normalized_rows
        )

        return {"message": f"Se crearon {count} documentos de ajuste correctamente."}

    except ValidationError as ve:
        raise HTTPException(
            status_code=400,
            detail={
                "error": ve.code,
                "message": ve.message,
                "details": ve.details
            }
        )
    except ValueError as ve:
        raise HTTPException(400, str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Error importando: {e}")
