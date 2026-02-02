# app/api/locations.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel
from datetime import date
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.location_service import LocationService
from app.exceptions import ValidationError, NotFoundError
import traceback
from fastapi.responses import StreamingResponse

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.LocationResponse])
async def get_all_locations(
    auth: AuthDependency,
    company_id: int = Query(...),
    skip: int = 0,
    limit: int = 100,
    
    # --- ¡PARÁMETROS DE FILTRO Y ORDEN AÑADIDOS! ---
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    path: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    warehouse_name: Optional[str] = Query(None),
    warehouse_status: Optional[str] = Query(None) # Para filtrar por estado de almacén
):
    """ Obtiene una lista de ubicaciones filtrada y paginada. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar LocationService para construir filtros
    clean_filters = LocationService.build_filter_dict(
        path=path,
        loc_type=type,
        warehouse_name=warehouse_name,
        warehouse_status=warehouse_status
    )

    locations_raw = db.get_locations_filtered_sorted(
        company_id, 
        filters=clean_filters, 
        sort_by=sort_by, 
        ascending=ascending, 
        limit=limit, 
        offset=skip
    )
    return [dict(loc) for loc in locations_raw]

# --- ¡NUEVO ENDPOINT DE CONTEO! ---
@router.get("/count", response_model=int)
async def get_locations_count(
    auth: AuthDependency,
    company_id: int = Query(...),

    # Reutilizamos los filtros
    path: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    warehouse_name: Optional[str] = Query(None),
    warehouse_status: Optional[str] = Query(None)):
    
    """ Obtiene el conteo total de ubicaciones filtradas. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar LocationService para construir filtros
    clean_filters = LocationService.build_filter_dict(
        path=path,
        loc_type=type,
        warehouse_name=warehouse_name,
        warehouse_status=warehouse_status
    )

    count = db.get_locations_count(company_id, filters=clean_filters)
    return count

@router.get("/{location_id}", response_model=schemas.LocationResponse)
async def get_location(location_id: int, auth: AuthDependency):
    """ Obtiene una ubicación por su ID. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    loc = db.get_location_details_by_id(location_id)
    if not loc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ubicación no encontrada")
    return dict(loc)

@router.post("/", response_model=schemas.LocationResponse, status_code=status.HTTP_201_CREATED)
async def create_location(
    location: schemas.LocationCreate,
    auth: AuthDependency,
    company_id: int = Query(...),
):
    """ Crea una nueva ubicación. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar LocationService para preparar datos normalizados
    prepared_data = LocationService.prepare_location_data(location.dict())

    try:
        new_loc_id = db.create_location(
            company_id=company_id,
            name=prepared_data["name"],
            path=prepared_data["path"],
            type=prepared_data["type"],
            category=prepared_data["category"],
            warehouse_id=prepared_data["warehouse_id"]
        )
        created_loc = db.get_location_details_by_id(new_loc_id)
        return dict(created_loc)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except ValidationError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=ve.message)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.put("/{location_id}", response_model=schemas.LocationResponse)
async def update_location(
    location_id: int,
    location: schemas.LocationUpdate,
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """ Actualiza una ubicación existente. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    current_data = db.get_location_details_by_id(location_id)
    if not current_data:
        raise NotFoundError("Ubicación no encontrada", "LOC_NOT_FOUND")

    # Merge datos actuales con los nuevos
    update_data = dict(current_data)
    update_data.update(location.dict(exclude_unset=True))

    # Usar LocationService para normalizar
    prepared_data = LocationService.prepare_location_data(update_data)

    try:
        db.update_location(
            location_id=location_id,
            company_id=company_id,
            name=prepared_data["name"],
            path=prepared_data["path"],
            type=prepared_data["type"],
            category=prepared_data["category"],
            warehouse_id=prepared_data["warehouse_id"]
        )

        updated_loc = db.get_location_details_by_id(location_id)
        return dict(updated_loc)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except ValidationError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=ve.message)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.delete("/{location_id}", status_code=status.HTTP_200_OK)
async def delete_location(location_id: int, auth: AuthDependency):
    """ Elimina una ubicación (si no está en uso). """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    success, message = db.delete_location(location_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return {"message": message}

@router.get("/export/csv", response_class=StreamingResponse)
async def export_locations_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    path: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    warehouse_name: Optional[str] = Query(None),
    warehouse_status: Optional[str] = Query(None)
):
    """ Genera y transmite un archivo CSV de las ubicaciones filtradas. """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    clean_filters = LocationService.build_filter_dict(
        path=path,
        loc_type=type,
        warehouse_name=warehouse_name,
        warehouse_status=warehouse_status
    )

    locations_raw = db.get_locations_filtered_sorted(
        company_id,
        filters=clean_filters,
        sort_by=sort_by or 'id',
        ascending=ascending,
        limit=None,
        offset=None
    )

    if not locations_raw:
        raise NotFoundError("No hay datos para exportar", "EXPORT_NO_DATA")

    # Usar LocationService para generar CSV
    csv_content = LocationService.generate_csv_content(locations_raw)

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ubicaciones.csv"}
    )

@router.post("/import/csv", response_model=dict)
async def import_locations_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """ Importa ubicaciones desde CSV (Auto-genera Path si falta). """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # 1. Leer y parsear CSV usando LocationService
    content = await file.read()
    rows, headers = LocationService.parse_csv_file(content)

    # 2. Validar headers requeridos
    LocationService.validate_csv_headers(headers)

    # 3. Cargar almacenes de BD para mapeos
    warehouses_db = db.get_warehouses_simple(company_id)
    warehouse_map = {wh['name'].upper(): wh['id'] for wh in warehouses_db}
    warehouse_code_map = {wh['id']: wh['code'] for wh in warehouses_db}

    # 4. Procesar filas
    created, updated = 0, 0
    error_list = []

    for i, row in enumerate(rows):
        row_num = i + 2
        name = row.get('name', '').strip()
        type_str = row.get('type', '').strip()

        if not name and not type_str:
            continue  # Saltar filas vacías

        try:
            # Usar LocationService para procesar y normalizar la fila
            prepared = LocationService.process_csv_row(
                row, row_num, warehouse_map, warehouse_code_map
            )

            # Upsert
            existing_loc = db.get_location_by_path(company_id, prepared["path"])

            if existing_loc:
                db.update_location(
                    existing_loc['id'],
                    company_id,
                    name=prepared["name"],
                    path=prepared["path"],
                    type=prepared["type"],
                    category=prepared["category"],
                    warehouse_id=prepared["warehouse_id"]
                )
                updated += 1
            else:
                db.create_location(
                    company_id,
                    name=prepared["name"],
                    path=prepared["path"],
                    type=prepared["type"],
                    category=prepared["category"],
                    warehouse_id=prepared["warehouse_id"]
                )
                created += 1

        except ValidationError as ve:
            error_list.append(ve.message)
        except Exception as e:
            error_list.append(f"Fila {row_num} ('{name}'): {e}")

    if error_list:
        raise ValidationError(
            "Importación con errores:\n- " + "\n- ".join(error_list[:10]),
            "CSV_IMPORT_ERRORS",
            {"errors": error_list}
        )

    return {"created": created, "updated": updated, "errors": 0}