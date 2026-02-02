# app/api/warehouses.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel
from datetime import date
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.warehouse_service import WarehouseService
from app.exceptions import ValidationError, NotFoundError, DuplicateError
import traceback
from fastapi.responses import StreamingResponse
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.WarehouseResponse])
async def get_all_warehouses(
    auth: AuthDependency,
    company_id: int = Query(...),
    skip: int = 0,
    limit: int = 100,
    
    # Estos deben coincidir con las claves de self.active_filters en Flet
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    code: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    address: Optional[str] = Query(None)
):
    """ Obtiene una lista de almacenes. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar WarehouseService para construir filtros
    clean_filters = WarehouseService.build_filter_dict(
        name=name,
        code=code,
        status=status,
        category_name=category_name,
        ruc=ruc,
        address=address
    )

    warehouses_raw = db.get_warehouses_filtered_sorted(
        company_id, 
        filters=clean_filters, 
        sort_by=sort_by, 
        ascending=ascending, 
        limit=limit, 
        offset=skip
    )
    return [dict(wh) for wh in warehouses_raw]

# --- ¡NUEVO ENDPOINT DE CONTEO! ---
@router.get("/count", response_model=int)
async def get_warehouses_count(
    auth: AuthDependency,
    company_id: int = Query(...),
    
    # MISMOS PARÁMETROS DE FILTRO QUE ARRIBA
    name: Optional[str] = Query(None),
    code: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    address: Optional[str] = Query(None)
):
    """ Obtiene el conteo total de almacenes para la paginación. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar WarehouseService para construir filtros
    clean_filters = WarehouseService.build_filter_dict(
        name=name,
        code=code,
        status=status,
        category_name=category_name,
        ruc=ruc,
        address=address
    )

    count = db.get_warehouses_count(company_id, filters=clean_filters)
    return count

@router.get("/simple", response_model=List[schemas.WarehouseSimple])
async def get_warehouses_simple_list(
    auth: AuthDependency,
    company_id: int = Query(...),
):
    """
    Devuelve una lista simple de almacenes (id, name, code)
    para usar en dropdowns en otras vistas (como Ubicaciones).
    """
    # No es necesario chequear permisos de 'warehouses.can_crud' aquí,
    # ya que esta es una función de ayuda para otras vistas (como 'locations.can_crud')
    
    try:
        warehouses_raw = db.get_warehouses_simple(company_id)
        return [dict(wh) for wh in warehouses_raw]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener lista simple de almacenes: {e}")

@router.get("/{warehouse_id}", response_model=schemas.WarehouseResponse)
async def get_warehouse(warehouse_id: int, auth: AuthDependency):
    """ Obtiene un almacén por su ID. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    wh = db.get_warehouse_details_by_id(warehouse_id)
    if not wh:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Almacén no encontrado")
    return dict(wh)

@router.post("/", response_model=schemas.WarehouseResponse, status_code=status.HTTP_201_CREATED)
async def create_warehouse(
    warehouse: schemas.WarehouseCreate,
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """ Crea un nuevo almacén y sus ubicaciones/operaciones por defecto. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar WarehouseService para preparar datos normalizados
    prepared_data = WarehouseService.prepare_warehouse_data(warehouse.dict())

    try:
        new_wh_id = await asyncio.to_thread(
            db.create_warehouse,
            company_id=company_id,
            name=prepared_data["name"],
            code=prepared_data["code"],
            category_id=prepared_data["category_id"],
            social_reason=prepared_data["social_reason"],
            ruc=prepared_data["ruc"],
            email=prepared_data["email"],
            phone=prepared_data["phone"],
            address=prepared_data["address"]
        )

        created_warehouse_raw = await asyncio.to_thread(db.get_warehouse_details_by_id, new_wh_id)

        if not created_warehouse_raw:
            raise HTTPException(status_code=500, detail="Almacén creado pero no encontrado.")

        return dict(created_warehouse_raw)

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except ValidationError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=ve.message)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")


@router.put("/{warehouse_id}", response_model=schemas.WarehouseResponse)
async def update_warehouse(
    warehouse_id: int,
    warehouse: schemas.WarehouseUpdate,
    auth: AuthDependency
):
    """ Actualiza un almacén existente. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    current_data = db.get_warehouse_details_by_id(warehouse_id)
    if not current_data:
        raise NotFoundError("Almacén no encontrado", "WH_NOT_FOUND")

    # Merge datos actuales con los nuevos
    update_data = dict(current_data)
    update_data.update(warehouse.dict(exclude_unset=True))

    # Usar WarehouseService para normalizar
    prepared_data = WarehouseService.prepare_warehouse_data(update_data)

    try:
        db.update_warehouse(
            wh_id=warehouse_id,
            name=prepared_data["name"],
            code=prepared_data["code"],
            category_id=prepared_data["category_id"],
            social_reason=prepared_data["social_reason"],
            ruc=prepared_data["ruc"],
            email=prepared_data["email"],
            phone=prepared_data["phone"],
            address=prepared_data["address"],
            status=WarehouseService.normalize_status(update_data.get("status"))
        )

        updated_wh = db.get_warehouse_details_by_id(warehouse_id)
        return dict(updated_wh)

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except ValidationError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=ve.message)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.delete("/{warehouse_id}", status_code=status.HTTP_200_OK)
async def inactivate_warehouse(warehouse_id: int, auth: AuthDependency):
    """ "Archiva" (desactiva) un almacén. No lo borra. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    success, message = db.inactivate_warehouse(warehouse_id)
    
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
        
    return {"message": message}

@router.get("/export/csv", response_class=StreamingResponse)
async def export_warehouses_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    code: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    address: Optional[str] = Query(None)
):
    """ Genera y transmite un archivo CSV de los almacenes filtrados. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    clean_filters = WarehouseService.build_filter_dict(
        name=name,
        code=code,
        status=status,
        category_name=category_name,
        ruc=ruc,
        address=address
    )

    warehouses_raw = db.get_warehouses_filtered_sorted(
        company_id,
        filters=clean_filters,
        sort_by=sort_by or 'id',
        ascending=ascending,
        limit=None,
        offset=None
    )

    if not warehouses_raw:
        raise NotFoundError("No hay datos para exportar", "EXPORT_NO_DATA")

    # Usar WarehouseService para generar CSV
    csv_content = WarehouseService.generate_csv_content(warehouses_raw)

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=almacenes.csv"}
    )


@router.post("/import/csv", response_model=dict)
async def import_warehouses_csv(
    auth: AuthDependency,
    file: UploadFile = File(...),
    company_id: int = Query(...)
):
    """ Importa almacenes desde un archivo CSV. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # 1. Leer y parsear CSV usando WarehouseService
    content = await file.read()
    rows, headers = WarehouseService.parse_csv_file(content)

    # 2. Validar headers requeridos
    WarehouseService.validate_csv_headers(headers)

    # 3. Cargar categorías de BD y validar referencias
    db_categories = db.get_warehouse_categories(company_id)
    valid_category_names = {cat['name'] for cat in db_categories}
    WarehouseService.validate_csv_categories(rows, valid_category_names)

    # 4. Crear mapeo de categorías
    cat_map = {cat['name']: cat['id'] for cat in db_categories}

    # 5. Procesar filas
    created, updated = 0, 0
    error_list = []

    for i, row in enumerate(rows):
        row_num = i + 2
        try:
            # Usar WarehouseService para procesar y normalizar la fila
            prepared = WarehouseService.process_csv_row(row, row_num, cat_map)

            result = db.upsert_warehouse_from_import(
                company_id=company_id,
                code=prepared["code"],
                name=prepared["name"],
                status=prepared["status"],
                social_reason=prepared["social_reason"],
                ruc=prepared["ruc"],
                email=prepared["email"],
                phone=prepared["phone"],
                address=prepared["address"],
                category_id=prepared["category_id"]
            )

            if result == "created":
                created += 1
            elif result == "updated":
                updated += 1

        except ValidationError as ve:
            error_list.append(ve.message)
        except Exception as e:
            error_list.append(f"Fila {row_num}: {e}")

    if error_list:
        raise ValidationError(
            "Importación con errores:\n- " + "\n- ".join(error_list),
            "CSV_IMPORT_ERRORS",
            {"errors": error_list}
        )

    return {"created": created, "updated": updated, "errors": 0}