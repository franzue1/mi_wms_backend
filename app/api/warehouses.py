# app/api/warehouses.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel
from datetime import date
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import io
import csv
from fastapi.responses import StreamingResponse

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.WarehouseResponse])
async def get_all_warehouses(
    auth: AuthDependency,
    company_id: int = 1,
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
        
    # 1. Construir el dict de filtros
    filters = {
        "name": name, "code": code, "status": status, 
        "category_name": category_name, "ruc": ruc, "address": address
    }
    # 2. Limpiar Nones
    clean_filters = {k: v for k, v in filters.items() if v is not None}
    
    # 3. Llamar a la DB con todos los parámetros
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
    company_id: int = 1,
    
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

    # 1. Construir el dict de filtros
    filters = {
        "name": name, "code": code, "status": status, 
        "category_name": category_name, "ruc": ruc, "address": address
    }
    # 2. Limpiar Nones
    clean_filters = {k: v for k, v in filters.items() if v is not None}
    
    # 3. Llamar a la DB
    count = db.get_warehouses_count(company_id, filters=clean_filters)
    return count

@router.get("/simple", response_model=List[schemas.WarehouseSimple])
async def get_warehouses_simple_list(
    auth: AuthDependency,
    company_id: int = 1
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
    company_id: int = 1 # Fijo por ahora
):
    """ Crea un nuevo almacén y sus ubicaciones/operaciones por defecto. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        with db.connect_db() as conn:
            with conn.cursor() as cursor:
                db._create_warehouse_with_cursor(
                    cursor,
                    name=warehouse.name,
                    code=warehouse.code.upper(),
                    category_id=warehouse.category_id,
                    company_id=company_id,
                    social_reason=warehouse.social_reason,
                    ruc=warehouse.ruc,
                    email=warehouse.email,
                    phone=warehouse.phone,
                    address=warehouse.address,
                    status=warehouse.status
                )
        
        new_wh_raw = db.execute_query("SELECT id FROM warehouses WHERE code = %s AND company_id = %s", (warehouse.code.upper(), company_id), fetchone=True)
        if not new_wh_raw:
            raise HTTPException(status_code=500, detail="Error al verificar la creación del almacén.")
            
        created_warehouse = db.get_warehouse_details_by_id(new_wh_raw['id'])
        return dict(created_warehouse)

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
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

    try:
        current_data = db.get_warehouse_details_by_id(warehouse_id)
        if not current_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Almacén no encontrado")
        
        update_data = current_data.copy()
        update_data.update(warehouse.dict(exclude_unset=True))
        
        db.update_warehouse(
            wh_id=warehouse_id,
            name=update_data['name'],
            code=update_data['code'],
            category_id=update_data['category_id'],
            social_reason=update_data['social_reason'],
            ruc=update_data['ruc'],
            email=update_data['email'],
            phone=update_data['phone'],
            address=update_data['address'],
            status=update_data['status']
        )
        
        updated_wh = db.get_warehouse_details_by_id(warehouse_id)
        return dict(updated_wh)

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
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
    company_id: int = 1,

    # Reutilizamos los filtros de la vista principal
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

    try:
        filters = {"name": name, "code": code, "status": status, "category_name": category_name, "ruc": ruc, "address": address}
        clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

        warehouses_raw = db.get_warehouses_filtered_sorted(
            company_id, filters=clean_filters, sort_by=sort_by or 'id', 
            ascending=ascending, limit=None, offset=None
        )

        if not warehouses_raw:
            raise HTTPException(status_code=404, detail="No hay datos para exportar.")

        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        headers = ["code", "name", "status", "social_reason", "ruc", "email", "phone", "address", "category_name"]
        writer.writerow(headers)

        for wh_row in warehouses_raw:
            wh_dict = dict(wh_row)
            writer.writerow([wh_dict.get(h, '') for h in headers])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=almacenes.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")

@router.post("/import/csv", response_model=dict)
async def import_warehouses_csv(
    auth: AuthDependency,
    company_id: int = 1,
    file: UploadFile = File(...)
):
    """ Importa almacenes desde un archivo CSV. """
    if "warehouses.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        content = await file.read()
        content_decoded = content.decode('utf-8-sig')
        file_io = io.StringIO(content_decoded)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al leer el archivo: {e}")

    reader = csv.DictReader(file_io, delimiter=';')
    try:
        rows = list(reader)
        if not rows: raise ValueError("El archivo CSV está vacío.")
        
        headers = {h.lower().strip() for h in reader.fieldnames or []}
        required_headers = {"code", "name", "status", "social_reason", "ruc", "email", "phone", "address", "category_name"}
        if not required_headers.issubset(headers):
            missing = required_headers - headers
            raise ValueError(f"Faltan columnas: {', '.join(sorted(list(missing)))}")

        # Validar categorías (BD Call)
        all_db_categories = {cat['name'] for cat in db.get_warehouse_categories()}
        invalid_categories = {row.get('category_name', '').strip() for row in rows if row.get('category_name', '').strip() and row.get('category_name', '').strip() not in all_db_categories}
        if invalid_categories:
            raise ValueError(f"Categorías no existen: {', '.join(invalid_categories)}")
        
        cat_map = {cat['name']: cat['id'] for cat in db.get_warehouse_categories()}

        created, updated = 0, 0
        error_list = []
        
        for i, row in enumerate(rows):
            row_num = i + 2
            code = row.get('code', '').strip().upper()
            name = row.get('name', '').strip()
            status = row.get('status', '').lower().strip()
            
            try:
                if not code or not name or not status or not row.get('category_name'):
                    raise ValueError("code, name, status, y category_name son obligatorios.")
                if status not in ['activo', 'inactivo']:
                    raise ValueError(f"Estado '{status}' inválido, debe ser 'activo' o 'inactivo'.")
                
                category_id = cat_map.get(row.get('category_name', '').strip())
                if category_id is None:
                    raise ValueError(f"Categoría '{row.get('category_name')}' no encontrada (cache).")

                # --- Aquí es donde ocurre la llamada a la BD ---
                result = db.upsert_warehouse_from_import(
                    company_id=company_id, code=code, name=name, status=status,
                    social_reason=row.get('social_reason', '').strip(),
                    ruc=row.get('ruc', '').strip(),
                    email=row.get('email', '').strip(),
                    phone=row.get('phone', '').strip(),
                    address=row.get('address', '').strip(),
                    category_id=category_id
                )
                
                if result == "created": created += 1
                elif result == "updated": updated += 1
            
            except Exception as e:
                # Captura el AttributeError o cualquier error de la BD
                error_list.append(f"Fila {row_num} (Code: {code}): {e}")

        # --- ¡ESTA ES LA CORRECCIÓN! ---
        # Si hubo CUALQUIER error, fallamos toda la operación
        if error_list:
            print(f"Importación fallida con {len(error_list)} errores.")
            raise HTTPException(
                status_code=400, 
                detail="Importación fallida. Corrija los errores y reintente:\n- " + "\n- ".join(error_list)
            )
        # --- FIN DE LA CORRECCIÓN ---

        return {"created": created, "updated": updated, "errors": 0}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico al procesar CSV: {e}")