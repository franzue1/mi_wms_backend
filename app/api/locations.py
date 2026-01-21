# app/api/locations.py
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
        
    # 1. Construir dict de filtros
    filters = {
        "path": path, "type": type, "warehouse_name": warehouse_name,
        "warehouse_status": warehouse_status
    }
    # 2. Limpiar Nones
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

    # 3. Llamar a la BD
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

    filters = {"path": path, "type": type, "warehouse_name": warehouse_name, "warehouse_status": warehouse_status}
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    try:
        # Asumimos que db.get_locations_count existe (debería, por el patrón)
        count = db.get_locations_count(company_id, filters=clean_filters)
        return count
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al contar ubicaciones: {e}")

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
    
    try:
        new_loc_id = db.create_location(
            company_id=company_id,
            name=location.name,
            path=location.path,
            type=location.type,
            category=location.category,
            warehouse_id=location.warehouse_id
        )
        created_loc = db.get_location_details_by_id(new_loc_id)
        return dict(created_loc)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
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

    try:
        current_data = db.get_location_details_by_id(location_id)
        if not current_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ubicación no encontrada")
        
        update_data = current_data.copy()
        update_data.update(location.dict(exclude_unset=True))
        
        db.update_location(
            location_id=location_id,
            company_id=company_id,
            name=update_data['name'],
            path=update_data['path'],
            type=update_data['type'],
            category=update_data['category'],
            warehouse_id=update_data['warehouse_id']
        )
        
        updated_loc = db.get_location_details_by_id(location_id)
        return dict(updated_loc)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
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

    # Reutilizamos los filtros
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

    try:
        filters = {"path": path, "type": type, "warehouse_name": warehouse_name, "warehouse_status": warehouse_status}
        clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

        locations_raw = db.get_locations_filtered_sorted(
            company_id, filters=clean_filters, sort_by=sort_by or 'id', 
            ascending=ascending, limit=None, offset=None
        )

        if not locations_raw:
            raise HTTPException(status_code=404, detail="No hay datos para exportar.")

        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        # Cabeceras para la exportación
        headers = ["path", "name", "type", "warehouse_name", "category"]
        writer.writerow(headers)

        LOCATION_TYPE_MAP = {
            "internal": "Ubicación Interna", "vendor": "Ubic. Proveedor (Virtual)",
            "customer": "Ubic. Cliente (Virtual)", "inventory": "Pérdida Inventario (Virtual)",
            "production": "Producción (Virtual)", "transit": "Tránsito (Virtual)",
        }

        for loc_row in locations_raw:
            loc_dict = dict(loc_row)
            # Escribimos los datos limpios
            writer.writerow([
                loc_dict.get('path', ''),
                loc_dict.get('name', ''),
                LOCATION_TYPE_MAP.get(loc_dict.get('type'), loc_dict.get('type', '')),
                loc_dict.get('warehouse_name', '') or '',
                loc_dict.get('category', '') or ''
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=ubicaciones.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")

@router.post("/import/csv", response_model=dict)
async def import_locations_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """ Importa ubicaciones desde CSV (Auto-genera Path si falta). """
    if "locations.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        content = await file.read()
        content_decoded = content.decode('utf-8-sig')
        file_io = io.StringIO(content_decoded)
        
        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(content_decoded[:1024], delimiters=";,")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ';'
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al leer el archivo: {e}")

    reader = csv.DictReader(file_io, dialect=dialect)
    
    try:
        rows = list(reader)
        if not rows: raise ValueError("El archivo CSV está vacío.")

        # Limpieza de cabeceras
        headers = {h.lower().strip() for h in reader.fieldnames or []}
        # Path ya no es estrictamente requerido en el CSV si se puede generar
        required_headers = {"name", "type"} 
        if not required_headers.issubset(headers):
            missing = required_headers - headers
            raise ValueError(f"Faltan columnas: {', '.join(sorted(list(missing)))}")

        LOCATION_TYPE_MAP_REVERSE = {
            "Ubicación Interna": "internal", "Ubic. Proveedor (Virtual)": "vendor",
            "Ubic. Cliente (Virtual)": "customer", "Pérdida Inventario (Virtual)": "inventory",
            "Producción (Virtual)": "production", "Tránsito (Virtual)": "transit",
        }
        
        # Cache de almacenes (Nombre -> ID y ID -> Código)
        warehouses_db = db.get_warehouses_simple(company_id)
        warehouse_map = {wh['name'].upper(): wh['id'] for wh in warehouses_db}
        
        # Necesitamos los códigos para auto-generar paths
        # Hacemos una consulta rápida o usamos un mapa auxiliar si get_warehouses_simple trajo el código
        # Asumiendo que get_warehouses_simple devuelve {'id', 'name', 'code'}
        warehouse_code_map = {wh['id']: wh['code'] for wh in warehouses_db}

        created, updated = 0, 0
        error_list = []

        for i, row in enumerate(rows):
            row_num = i + 2
            
            # Limpieza básica
            raw_path = row.get('path', '').strip()
            name = row.get('name', '').strip()
            type_str = row.get('type', '').strip()
            
            if not name and not type_str: continue # Saltar vacíos

            try:
                # 1. Validar Tipo
                type_code = LOCATION_TYPE_MAP_REVERSE.get(type_str)
                if not type_code:
                    if type_str in LOCATION_TYPE_MAP_REVERSE.values():
                        type_code = type_str
                    else:
                        raise ValueError(f"Tipo '{type_str}' inválido.")

                # 2. Validar Almacén
                warehouse_id = None
                if type_code == 'internal':
                    wh_name = row.get('warehouse_name', '').strip()
                    if not wh_name:
                        raise ValueError("warehouse_name es obligatorio para Ubicación Interna.")
                    
                    warehouse_id = warehouse_map.get(wh_name.upper())
                    if not warehouse_id:
                        raise ValueError(f"Almacén '{wh_name}' no existe.")

                # 3. AUTO-GENERACIÓN DE PATH (La Magia)
                final_path = raw_path
                if not final_path:
                    if type_code == 'internal' and warehouse_id:
                        wh_code = warehouse_code_map.get(warehouse_id)
                        if wh_code:
                            # Genera: WH/STOCK
                            final_path = f"{wh_code}/{name.upper()}"
                        else:
                            raise ValueError("No se pudo generar Path (Almacén sin código).")
                    else:
                        # Para virtuales, usamos el nombre como path si viene vacío
                        final_path = name.upper()

                # 4. Upsert
                existing_loc = db.get_location_by_path(company_id, final_path)

                payload = {
                    "name": name,
                    "path": final_path,
                    "type": type_code,
                    "category": row.get('category', '').strip() or None,
                    "warehouse_id": warehouse_id
                }

                if existing_loc:
                    db.update_location(existing_loc['id'], company_id, **payload)
                    updated += 1
                else:
                    db.create_location(company_id, **payload)
                    created += 1

            except Exception as e:
                error_list.append(f"Fila {row_num} ('{name}'): {e}")

        if error_list:
            display_errors = error_list[:10]
            if len(error_list) > 10: display_errors.append("...")
            # Lanzamos HTTPException directamente (el catch de abajo la dejará pasar)
            raise HTTPException(status_code=400, detail="Errores:\n- " + "\n- ".join(display_errors))

        return {"created": created, "updated": updated, "errors": 0}

    # [CORRECCIÓN CRÍTICA]
    # Primero capturamos HTTPException para que FastAPI la devuelva como 400 normal
    except HTTPException as he:
        raise he 
    # Luego capturamos cualquier otro crash inesperado como 500
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico: {e}")