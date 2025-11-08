# app/api/warehouses.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData

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