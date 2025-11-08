# app/api/products.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.ProductResponse])
async def get_all_products(
    auth: AuthDependency,
    company_id: int = 1,
    skip: int = 0,
    limit: int = 100,
    
    # --- ¡PARÁMETROS DE FILTRO Y ORDEN AÑADIDOS! ---
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    uom_name: Optional[str] = Query(None),
    tracking: Optional[str] = Query(None),
    ownership: Optional[str] = Query(None)
):
    """ Obtiene una lista de productos filtrada y paginada. """
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso para ver productos")
        
    # 1. Construir el dict de filtros
    filters = {
        "name": name, "sku": sku, "category_name": category_name,
        "uom_name": uom_name, "tracking": tracking, "ownership": ownership
    }
    # 2. Limpiar Nones
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    # 3. Llamar a la BD
    products_raw = db.get_products_filtered_sorted(
        company_id, 
        filters=clean_filters, 
        sort_by=sort_by, 
        ascending=ascending, 
        limit=limit, 
        offset=skip
    )
    return [dict(p) for p in products_raw]

# --- ¡NUEVO ENDPOINT DE CONTEO! ---
@router.get("/count", response_model=int)
async def get_products_count(
    auth: AuthDependency,
    company_id: int = 1,
    
    # Mismos filtros que get_all_products
    name: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    uom_name: Optional[str] = Query(None),
    tracking: Optional[str] = Query(None),
    ownership: Optional[str] = Query(None)
):
    """ Obtiene el conteo total de productos para la paginación. """
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = {
        "name": name, "sku": sku, "category_name": category_name,
        "uom_name": uom_name, "tracking": tracking, "ownership": ownership
    }
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    count = db.get_products_count(company_id, filters=clean_filters)
    return count

@router.post("/", response_model=schemas.ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(
    product: schemas.ProductCreate,
    auth: AuthDependency,
    company_id: int = 1
):
    """ Crea un nuevo producto. """
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso para crear productos")

    try:
        new_product_id = db.create_product(
            name=product.name, sku=product.sku,
            category_id=product.category_id, tracking=product.tracking,
            uom_id=product.uom_id, company_id=company_id,
            ownership=product.ownership, standard_price=product.standard_price
        )
        created_product_raw = db.get_product_details(new_product_id)
        return dict(created_product_raw)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.get("/{product_id}", response_model=schemas.ProductResponse)
async def get_product(product_id: int, auth: AuthDependency):
    """ Obtiene un producto por su ID. """
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso")
        
    product_raw = db.get_product_details(product_id)
    if not product_raw:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado")
    return dict(product_raw)

@router.put("/{product_id}", response_model=schemas.ProductResponse)
async def update_product(
    product_id: int,
    product: schemas.ProductUpdate,
    auth: AuthDependency
):
    """ Actualiza un producto existente por su ID. """
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        current_data = db.get_product_details(product_id)
        if not current_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado")
        
        update_data = dict(current_data)
        update_data.update(product.dict(exclude_unset=True))
        
        db.update_product(
            product_id=product_id, name=update_data['name'], sku=update_data['sku'],
            category_id=update_data['category_id'], tracking=update_data['tracking'],
            uom_id=update_data['uom_id'], ownership=update_data['ownership'],
            standard_price=update_data['standard_price']
        )
        
        updated_product_raw = db.get_product_details(product_id)
        return dict(updated_product_raw)

    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")


@router.delete("/{product_id}", status_code=status.HTTP_200_OK)
async def delete_product(product_id: int, auth: AuthDependency):
    """ Elimina un producto por su ID. """
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    success, message = db.delete_product(product_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return {"message": message}