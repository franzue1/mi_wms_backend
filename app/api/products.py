# app/api/products.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel
from datetime import date
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.product_service import ProductService
from app.exceptions import ValidationError, WMSBaseException
import traceback
import io
import csv
from fastapi.responses import StreamingResponse
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/search-storable", response_model=List[schemas.ProductResponse])
async def search_storable_products(
    auth: AuthDependency,
    company_id: int = Query(...),
    term: str = Query(..., min_length=2) # Requiere al menos 2 caracteres
):
    """
    Busca productos ALMACENABLES y ACTIVOS por nombre o SKU.
    Optimizado para ser llamado por 'on_change' (con debounce).
    """
    try:
        # Usamos to_thread para la consulta
        products = await asyncio.to_thread(
            db.search_storable_products_by_term,
            company_id=company_id,
            search_term=term
        )
        return [dict(p) for p in products]
    except Exception as e:
        # ... (manejo de error)
        raise HTTPException(status_code=500, detail="Error al buscar productos")

@router.get("/", response_model=List[schemas.ProductResponse])
async def get_all_products(
    auth: AuthDependency,
    company_id: int = Query(...),
    skip: int = 0,
    limit: int = 100,
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    uom_name: Optional[str] = Query(None),
    tracking: Optional[str] = Query(None),
    ownership: Optional[str] = Query(None)
):
    """Obtiene una lista de productos filtrada y paginada."""
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso para ver productos")

    # Usar servicio para construir filtros (elimina duplicación)
    clean_filters = ProductService.build_filter_dict(
        name=name, sku=sku, category_name=category_name,
        uom_name=uom_name, tracking=tracking, ownership=ownership
    )

    products_raw = db.get_products_filtered_sorted(
        company_id,
        filters=clean_filters,
        sort_by=sort_by,
        ascending=ascending,
        limit=limit,
        offset=skip
    )
    return [dict(p) for p in products_raw]


@router.get("/count", response_model=int)
async def get_products_count(
    auth: AuthDependency,
    company_id: int = Query(...),
    name: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    uom_name: Optional[str] = Query(None),
    tracking: Optional[str] = Query(None),
    ownership: Optional[str] = Query(None)
):
    """Obtiene el conteo total de productos para la paginación."""
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar servicio para construir filtros
    clean_filters = ProductService.build_filter_dict(
        name=name, sku=sku, category_name=category_name,
        uom_name=uom_name, tracking=tracking, ownership=ownership
    )

    count = db.get_products_count(company_id, filters=clean_filters)
    return count


@router.post("/", response_model=schemas.ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(
    product: schemas.ProductCreate,
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """Crea un nuevo producto."""
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso para crear productos")

    try:
        # Validar y normalizar datos usando el servicio
        validated_data = ProductService.prepare_product_data(
            sku=product.sku,
            name=product.name,
            tracking=product.tracking,
            ownership=product.ownership,
            standard_price=product.standard_price,
            category_id=product.category_id,
            uom_id=product.uom_id
        )

        new_product_id = db.create_product(
            name=validated_data['name'],
            sku=validated_data['sku'],
            category_id=validated_data['category_id'],
            tracking=validated_data['tracking'],
            uom_id=validated_data['uom_id'],
            company_id=company_id,
            ownership=validated_data['ownership'],
            standard_price=validated_data['standard_price']
        )
        created_product_raw = db.get_product_details(new_product_id)
        return dict(created_product_raw)

    except WMSBaseException:
        raise  # El exception handler global lo maneja
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
    """Actualiza un producto existente por su ID."""
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        current_data = db.get_product_details(product_id)
        if not current_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado")

        # Merge datos actuales con nuevos
        update_data = dict(current_data)
        update_data.update(product.dict(exclude_unset=True))

        # Validar y normalizar usando el servicio
        validated_data = ProductService.prepare_product_data(
            sku=update_data['sku'],
            name=update_data['name'],
            tracking=update_data['tracking'],
            ownership=update_data['ownership'],
            standard_price=update_data['standard_price'],
            category_id=update_data['category_id'],
            uom_id=update_data['uom_id']
        )

        db.update_product(
            product_id=product_id,
            name=validated_data['name'],
            sku=validated_data['sku'],
            category_id=validated_data['category_id'],
            tracking=validated_data['tracking'],
            uom_id=validated_data['uom_id'],
            ownership=validated_data['ownership'],
            standard_price=validated_data['standard_price']
        )

        updated_product_raw = db.get_product_details(product_id)
        return dict(updated_product_raw)

    except WMSBaseException:
        raise  # El exception handler global lo maneja
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

@router.get("/export/csv", response_class=StreamingResponse)
async def export_products_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    uom_name: Optional[str] = Query(None),
    tracking: Optional[str] = Query(None),
    ownership: Optional[str] = Query(None)
):
    """Genera y transmite un archivo CSV de los productos filtrados."""
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        # Usar servicio para construir filtros
        clean_filters = ProductService.build_filter_dict(
            name=name, sku=sku, category_name=category_name,
            uom_name=uom_name, tracking=tracking, ownership=ownership
        )

        # Obtener todos los datos (sin paginación)
        products_raw = db.get_products_filtered_sorted(
            company_id,
            filters=clean_filters,
            sort_by=sort_by or 'id',
            ascending=ascending,
            limit=None,
            offset=None
        )

        if not products_raw:
            raise HTTPException(status_code=404, detail="No hay datos para exportar con esos filtros.")

        # Usar servicio para generar CSV
        csv_content = ProductService.generate_csv_content(products_raw)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=productos.csv"}
        )

    except WMSBaseException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")
    
@router.post("/import/csv", response_model=dict)
async def import_products_csv(
    auth: AuthDependency,
    file: UploadFile = File(...),
    company_id: int = Query(...)
):
    """Importa productos desde un archivo CSV."""
    if "products.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        # 1. Leer archivo
        content = await file.read()

        # 2. Parsear CSV (Service Layer)
        rows, headers = ProductService.parse_csv_file(content)

        # 3. Validar headers (Service Layer)
        ProductService.validate_csv_headers(headers)

        # 4. Cargar datos de referencia (Repository - SQL puro)
        categories_map = {cat['name']: cat['id'] for cat in db.get_product_categories(company_id)}
        uoms_map = {uom['name']: uom['id'] for uom in db.get_uoms(company_id)}

        # 5. Validar referencias (Service Layer)
        ProductService.validate_csv_references(rows, categories_map, uoms_map)

        # 6. Procesar filas
        created, updated = 0, 0
        error_list = []

        for i, row in enumerate(rows):
            row_num = i + 2
            sku = row.get('sku', '').strip()

            try:
                # Validar y normalizar fila (Service Layer)
                validated_data = ProductService.process_csv_row(
                    row, row_num, categories_map, uoms_map
                )

                # Insertar/Actualizar (Repository - SQL puro)
                result = db.upsert_product_from_import(
                    company_id=company_id,
                    sku=validated_data['sku'],
                    name=validated_data['name'],
                    category_id=validated_data['category_id'],
                    uom_id=validated_data['uom_id'],
                    tracking=validated_data['tracking'],
                    ownership=validated_data['ownership'],
                    price=validated_data['standard_price']
                )

                if result == "created":
                    created += 1
                elif result == "updated":
                    updated += 1

            except ValidationError as ve:
                error_list.append(f"{ve.message}")
            except Exception as e:
                error_list.append(f"Fila {row_num} (SKU: {sku}): {e}")

        if error_list:
            raise HTTPException(
                status_code=400,
                detail="Importación fallida. Corrija los errores y reintente:\n- " + "\n- ".join(error_list)
            )

        return {"created": created, "updated": updated, "errors": 0}

    except WMSBaseException:
        raise  # El exception handler global lo maneja
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico al procesar CSV: {e}")
    
class SKUImportRequest(BaseModel):
    company_id: int
    raw_text: str


class SKUImportResponse(BaseModel):
    found: List[dict]
    errors: List[str]


@router.post("/validate-skus-import", response_model=SKUImportResponse)
async def validate_skus_for_import(
    request: SKUImportRequest,
    auth: AuthDependency
):
    """
    Valida una lista de SKUs pegados desde texto crudo.
    Devuelve los productos encontrados y los errores.
    """
    try:
        # 1. Parsear texto (Service Layer)
        parsed_skus, parse_errors = ProductService.parse_sku_text(request.raw_text)

        if not parsed_skus:
            return {"found": [], "errors": parse_errors}

        # 2. Buscar productos en BD (Repository - SQL puro)
        skus_to_find = list(parsed_skus.keys())
        found_products = await asyncio.to_thread(
            db.find_products_by_skus,
            company_id=request.company_id,
            skus=skus_to_find
        )

        # 3. Construir respuesta (Service Layer)
        final_list, not_found_errors = ProductService.build_sku_import_response(
            parsed_skus, found_products
        )

        # Combinar errores de parsing y de productos no encontrados
        all_errors = parse_errors + not_found_errors

        return {"found": final_list, "errors": all_errors}

    except WMSBaseException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al procesar SKUs: {e}")
