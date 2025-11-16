# app/api/products.py
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

@router.get("/export/csv", response_class=StreamingResponse)
async def export_products_csv(
    auth: AuthDependency,
    company_id: int = 1,

    # Reutilizamos los mismos filtros que la vista principal
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None),
    uom_name: Optional[str] = Query(None),
    tracking: Optional[str] = Query(None),
    ownership: Optional[str] = Query(None)
):
    """
    Genera y transmite un archivo CSV de los productos filtrados.
    """
    if "products.can_crud" not in auth.permissions: # O un permiso de exportación
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        # 1. Construir filtros
        filters = {
            "name": name, "sku": sku, "category_name": category_name,
            "uom_name": uom_name, "tracking": tracking, "ownership": ownership
        }
        clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

        # 2. Obtener TODOS los datos (sin paginación, limit=None)
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

        # 3. Crear un archivo CSV en memoria
        output = io.StringIO(newline='')
        # Usar ';' como delimitador para compatibilidad con Excel en español
        writer = csv.writer(output, delimiter=';') 

        # 4. Escribir cabeceras
        headers = ["sku", "name", "ownership", "standard_price", "category_name", "uom_name", "tracking"]
        writer.writerow(headers)

        # 5. Escribir datos
        for prod_row in products_raw:
            prod_dict = dict(prod_row)
            writer.writerow([prod_dict.get(h, '') for h in headers])

        # 6. Preparar la respuesta para streaming
        output.seek(0)

        # 7. Devolver el archivo
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=productos.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")
    
# app/api/products.py

@router.post("/import/csv", response_model=dict)
async def import_products_csv(
    auth: AuthDependency,
    file: UploadFile = File(...),
    company_id: int = Query(...)  # <-- CORRECCIÓN 1: Requerir company_id
):
    """ Importa productos desde un archivo CSV. """
    if "products.can_crud" not in auth.permissions:
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
        required_headers = {"sku", "name", "category_name", "uom_name", "tracking", "ownership", "standard_price"}
        
        if not required_headers.issubset(headers):
            missing = required_headers - headers
            raise ValueError(f"Faltan columnas: {', '.join(sorted(list(missing)))}")

        # --- CORRECCIÓN 2: Validar categorías y UdM usando el company_id ---
        all_db_categories = {cat['name']: cat['id'] for cat in db.get_product_categories(company_id)}
        all_db_uoms = {uom['name']: uom['id'] for uom in db.get_uoms()} # Asumiendo que UdM es global

        invalid_categories = set()
        invalid_uoms = set()

        for row in rows:
            cat_name = row.get('category_name', '').strip()
            uom_name = row.get('uom_name', '').strip()
            if cat_name and cat_name not in all_db_categories:
                invalid_categories.add(cat_name)
            if uom_name and uom_name not in all_db_uoms:
                invalid_uoms.add(uom_name)
        
        errors_to_report = []
        if invalid_categories:
            errors_to_report.append(f"Categorías no existen: {', '.join(sorted(list(invalid_categories)))}")
        if invalid_uoms:
            errors_to_report.append(f"UdM no existen: {', '.join(sorted(list(invalid_uoms)))}")
        
        if errors_to_report:
            raise ValueError(". ".join(errors_to_report))
        # --- FIN DE LA CORRECCIÓN 2 ---

        created, updated = 0, 0
        error_list = []
        
        for i, row in enumerate(rows):
            row_num = i + 2
            sku = row.get('sku', '').strip()
            name = row.get('name', '').strip()
            
            try:
                if not sku or not name:
                    raise ValueError("sku y name son obligatorios.")
                
                price = float(row.get('standard_price', '0').replace(',', '.'))
                if price < 0:
                    raise ValueError("standard_price no puede ser negativo.")
                
                tracking = row.get('tracking', 'none').strip()
                if tracking not in ['none', 'lot', 'serial']:
                    raise ValueError(f"Valor de 'tracking' inválido: {tracking}")
                
                ownership = row.get('ownership', 'owned').strip()
                if ownership not in ['owned', 'consigned']:
                    raise ValueError(f"Valor de 'ownership' inválido: {ownership}")

                category_id = all_db_categories[row['category_name'].strip()]
                uom_id = all_db_uoms[row['uom_name'].strip()]

                # Esta función de BD debe manejar la lógica de INSERT/UPDATE
                result = db.upsert_product_from_import(
                    company_id=company_id, sku=sku, name=name,
                    category_id=category_id, uom_id=uom_id,
                    tracking=tracking, ownership=ownership, price=price
                )
                
                if result == "created": created += 1
                elif result == "updated": updated += 1
            
            except Exception as e:
                error_list.append(f"Fila {row_num} (SKU: {sku}): {e}")

        if error_list:
            raise HTTPException(
                status_code=400, 
                detail="Importación fallida. Corrija los errores y reintente:\n- " + "\n- ".join(error_list)
            )

        return {"created": created, "updated": updated, "errors": 0}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico al procesar CSV: {e}")