# app/api/partners.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional, Dict
from pydantic import BaseModel
from datetime import date
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import io # <-- AÑADIR
import csv # <-- AÑADIR
from fastapi.responses import StreamingResponse # <-- AÑADIR

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.PartnerResponse])
async def get_all_partners(
    auth: AuthDependency,
    company_id: int = Query(...),
    skip: int = 0,
    limit: int = 100,
    
    # --- ¡PARÁMETROS DE FILTRO Y ORDEN AÑADIDOS! ---
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    social_reason: Optional[str] = Query(None),
    address: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None)
):
    """ Obtiene una lista de socios (proveedores/clientes). """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    # 1. Construir dict de filtros
    filters = {
        "name": name, "ruc": ruc, "social_reason": social_reason,
        "address": address, "category_name": category_name
    }
    # 2. Limpiar Nones
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

    # 3. Llamar a la BD
    partners_raw = db.get_partners_filtered_sorted(
        company_id, 
        filters=clean_filters,
        sort_by=sort_by, 
        ascending=ascending, 
        limit=limit, 
        offset=skip
    )
    return [dict(p) for p in partners_raw]


# --- ¡NUEVO ENDPOINT DE CONTEO! ---
@router.get("/count", response_model=int)
async def get_partners_count(
    auth: AuthDependency,
    company_id: int = Query(...),
    
    # Mismos filtros que get_all_partners
    name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    social_reason: Optional[str] = Query(None),
    address: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None)
):
    """ Obtiene el conteo total de socios para la paginación. """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    filters = {
        "name": name, "ruc": ruc, "social_reason": social_reason,
        "address": address, "category_name": category_name
    }
    clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    count = db.get_partners_count(company_id, filters=clean_filters)
    return count

@router.get("/{partner_id}", response_model=schemas.PartnerResponse)
async def get_partner(partner_id: int, auth: AuthDependency):
    """ Obtiene un socio por su ID. """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    partner = db.get_partner_details_by_id(partner_id)
    if not partner:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Socio no encontrado")
    return dict(partner)

@router.post("/", response_model=schemas.PartnerResponse, status_code=status.HTTP_201_CREATED)
async def create_partner(
    partner: schemas.PartnerCreate,
    auth: AuthDependency,
    company_id: int = Query(...),
):
    """ Crea un nuevo socio. """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_partner_id = db.create_partner(
            name=partner.name, category_id=partner.category_id,
            company_id=company_id, social_reason=partner.social_reason,
            ruc=partner.ruc, email=partner.email,
            phone=partner.phone, address=partner.address
        )
        created_partner = db.get_partner_details_by_id(new_partner_id)
        return dict(created_partner)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.put("/{partner_id}", response_model=schemas.PartnerResponse)
async def update_partner(
    partner_id: int,
    partner: schemas.PartnerUpdate,
    auth: AuthDependency
):
    """ Actualiza un socio existente. """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        current_data = db.get_partner_details_by_id(partner_id)
        if not current_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Socio no encontrado")
        
        update_data = current_data.copy()
        update_data.update(partner.dict(exclude_unset=True))
        
        db.update_partner(
            partner_id=partner_id, name=update_data['name'],
            category_id=update_data['category_id'], social_reason=update_data['social_reason'],
            ruc=update_data['ruc'], email=update_data['email'],
            phone=update_data['phone'], address=update_data['address']
        )
        
        updated_partner = db.get_partner_details_by_id(partner_id)
        return dict(updated_partner)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error interno: {e}")

@router.delete("/{partner_id}", status_code=status.HTTP_200_OK)
async def delete_partner(partner_id: int, auth: AuthDependency):
    """ Elimina un socio (si no está en uso). """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    success, message = db.delete_partner(partner_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return {"message": message}

@router.get("/export/csv", response_class=StreamingResponse)
async def export_partners_csv(
    auth: AuthDependency,
    company_id: int = Query(...),

    # Reutilizamos los filtros de la vista principal
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    social_reason: Optional[str] = Query(None),
    address: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None)
):
    """ Genera y transmite un archivo CSV de los socios filtrados. """
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        filters = {"name": name, "ruc": ruc, "social_reason": social_reason, "address": address, "category_name": category_name}
        clean_filters = {k: v for k, v in filters.items() if v is not None and v != ""}

        partners_raw = db.get_partners_filtered_sorted(
            company_id, filters=clean_filters, sort_by=sort_by or 'id', 
            ascending=ascending, limit=None, offset=None
        )

        if not partners_raw:
            raise HTTPException(status_code=404, detail="No hay datos para exportar.")

        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        headers = ["name", "category_name", "ruc", "social_reason", "address", "email", "phone"]
        writer.writerow(headers)

        for partner_row in partners_raw:
            partner_dict = dict(partner_row)
            writer.writerow([partner_dict.get(h, '') for h in headers])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=socios.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al generar CSV: {e}")

@router.post("/import/csv", response_model=dict)
async def import_partners_csv(
    auth: AuthDependency,
    company_id: int = Query(...), # <-- 1. Haz que el company_id sea REQUERIDO
    file: UploadFile = File(...)
):
    """ Importa socios (partners) desde un archivo CSV. """
    if "partners.can_crud" not in auth.permissions:
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
        required_headers = {"name", "category_name"} # Mínimos requeridos

        if not required_headers.issubset(headers):
            missing = required_headers - headers
            raise ValueError(f"Faltan columnas obligatorias: {', '.join(sorted(list(missing)))}")

        # Validar categorías
        all_db_categories = {cat['name'] for cat in db.get_partner_categories(company_id)}
        invalid_categories = {row.get('category_name','').strip() for row in rows if row.get('category_name','').strip() and row.get('category_name','').strip() not in all_db_categories}
        if invalid_categories:
            raise ValueError(f"Las siguientes categorías no existen: {', '.join(sorted(list(invalid_categories)))}")        
        cat_map = {cat['name']: cat['id'] for cat in db.get_partner_categories(company_id)}

        created, updated = 0, 0
        error_list = []

        for i, row in enumerate(rows):
            row_num = i + 2
            name = row.get('name', '').strip()
            category_name = row.get('category_name', '').strip()

            try:
                if not name or not category_name:
                    raise ValueError("name y category_name son obligatorios.")

                category_id = cat_map.get(category_name)
                if category_id is None:
                    raise ValueError(f"Categoría '{category_name}' no encontrada (cache).")

                result = db.upsert_partner_from_import(
                    company_id=company_id, name=name, category_id=category_id,
                    ruc=row.get('ruc', '').strip() or None,
                    social_reason=row.get('social_reason', '').strip() or None,
                    address=row.get('address', '').strip() or None,
                    email=row.get('email', '').strip() or None,
                    phone=row.get('phone', '').strip() or None
                )

                if result == "created": created += 1
                elif result == "updated": updated += 1

            except Exception as e:
                error_list.append(f"Fila {row_num} (Nombre: {name}): {e}")

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
