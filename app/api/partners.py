# app/api/partners.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.partner_service import PartnerService
from app.exceptions import ValidationError, DuplicateError, NotFoundError
import traceback
from fastapi.responses import StreamingResponse

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.PartnerResponse])
async def get_all_partners(
    auth: AuthDependency,
    company_id: int = Query(...),
    skip: int = 0,
    limit: int = 100,
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    social_reason: Optional[str] = Query(None),
    address: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None)
):
    """Obtiene una lista de socios (proveedores/clientes)."""
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar PartnerService para construir filtros
    clean_filters = PartnerService.build_filter_dict(
        name=name,
        ruc=ruc,
        social_reason=social_reason,
        address=address,
        category_name=category_name
    )

    partners_raw = db.get_partners_filtered_sorted(
        company_id,
        filters=clean_filters,
        sort_by=sort_by,
        ascending=ascending,
        limit=limit,
        offset=skip
    )
    return [dict(p) for p in partners_raw]


@router.get("/count", response_model=int)
async def get_partners_count(
    auth: AuthDependency,
    company_id: int = Query(...),
    name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    social_reason: Optional[str] = Query(None),
    address: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None)
):
    """Obtiene el conteo total de socios para la paginación."""
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    clean_filters = PartnerService.build_filter_dict(
        name=name,
        ruc=ruc,
        social_reason=social_reason,
        address=address,
        category_name=category_name
    )

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
    """Crea un nuevo socio."""
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # Usar PartnerService para preparar datos normalizados
    prepared_data = PartnerService.prepare_partner_data(partner.dict())

    try:
        new_partner_id = db.create_partner(
            name=prepared_data["name"],
            category_id=prepared_data["category_id"],
            company_id=company_id,
            social_reason=prepared_data["social_reason"],
            ruc=prepared_data["ruc"],
            email=prepared_data["email"],
            phone=prepared_data["phone"],
            address=prepared_data["address"]
        )
        created_partner = db.get_partner_details_by_id(new_partner_id)
        return dict(created_partner)
    except Exception as e:
        if "partners_company_id_name_key" in str(e):
            raise DuplicateError(
                f"Ya existe un proveedor/cliente con el nombre '{prepared_data['name']}'",
                "PARTNER_DUPLICATE_NAME"
            )
        raise

@router.put("/{partner_id}", response_model=schemas.PartnerResponse)
async def update_partner(
    partner_id: int,
    partner: schemas.PartnerUpdate,
    auth: AuthDependency
):
    """Actualiza un socio existente."""
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    current_data = db.get_partner_details_by_id(partner_id)
    if not current_data:
        raise NotFoundError("Socio no encontrado", "PARTNER_NOT_FOUND")

    # Merge datos actuales con los nuevos
    update_data = dict(current_data)
    update_data.update(partner.dict(exclude_unset=True))

    # Usar PartnerService para normalizar
    prepared_data = PartnerService.prepare_partner_data(update_data)

    try:
        db.update_partner(
            partner_id=partner_id,
            name=prepared_data["name"],
            category_id=prepared_data["category_id"],
            social_reason=prepared_data["social_reason"],
            ruc=prepared_data["ruc"],
            email=prepared_data["email"],
            phone=prepared_data["phone"],
            address=prepared_data["address"]
        )

        updated_partner = db.get_partner_details_by_id(partner_id)
        return dict(updated_partner)
    except Exception as e:
        if "partners_company_id_name_key" in str(e):
            raise DuplicateError(
                f"Ya existe otro proveedor/cliente con el nombre '{prepared_data['name']}'",
                "PARTNER_DUPLICATE_NAME"
            )
        raise

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
    sort_by: Optional[str] = Query(None),
    ascending: bool = Query(True),
    name: Optional[str] = Query(None),
    ruc: Optional[str] = Query(None),
    social_reason: Optional[str] = Query(None),
    address: Optional[str] = Query(None),
    category_name: Optional[str] = Query(None)
):
    """Genera y transmite un archivo CSV de los socios filtrados."""
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    clean_filters = PartnerService.build_filter_dict(
        name=name,
        ruc=ruc,
        social_reason=social_reason,
        address=address,
        category_name=category_name
    )

    partners_raw = db.get_partners_filtered_sorted(
        company_id,
        filters=clean_filters,
        sort_by=sort_by or 'id',
        ascending=ascending,
        limit=None,
        offset=None
    )

    if not partners_raw:
        raise NotFoundError("No hay datos para exportar", "EXPORT_NO_DATA")

    # Usar PartnerService para generar CSV
    csv_content = PartnerService.generate_csv_content(partners_raw)

    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=socios.csv"}
    )

@router.post("/import/csv", response_model=dict)
async def import_partners_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """Importa socios (partners) desde un archivo CSV."""
    if "partners.can_crud" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    # 1. Leer y parsear CSV usando PartnerService
    content = await file.read()
    rows, headers = PartnerService.parse_csv_file(content)

    # 2. Validar headers requeridos
    PartnerService.validate_csv_headers(headers)

    # 3. Cargar categorías de BD y validar referencias
    db_categories = db.get_partner_categories(company_id)
    valid_category_names = {cat['name'] for cat in db_categories}
    PartnerService.validate_csv_categories(rows, valid_category_names)

    # 4. Crear mapeo de categorías
    cat_map = {cat['name']: cat['id'] for cat in db_categories}

    # 5. Procesar filas
    created, updated = 0, 0
    error_list = []

    for i, row in enumerate(rows):
        row_num = i + 2
        try:
            # Usar PartnerService para procesar y normalizar la fila
            prepared = PartnerService.process_csv_row(row, row_num, cat_map)

            result = db.upsert_partner_from_import(
                company_id=company_id,
                name=prepared["name"],
                category_id=prepared["category_id"],
                ruc=prepared["ruc"],
                social_reason=prepared["social_reason"],
                address=prepared["address"],
                email=prepared["email"],
                phone=prepared["phone"]
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
