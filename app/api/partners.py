# app/api/partners.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

@router.get("/", response_model=List[schemas.PartnerResponse])
async def get_all_partners(
    auth: AuthDependency,
    company_id: int = 1,
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
    company_id: int = 1,
    
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
    company_id: int = 1
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