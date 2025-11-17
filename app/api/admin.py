# app/api/admin.py
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Annotated, Dict
from pydantic import BaseModel
from app import database as db
from app import schemas, security
from app.security import TokenData, get_password_hash
import traceback 
import asyncio 

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper de Permisos de Administrador ---
def check_admin_permission(auth: AuthDependency):
    if "nav.admin.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado para administración")

# --- Usuarios ---
@router.get("/users", response_model=List[schemas.UserResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_users():
    users_raw = await asyncio.to_thread(db.get_users_for_admin)
    return [dict(u) for u in users_raw]

@router.post("/users", response_model=schemas.UserResponse, status_code=201)
async def create_user(user: schemas.UserCreate, auth: AuthDependency):
    if "admin.can_manage_users" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_user_id = await asyncio.to_thread(
            db.create_user,
            username=user.username,
            plain_password=user.password,
            full_name=user.full_name,
            role_id=user.role_id
        )
        user_raw = await asyncio.to_thread(db.get_users_for_admin)
        new_user = next((u for u in user_raw if u['id'] == new_user_id), None)
        return dict(new_user)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.put("/users/{user_id}", response_model=schemas.UserResponse)
async def update_user(user_id: int, user: schemas.UserUpdate, auth: AuthDependency):
    if "admin.can_manage_users" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        await asyncio.to_thread(
            db.update_user,
            user_id=user_id,
            full_name=user.full_name,
            role_id=user.role_id,
            is_active=user.is_active,
            new_password=user.password
        )
        user_raw = await asyncio.to_thread(db.get_users_for_admin)
        updated_user = next((u for u in user_raw if u['id'] == user_id), None)
        if not updated_user:
             raise HTTPException(status_code=404, detail="Usuario no encontrado después de actualizar")
        return dict(updated_user)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

# --- Roles ---
@router.get("/roles", response_model=List[schemas.ConfigDescResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_roles():
    roles_raw = await asyncio.to_thread(db.get_roles_for_admin)
    return [dict(r) for r in roles_raw]

@router.post("/roles", response_model=schemas.ConfigDescResponse, status_code=201)
async def create_role(role: schemas.ConfigDescCreate, auth: AuthDependency):
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        new_id = await asyncio.to_thread(db.create_role, role.name, role.description)
        return {"id": new_id, "name": role.name, "description": role.description}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.put("/roles/{role_id}", response_model=schemas.ConfigDescResponse)
async def update_role(role_id: int, role: schemas.ConfigDescCreate, auth: AuthDependency):
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        await asyncio.to_thread(db.update_role, role_id, role.name, role.description)
        return {"id": role_id, "name": role.name, "description": role.description}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

# --- Permisos y Matriz ---
@router.get("/permissions", response_model=List[schemas.PermissionResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_permissions():
    perms_raw = await asyncio.to_thread(db.get_permissions_for_admin)
    return [dict(p) for p in perms_raw]

@router.get("/permission-matrix", response_model=Dict[str, Dict[str, bool]])
async def get_permission_matrix(auth: AuthDependency):
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    roles, permissions, matrix = await asyncio.to_thread(db.get_permission_matrix)
    
    response_matrix = {}
    for role_id, role_name in roles.items():
        response_matrix[role_name] = {}
        for perm_id, perm_key in permissions.items():
            has_perm = (role_id, perm_id) in matrix
            response_matrix[role_name][perm_key] = has_perm
            
    return response_matrix

@router.put("/roles/{role_id}/permissions", status_code=200)
async def update_role_permission(
    role_id: int, 
    update: schemas.PermissionMatrixUpdate,
    auth: AuthDependency
):
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    success, msg = await asyncio.to_thread(
        db.update_role_permissions, 
        role_id, 
        update.permission_id, 
        update.has_permission
    )
    if not success:
        raise HTTPException(status_code=500, detail=msg)
    return {"message": msg}

# ==========================================
# --- Compañías (CRUD Completo) ---
# ==========================================

# --- Modelos Pydantic Locales ---
class CompanyBase(BaseModel):
    name: str
    country_code: str = "PE"

class CompanyCreate(CompanyBase):
    pass

class CompanyUpdate(CompanyBase):
    pass

class CompanyResponse(CompanyBase):
    id: int

# --- Endpoints ---

@router.get("/companies", response_model=List[CompanyResponse])
async def get_all_companies(auth: AuthDependency):
    """Obtiene una lista de todas las compañías."""
    try:
        companies_raw = await asyncio.to_thread(db.get_companies) 
        return [dict(c) for c in companies_raw]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener compañías: {e}")

@router.post("/companies", response_model=CompanyResponse, status_code=201)
async def create_company(company_data: CompanyCreate, auth: AuthDependency):
    """Crea una nueva compañía."""
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_company = await asyncio.to_thread(
            db.create_company, company_data.name, company_data.country_code
        )
        return dict(new_company)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.put("/companies/{company_id}", response_model=CompanyResponse)
async def update_company(company_id: int, company_data: CompanyUpdate, auth: AuthDependency):
    """Actualiza nombre y país."""
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    try:
        updated_company = await asyncio.to_thread(
            db.update_company, company_id, company_data.name, company_data.country_code
        )
        if not updated_company:
            raise HTTPException(status_code=404, detail="Compañía no encontrada.")
        return dict(updated_company)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.delete("/companies/{company_id}", status_code=200)
async def delete_company(company_id: int, auth: AuthDependency):
    # (Este endpoint no cambia, es igual que antes)
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    try:
        await asyncio.to_thread(db.delete_company, company_id)
        return {"message": "Compañía eliminada."}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


