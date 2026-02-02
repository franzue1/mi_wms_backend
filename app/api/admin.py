# app/api/admin.py
"""
Endpoints de Administración.
Delega lógica de negocio al AdminService.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Annotated, Dict
from pydantic import BaseModel
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.admin_service import AdminService
from app.exceptions import ValidationError, BusinessRuleError, PermissionDeniedError
import traceback
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]


# --- Helper de Permisos de Administrador ---
def check_admin_permission(auth: AuthDependency):
    if "nav.admin.view" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado para administración"
        )


# ==========================================
# --- Usuarios ---
# ==========================================

@router.get("/users", response_model=List[schemas.UserResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_users():
    """Obtiene todos los usuarios (para administradores)."""
    users_raw = await asyncio.to_thread(db.get_users_for_admin)
    return [AdminService.build_user_response(dict(u)) for u in users_raw]


@router.post("/users", response_model=schemas.UserResponse, status_code=201)
async def create_user(user: schemas.UserCreate, auth: AuthDependency):
    """
    Crea un nuevo usuario.
    Usa AdminService para validación de datos.
    """
    if "admin.can_manage_users" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        # Validar datos usando el servicio
        AdminService.validate_user_data(
            username=user.username,
            full_name=user.full_name,
            role_id=user.role_id,
            company_ids=user.company_ids,
            password=user.password,
            is_new=True
        )

        new_user_id = await asyncio.to_thread(
            db.create_user,
            username=user.username,
            plain_password=user.password,
            full_name=user.full_name,
            role_id=user.role_id,
            company_ids=user.company_ids,
            warehouse_ids=user.warehouse_ids
        )

        # Recargar usuario para devolverlo completo
        user_raw = await asyncio.to_thread(db.get_users_for_admin)
        new_user = next((u for u in user_raw if u['id'] == new_user_id), None)
        return AdminService.build_user_response(dict(new_user))

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@router.put("/users/{user_id}", response_model=schemas.UserResponse)
async def update_user(user_id: int, user: schemas.UserUpdate, auth: AuthDependency):
    """
    Actualiza un usuario existente.
    Usa AdminService para validación.
    """
    if "admin.can_manage_users" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        await asyncio.to_thread(
            db.update_user,
            user_id=user_id,
            full_name=user.full_name,
            role_id=user.role_id,
            is_active=user.is_active,
            new_password=user.password,
            company_ids=user.company_ids,
            warehouse_ids=user.warehouse_ids
        )

        # Recargar usuario
        user_raw = await asyncio.to_thread(db.get_users_for_admin)
        updated_user = next((u for u in user_raw if u['id'] == user_id), None)

        if not updated_user:
            raise HTTPException(
                status_code=404,
                detail="Usuario no encontrado después de actualizar"
            )

        return AdminService.build_user_response(dict(updated_user))

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


# ==========================================
# --- Roles ---
# ==========================================

@router.get("/roles", response_model=List[schemas.ConfigDescResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_roles():
    """Obtiene todos los roles."""
    roles_raw = await asyncio.to_thread(db.get_roles_for_admin)
    return [AdminService.build_role_response(dict(r)) for r in roles_raw]


@router.post("/roles", response_model=schemas.ConfigDescResponse, status_code=201)
async def create_role(role: schemas.ConfigDescCreate, auth: AuthDependency):
    """
    Crea un nuevo rol.
    Usa AdminService para validación.
    """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        # Validar datos usando el servicio
        validated = AdminService.validate_role_data(role.name, role.description)

        new_id = await asyncio.to_thread(
            db.create_role,
            validated["name"],
            validated.get("description")
        )

        return {
            "id": new_id,
            "name": validated["name"],
            "description": validated.get("description", "")
        }

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@router.put("/roles/{role_id}", response_model=schemas.ConfigDescResponse)
async def update_role(role_id: int, role: schemas.ConfigDescCreate, auth: AuthDependency):
    """
    Actualiza un rol existente.
    Usa AdminService para validación.
    """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        # Validar datos usando el servicio
        validated = AdminService.validate_role_data(role.name, role.description)

        await asyncio.to_thread(
            db.update_role,
            role_id,
            validated["name"],
            validated.get("description")
        )

        return {
            "id": role_id,
            "name": validated["name"],
            "description": validated.get("description", "")
        }

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


# ==========================================
# --- Permisos y Matriz ---
# ==========================================

@router.get("/permissions", response_model=List[schemas.PermissionResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_permissions():
    """Obtiene todos los permisos del sistema."""
    perms_raw = await asyncio.to_thread(db.get_permissions_for_admin)
    return [dict(p) for p in perms_raw]


@router.get("/permission-matrix", response_model=Dict[str, Dict[str, bool]])
async def get_permission_matrix(auth: AuthDependency):
    """Obtiene la matriz de permisos por rol."""
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

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
    """
    Agrega o quita un permiso a un rol.
    Usa AdminService para validación.
    """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        # Verificar si el rol puede ser modificado (no es admin)
        if role_id == AdminService.ADMIN_ROLE_ID:
            raise BusinessRuleError(
                "No se pueden modificar los permisos del rol Administrador",
                "ADMIN_CANNOT_MODIFY_ADMIN_ROLE",
                {"role_id": role_id}
            )

        success, msg = await asyncio.to_thread(
            db.update_role_permissions,
            role_id,
            update.permission_id,
            update.has_permission
        )

        if not success:
            raise HTTPException(status_code=500, detail=msg)

        return {"message": msg}

    except BusinessRuleError as bre:
        raise HTTPException(status_code=400, detail=bre.message)


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


@router.get("/companies", response_model=List[CompanyResponse])
async def get_all_companies(auth: AuthDependency):
    """
    Obtiene lista de compañías.
    Usa AdminService para filtrar según rol del usuario.
    """
    try:
        # Obtener datos completos del usuario actual
        user = await asyncio.to_thread(db.get_user_by_username, auth.username)
        if not user:
            raise HTTPException(401, "Usuario no encontrado")

        # Obtener todas las compañías
        all_companies = await asyncio.to_thread(db.get_companies)
        all_companies_list = [dict(c) for c in all_companies]

        # Filtrar según rol usando el servicio
        if user['role_id'] == AdminService.ADMIN_ROLE_ID:
            filtered = all_companies_list
        else:
            user_companies = await asyncio.to_thread(db.get_user_companies, user['id'])
            user_company_ids = [c['id'] for c in user_companies]
            filtered = AdminService.filter_companies_for_user(
                all_companies_list,
                auth.role_name,
                user_company_ids
            )

        return [AdminService.build_company_response(c) for c in filtered]

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error al obtener compañías: {e}")


@router.post("/companies", response_model=CompanyResponse, status_code=201)
async def create_company(company_data: CompanyCreate, auth: AuthDependency):
    """
    Crea una nueva compañía.
    Usa AdminService para validación.
    """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        # Validar datos usando el servicio
        validated = AdminService.validate_company_data(
            company_data.name,
            company_data.country_code
        )

        new_company = await asyncio.to_thread(
            db.create_company,
            validated["name"],
            validated.get("country", "PE"),
            auth.user_id
        )

        return AdminService.build_company_response(dict(new_company))

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@router.put("/companies/{company_id}", response_model=CompanyResponse)
async def update_company(company_id: int, company_data: CompanyUpdate, auth: AuthDependency):
    """
    Actualiza una compañía existente.
    Usa AdminService para validación.
    """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        # Validar datos usando el servicio
        validated = AdminService.validate_company_data(
            company_data.name,
            company_data.country_code
        )

        updated_company = await asyncio.to_thread(
            db.update_company,
            company_id,
            validated["name"],
            validated.get("country", "PE")
        )

        if not updated_company:
            raise HTTPException(status_code=404, detail="Compañía no encontrada.")

        return AdminService.build_company_response(dict(updated_company))

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


@router.delete("/companies/{company_id}", status_code=200)
async def delete_company(company_id: int, auth: AuthDependency):
    """Elimina una compañía si no tiene dependencias."""
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )

    try:
        await asyncio.to_thread(db.delete_company, company_id)
        return {"message": "Compañía eliminada."}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")
