# app/api/admin.py
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Annotated, Dict
from app import database as db
from app import schemas, security
from app.security import TokenData, get_password_hash

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Helper de Permisos de Administrador ---
def check_admin_permission(auth: AuthDependency):
    if "nav.admin.view" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado para administración")

# --- Usuarios ---

@router.get("/users", response_model=List[schemas.UserResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_users():
    users_raw = db.get_users_for_admin()
    return [dict(u) for u in users_raw]

@router.post("/users", response_model=schemas.UserResponse, status_code=201)
async def create_user(user: schemas.UserCreate, auth: AuthDependency):
    if "admin.can_manage_users" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    try:
        new_user_id = db.create_user(
            username=user.username,
            plain_password=user.password,
            full_name=user.full_name,
            role_id=user.role_id
        )
        user_raw = db.get_users_for_admin() # Re-consultamos para obtener el role_name
        new_user = next((u for u in user_raw if u['id'] == new_user_id), None)
        return dict(new_user)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/users/{user_id}", response_model=schemas.UserResponse)
async def update_user(user_id: int, user: schemas.UserUpdate, auth: AuthDependency):
    if "admin.can_manage_users" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    try:
        db.update_user(
            user_id=user_id,
            full_name=user.full_name,
            role_id=user.role_id,
            is_active=user.is_active,
            new_password=user.password # Pasa la nueva contraseña si se proveyó
        )
        user_raw = db.get_users_for_admin() # Re-consultamos
        updated_user = next((u for u in user_raw if u['id'] == user_id), None)
        if not updated_user:
             raise HTTPException(status_code=404, detail="Usuario no encontrado después de actualizar")
        return dict(updated_user)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

# --- Roles ---

@router.get("/roles", response_model=List[schemas.ConfigDescResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_roles():
    roles_raw = db.get_roles_for_admin()
    return [dict(r) for r in roles_raw]

@router.post("/roles", response_model=schemas.ConfigDescResponse, status_code=201)
async def create_role(role: schemas.ConfigDescCreate, auth: AuthDependency):
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        new_id = db.create_role(role.name, role.description)
        return {"id": new_id, "name": role.name, "description": role.description}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

@router.put("/roles/{role_id}", response_model=schemas.ConfigDescResponse)
async def update_role(role_id: int, role: schemas.ConfigDescCreate, auth: AuthDependency):
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    try:
        db.update_role(role_id, role.name, role.description)
        return {"id": role_id, "name": role.name, "description": role.description}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

# --- Permisos y Matriz ---

@router.get("/permissions", response_model=List[schemas.PermissionResponse], dependencies=[Depends(check_admin_permission)])
async def get_all_permissions():
    perms_raw = db.get_permissions_for_admin()
    return [dict(p) for p in perms_raw]

@router.get("/permission-matrix", response_model=Dict[str, Dict[str, bool]])
async def get_permission_matrix(auth: AuthDependency):
    """
    Obtiene la matriz de permisos completa, formateada para la UI.
    Formato: { "Rol Admin": { "perm_key_1": true, "perm_key_2": false }, ... }
    """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
        
    roles, permissions, matrix = db.get_permission_matrix()
    
    # Formatear la respuesta para que sea fácil de usar en Flet
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
    """ Actualiza un permiso específico para un rol. """
    if "admin.can_manage_roles" not in auth.permissions:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")
    
    success, msg = db.update_role_permissions(role_id, update.permission_id, update.has_permission)
    if not success:
        raise HTTPException(status_code=500, detail=msg)
    return {"message": msg}