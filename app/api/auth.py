# app/api/auth.py
"""
Endpoints de Autenticación.
Delega lógica de negocio al AuthService.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
from app import database as db
from app import security
from app.security import TokenData
from app.schemas import TokenResponse, PasswordChangeRequest
from app.services.auth_service import AuthService
from app.exceptions import ValidationError, PermissionDeniedError

router = APIRouter()

AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]


@router.post("/token", response_model=TokenResponse)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    """
    Endpoint de login. Genera el token JWT e informa si se requiere cambio de contraseña.
    Usa AuthService para validación y generación de tokens.
    """
    # 1. Validar credenciales (el repositorio verifica usuario/contraseña)
    user_data, permissions_set = db.validate_user_and_get_permissions(
        form_data.username, form_data.password
    )

    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 2. Verificar que el usuario esté activo (usando el servicio)
    try:
        AuthService.validate_user_is_active(user_data)
    except PermissionDeniedError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=e.message
        )

    # 3. Obtener compañías del usuario
    allowed_companies = db.get_user_companies(user_data['id'])
    company_ids = [c['id'] for c in allowed_companies]

    # 4. Construir payload del token (usando el servicio)
    token_payload = AuthService.build_token_payload(
        username=user_data['username'],
        user_id=user_data['id'],
        full_name=user_data['full_name'],
        permissions=permissions_set,
        role_name=user_data.get('role_name'),
        company_ids=company_ids
    )

    # 5. Generar Token
    access_token = security.create_access_token(data=token_payload)

    # 6. Construir y devolver respuesta (usando el servicio)
    return AuthService.build_login_response(
        access_token=access_token,
        must_change_password=user_data.get('must_change_password', False)
    )


@router.get("/me")
async def read_users_me(
    current_user_data: Annotated[TokenData, Depends(security.get_current_user_data)]
):
    """
    Endpoint protegido que devuelve la información del usuario actual.
    """
    return current_user_data


@router.post("/change-password")
async def change_password(
    payload: PasswordChangeRequest,
    current_user: TokenData = Depends(security.get_current_user_data)
):
    """
    Endpoint para que el usuario logueado cambie su propia contraseña.
    Usa AuthService para validación de contraseñas.
    """
    # 1. Validar que la nueva contraseña cumple los requisitos
    try:
        AuthService.validate_password_strength(payload.new_password)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=e.message)

    # 2. Validar la contraseña actual
    user_data, _ = db.validate_user_and_get_permissions(
        current_user.username,
        payload.old_password
    )
    if not user_data:
        raise HTTPException(
            status_code=400,
            detail="La contraseña actual es incorrecta."
        )

    # 3. Validar que no sea igual a la anterior
    if payload.old_password == payload.new_password:
        raise HTTPException(
            status_code=400,
            detail="La nueva contraseña debe ser diferente a la actual."
        )

    # 4. Cambiar la contraseña en BD
    try:
        db.change_own_password(current_user.user_id, payload.new_password)
        return {"message": "Contraseña actualizada correctamente"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al cambiar contraseña: {str(e)}"
        )


@router.post("/switch-company", response_model=TokenResponse)
async def switch_company(
    auth: AuthDependency,
    new_company_id: int = Body(..., embed=True)
):
    """
    Endpoint para cambiar la compañía activa y obtener un nuevo token.

    Este endpoint resuelve el problema de multi-compañía donde el token
    original puede no reflejar correctamente los permisos actualizados.

    Args:
        new_company_id: ID de la nueva compañía a activar

    Returns:
        TokenResponse: Nuevo token JWT con la compañía activa actualizada
    """
    print(f"[AUTH] Switch-company solicitado por '{auth.username}' -> Company ID: {new_company_id}")

    # 1. Verificar que el usuario tenga acceso a la compañía solicitada
    user_companies = db.get_user_companies(auth.user_id)
    allowed_ids = [c['id'] for c in user_companies]

    print(f"[AUTH] Compañías permitidas para '{auth.username}': {allowed_ids}")

    # Admin tiene pase maestro
    if auth.role_name != "Administrador" and new_company_id not in allowed_ids:
        print(f"[AUTH] BLOQUEADO: Usuario '{auth.username}' no tiene acceso a Company ID {new_company_id}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"No tienes acceso a la compañía con ID {new_company_id}"
        )

    # 2. Obtener datos actualizados del usuario (usando username del token actual)
    user_data = db.get_user_by_username(auth.username)
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )

    # 3. Obtener rol y permisos actualizados
    user_full = db.get_user_for_auth(auth.username)
    if not user_full:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    permissions_set = db.get_permissions_by_role_id(user_full['role_id'])

    # 4. Construir nuevo payload con todas las compañías (no solo la activa)
    # Esto asegura que el token siempre tenga la lista completa actualizada
    token_payload = AuthService.build_token_payload(
        username=user_full['username'],
        user_id=user_full['id'],
        full_name=user_full.get('full_name', user_full['username']),
        permissions=permissions_set,
        role_name=user_full.get('role_name'),
        company_ids=allowed_ids  # Lista actualizada de compañías
    )

    # 5. Generar nuevo token
    new_token = security.create_access_token(data=token_payload)

    print(f"[AUTH] Nuevo token generado para '{auth.username}' con {len(allowed_ids)} compañías")

    return AuthService.build_login_response(
        access_token=new_token,
        must_change_password=False
    )
