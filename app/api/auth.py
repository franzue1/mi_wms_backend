# app/api/auth.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
from app import database as db
from app import security
from app.security import TokenData # Importamos la clase

router = APIRouter()

@router.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    """
    Endpoint de login. Recibe 'username' y 'password' de un formulario.
    """
    # 1. Validar al usuario contra la BD (usando tu función existente)
    # Nota: Tu función valida con SHA256. La nuestra lo soporta.
    user_data, permissions_set = db.validate_user_and_get_permissions(
        form_data.username, form_data.password
    )
    
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # --- ¡CAMBIO! Obtener compañías permitidas ---
    allowed_companies = db.get_user_companies(user_data['id'])
    # Convertir a lista de dicts simple para el token (o endpoint 'me')
    companies_list = [dict(c) for c in allowed_companies]

    # 2. Crear el token JWT con los datos del usuario
    token_data_to_encode = {
        "sub": user_data['username'],
        "user_id": user_data['id'],
        "full_name": user_data['full_name'],
        "permissions": list(permissions_set), # Convertir el set a lista
        "allowed_companies": companies_list
    }
    
    access_token = security.create_access_token(data=token_data_to_encode)

    # 3. Devolver el token
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me")
async def read_users_me(
    current_user_data: Annotated[TokenData, Depends(security.get_current_user_data)]
):
    """
    Endpoint protegido que devuelve la información del usuario basada en el token.
    """
    # Los datos ya vienen validados desde get_current_user_data
    return current_user_data