# app/api/auth.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
from app import database as db
from app import security
from app.security import TokenData 

router = APIRouter()

@router.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    """
    Endpoint de login. Genera el token JWT con Roles y Compañías.
    """
    # 1. Validar al usuario contra la BD
    user_data, permissions_set = db.validate_user_and_get_permissions(
        form_data.username, form_data.password
    )
    
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 2. Obtener compañías permitidas
    allowed_companies = db.get_user_companies(user_data['id'])
    
    # --- CORRECCIÓN CRÍTICA ---
    # Extraemos SOLO los IDs (List[int]) para que security.py pueda validarlos rápido.
    company_ids = [c['id'] for c in allowed_companies]

    # 3. Crear el token JWT con los datos EXACTOS que espera security.py
    token_data_to_encode = {
        "sub": user_data['username'],
        "user_id": user_data['id'],
        "full_name": user_data['full_name'],
        "permissions": list(permissions_set),
        
        # --- Claves que faltaban o estaban mal nombradas ---
        "role": user_data.get('role_name'), # security.py busca "role"
        "companies": company_ids            # security.py busca "companies" (List[int])
    }
    
    # 4. Generar y devolver el token
    access_token = security.create_access_token(data=token_data_to_encode)

    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me")
async def read_users_me(
    current_user_data: Annotated[TokenData, Depends(security.get_current_user_data)]
):
    """
    Endpoint protegido que devuelve la información del usuario.
    Sirve para verificar que el token se está decodificando bien.
    """
    return current_user_data