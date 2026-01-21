from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated
from app import database as db
from app import security
from app.security import TokenData 
from app.schemas import TokenResponse, PasswordChangeRequest # Importar los nuevos schemas

router = APIRouter()

@router.post("/token", response_model=TokenResponse)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    """
    Endpoint de login. Genera el token JWT e informa si se requiere cambio de contraseña.
    """
    # 1. Validar credenciales
    user_data, permissions_set = db.validate_user_and_get_permissions(
        form_data.username, form_data.password
    )
    
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 2. Obtener compañías
    allowed_companies = db.get_user_companies(user_data['id'])
    company_ids = [c['id'] for c in allowed_companies]

    # 3. Preparar datos del token
    token_data_to_encode = {
        "sub": user_data['username'],
        "user_id": user_data['id'],
        "full_name": user_data['full_name'],
        "permissions": list(permissions_set),
        "role": user_data.get('role_name'),
        "companies": company_ids
    }
    
    # 4. Generar Token
    access_token = security.create_access_token(data=token_data_to_encode)

    # 5. Devolver respuesta con el flag 'must_change_password'
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "must_change_password": user_data.get('must_change_password', False)
    }

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
    """
    
    # 1. Validar la contraseña actual (Seguridad extra indispensable)
    user_data, _ = db.validate_user_and_get_permissions(current_user.username, payload.old_password)
    if not user_data:
         raise HTTPException(status_code=400, detail="La contraseña actual es incorrecta.")
         
    # 2. Cambiar la contraseña en BD
    try:
        db.change_own_password(current_user.user_id, payload.new_password)
        return {"message": "Contraseña actualizada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al cambiar contraseña: {str(e)}")