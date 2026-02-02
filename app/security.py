# app/security.py
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from passlib.context import CryptContext
from jose import JWTError, jwt
from pydantic import BaseModel
from app import database as db
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

# --- Configuración de Hashing de Contraseña ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- Configuración de Token JWT ---
SECRET_KEY = os.environ.get("SECRET_KEY", "tu_super_secreto_por_defecto_cambia_esto")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 8 # 8 horas

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

# --- MODELO DE DATOS DEL TOKEN (CORREGIDO) ---
class TokenData(BaseModel):
    username: Optional[str] = None
    user_id: Optional[int] = None
    permissions: Optional[list] = []
    # --- NUEVOS CAMPOS NECESARIOS PARA VALIDACIÓN IDOR ---
    role_name: Optional[str] = None
    company_ids: List[int] = []

# --- Funciones de Contraseña ---

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica una contraseña plana contra un hash."""
    if db.check_password(hashed_password, plain_password):
        return True
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False

def get_password_hash(password: str) -> str:
    """Crea un nuevo hash seguro (bcrypt)."""
    return pwd_context.hash(password)

# --- Funciones de Token JWT ---

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Crea un nuevo token JWT."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user_data(token: str = Depends(oauth2_scheme)) -> TokenData:
    """
    Valida el token y devuelve los datos del usuario (incluyendo rol y compañías).
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        username: str = payload.get("sub")
        user_id = payload.get("user_id")
        permissions: list = payload.get("permissions", [])
        
        # --- EXTRACCIÓN DE NUEVOS DATOS ---
        role_name: str = payload.get("role")        # Rol del usuario
        company_ids: list = payload.get("companies", []) # Lista de IDs de empresa permitidos

        if username is None:
            raise credentials_exception
            
        token_data = TokenData(
            username=username, 
            user_id=user_id, 
            permissions=permissions,
            role_name=role_name,      # <--- Asignamos
            company_ids=company_ids   # <--- Asignamos
        )
    except JWTError:
        raise credentials_exception
    
    return token_data

def verify_company_access(auth: TokenData, company_id: int):
    """
    Verifica estrictamente si el usuario tiene permiso para acceder a la compañía solicitada.
    Lanza HTTP 403 Forbidden si no tiene permiso.
    """
    # [DEBUG] Log para diagnóstico de problemas multi-compañía
    print(f"[SECURITY] verify_company_access: User='{auth.username}', Rol='{auth.role_name}', "
          f"Requested Company={company_id}, Token Companies={auth.company_ids}")

    # 1. El Super Admin (Rol 'Administrador') tiene pase maestro.
    if auth.role_name == "Administrador":
        print(f"[SECURITY] ADMIN PASS: Usuario '{auth.username}' es Administrador - acceso permitido")
        return

    # 2. Verificar si el ID de la empresa está en la lista permitida del token.
    if not auth.company_ids:
        print(f"[SECURITY WARNING] Usuario '{auth.username}' tiene lista de compañías VACÍA en el token!")

    if company_id not in auth.company_ids:
        print(f"[SECURITY BLOCK] Usuario '{auth.username}' (Rol: {auth.role_name}) "
              f"intentó acceder a Company ID {company_id}. "
              f"Compañías permitidas en token: {auth.company_ids}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"ACCESO DENEGADO: No tienes autorización para la compañía {company_id}. "
                   f"Compañías permitidas: {auth.company_ids}"
        )

    print(f"[SECURITY OK] Usuario '{auth.username}' tiene acceso a Company ID {company_id}")
