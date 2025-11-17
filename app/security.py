# app/security.py
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from passlib.context import CryptContext
from jose import JWTError, jwt
from pydantic import BaseModel
from app import database as db # Importamos app.database.py
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

# --- Configuración de Hashing de Contraseña ---
# Usamos passlib para hashear contraseñas de forma segura
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- Configuración de Token JWT ---
# Idealmente, esto debe estar en variables de entorno
SECRET_KEY = os.environ.get("SECRET_KEY", "tu_super_secreto_por_defecto_cambia_esto")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 8 # 8 horas

# Esquema de OAuth2 para que FastAPI sepa cómo recibir el token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

# Modelo Pydantic para los datos del token
class TokenData(BaseModel):
    username: Optional[str] = None
    user_id: Optional[int] = None # <-- Añadir
    permissions: Optional[list] = []

# --- Funciones de Contraseña ---

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica una contraseña plana contra un hash."""
    # Primero, verificamos contra el hash que ya tienes (sha256)
    if db.check_password(hashed_password, plain_password):
        return True
    # Luego, verificamos contra el nuevo hash (bcrypt)
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False

def get_password_hash(password: str) -> str:
    """Crea un nuevo hash seguro (bcrypt) para contraseñas nuevas."""
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
    Dependencia de FastAPI: Valida el token y devuelve los datos del usuario.
    Esto protegerá nuestros endpoints.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id = payload.get("user_id") # <-- Leer del token
        permissions: list = payload.get("permissions", [])
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username, user_id=user_id, permissions=permissions)
    except JWTError:
        raise credentials_exception
    return token_data