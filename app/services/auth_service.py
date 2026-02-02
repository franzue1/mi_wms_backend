# app/services/auth_service.py
"""
Service Layer para Autenticación y Seguridad.
Centraliza el hashing de contraseñas, validación de credenciales y generación de tokens JWT.
"""

from typing import Optional, Dict, List, Any, Tuple, Set
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
import hashlib
import jwt
import os
import re

from app.exceptions import (
    ValidationError,
    NotFoundError,
    BusinessRuleError,
    PermissionDeniedError,
    ErrorCodes
)


class AuthService:
    """
    Servicio de autenticación y gestión de credenciales.
    Maneja hashing de contraseñas, validación y tokens JWT.
    """

    # ==========================================================================
    # CONSTANTES - CONFIGURACIÓN
    # ==========================================================================

    # Algoritmo JWT
    ALGORITHM = "HS256"

    # Tiempo de expiración del token (minutos)
    ACCESS_TOKEN_EXPIRE_MINUTES = 480  # 8 horas

    # Longitud mínima de contraseña
    MIN_PASSWORD_LENGTH = 6

    # Longitud máxima de username
    MAX_USERNAME_LENGTH = 50

    # Context para BCrypt (moderno y seguro)
    _pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    # ==========================================================================
    # HASHING DE CONTRASEÑAS
    # ==========================================================================

    @staticmethod
    def hash_password_bcrypt(password: str) -> str:
        """
        Genera un hash seguro de contraseña usando BCrypt.

        Args:
            password: Contraseña en texto plano

        Returns:
            str: Hash BCrypt de la contraseña
        """
        return AuthService._pwd_context.hash(password)

    @staticmethod
    def hash_password_sha256(password: str) -> str:
        """
        Genera un hash SHA-256 de contraseña (legacy, para compatibilidad).

        Args:
            password: Contraseña en texto plano

        Returns:
            str: Hash SHA-256 de la contraseña

        Note:
            Solo usar para verificar contraseñas existentes.
            Nuevas contraseñas deben usar BCrypt.
        """
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """
        Verifica una contraseña contra su hash.
        Soporta tanto BCrypt (nuevo) como SHA-256 (legacy).

        Args:
            plain_password: Contraseña en texto plano
            hashed_password: Hash almacenado

        Returns:
            bool: True si la contraseña es correcta
        """
        # Primero intentar SHA-256 (legacy)
        if AuthService.hash_password_sha256(plain_password) == hashed_password:
            return True

        # Luego intentar BCrypt (moderno)
        try:
            return AuthService._pwd_context.verify(plain_password, hashed_password)
        except Exception:
            return False

    @staticmethod
    def needs_rehash(hashed_password: str) -> bool:
        """
        Verifica si una contraseña necesita ser re-hasheada (migración a BCrypt).

        Args:
            hashed_password: Hash almacenado

        Returns:
            bool: True si debe migrarse a BCrypt
        """
        # Los hashes SHA-256 tienen exactamente 64 caracteres hex
        if len(hashed_password) == 64 and all(c in '0123456789abcdef' for c in hashed_password):
            return True
        return False

    # ==========================================================================
    # VALIDACIÓN DE CONTRASEÑAS
    # ==========================================================================

    @staticmethod
    def validate_password_strength(password: str) -> None:
        """
        Valida que la contraseña cumpla los requisitos mínimos.

        Args:
            password: Contraseña a validar

        Raises:
            ValidationError: Si la contraseña no cumple los requisitos
        """
        if not password:
            raise ValidationError(
                "La contraseña es requerida",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "password"}
            )

        if len(password) < AuthService.MIN_PASSWORD_LENGTH:
            raise ValidationError(
                f"La contraseña debe tener al menos {AuthService.MIN_PASSWORD_LENGTH} caracteres",
                "AUTH_PASSWORD_TOO_SHORT",
                {"min_length": AuthService.MIN_PASSWORD_LENGTH}
            )

    @staticmethod
    def validate_password_change(
        old_password: str,
        new_password: str,
        stored_hash: str
    ) -> None:
        """
        Valida un cambio de contraseña.

        Args:
            old_password: Contraseña actual
            new_password: Nueva contraseña
            stored_hash: Hash almacenado de la contraseña actual

        Raises:
            ValidationError: Si la validación falla
        """
        # Verificar contraseña actual
        if not AuthService.verify_password(old_password, stored_hash):
            raise ValidationError(
                "La contraseña actual es incorrecta",
                ErrorCodes.INVALID_CREDENTIALS,
                {"field": "old_password"}
            )

        # Validar fortaleza de la nueva
        AuthService.validate_password_strength(new_password)

        # No permitir que sea igual a la anterior
        if old_password == new_password:
            raise ValidationError(
                "La nueva contraseña debe ser diferente a la actual",
                "AUTH_PASSWORD_SAME_AS_OLD"
            )

    # ==========================================================================
    # VALIDACIÓN DE USUARIOS
    # ==========================================================================

    @staticmethod
    def validate_username(username: str) -> str:
        """
        Valida y normaliza el nombre de usuario.

        Args:
            username: Nombre de usuario a validar

        Returns:
            str: Nombre de usuario normalizado

        Raises:
            ValidationError: Si el username es inválido
        """
        if not username:
            raise ValidationError(
                "El nombre de usuario es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "username"}
            )

        username = username.strip().lower()

        if len(username) > AuthService.MAX_USERNAME_LENGTH:
            raise ValidationError(
                f"El nombre de usuario no puede exceder {AuthService.MAX_USERNAME_LENGTH} caracteres",
                "AUTH_USERNAME_TOO_LONG",
                {"max_length": AuthService.MAX_USERNAME_LENGTH}
            )

        # Validar caracteres permitidos (alfanumérico + guiones + puntos)
        if not re.match(r'^[a-z0-9._-]+$', username):
            raise ValidationError(
                "El nombre de usuario solo puede contener letras, números, puntos, guiones y guiones bajos",
                "AUTH_USERNAME_INVALID_CHARS",
                {"username": username}
            )

        return username

    @staticmethod
    def validate_user_is_active(user_data: Dict[str, Any]) -> None:
        """
        Verifica que el usuario esté activo.

        Args:
            user_data: Datos del usuario

        Raises:
            PermissionDeniedError: Si el usuario está inactivo
        """
        if not user_data.get('is_active', False):
            raise PermissionDeniedError(
                "Usuario desactivado. Contacte al administrador.",
                "AUTH_USER_INACTIVE",
                {"username": user_data.get('username')}
            )

    # ==========================================================================
    # GENERACIÓN DE TOKENS JWT
    # ==========================================================================

    @staticmethod
    def create_access_token(
        data: Dict[str, Any],
        secret_key: str,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Crea un token JWT.

        Args:
            data: Datos a incluir en el payload
            secret_key: Clave secreta para firmar
            expires_delta: Tiempo de expiración opcional

        Returns:
            str: Token JWT codificado
        """
        to_encode = data.copy()

        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(
                minutes=AuthService.ACCESS_TOKEN_EXPIRE_MINUTES
            )

        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(
            to_encode,
            secret_key,
            algorithm=AuthService.ALGORITHM
        )

        return encoded_jwt

    @staticmethod
    def decode_token(
        token: str,
        secret_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Decodifica y valida un token JWT.

        Args:
            token: Token JWT a decodificar
            secret_key: Clave secreta para verificar

        Returns:
            Optional[Dict]: Payload del token o None si es inválido
        """
        try:
            payload = jwt.decode(
                token,
                secret_key,
                algorithms=[AuthService.ALGORITHM]
            )
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.JWTError:
            return None

    @staticmethod
    def build_token_payload(
        username: str,
        user_id: int,
        full_name: str,
        permissions: Set[str],
        role_name: str,
        company_ids: List[int]
    ) -> Dict[str, Any]:
        """
        Construye el payload para el token JWT.

        Args:
            username: Nombre de usuario
            user_id: ID del usuario
            full_name: Nombre completo
            permissions: Set de permisos
            role_name: Nombre del rol
            company_ids: Lista de IDs de empresas

        Returns:
            Dict[str, Any]: Payload listo para codificar
        """
        return {
            "sub": username,
            "user_id": user_id,
            "full_name": full_name,
            "permissions": list(permissions),
            "role": role_name,
            "companies": company_ids
        }

    # ==========================================================================
    # VALIDACIÓN DE PERMISOS
    # ==========================================================================

    @staticmethod
    def check_permission(
        permissions: List[str],
        required_permission: str
    ) -> bool:
        """
        Verifica si un usuario tiene un permiso específico.

        Args:
            permissions: Lista de permisos del usuario
            required_permission: Permiso requerido

        Returns:
            bool: True si tiene el permiso
        """
        return required_permission in permissions

    @staticmethod
    def require_permission(
        permissions: List[str],
        required_permission: str
    ) -> None:
        """
        Exige que el usuario tenga un permiso específico.

        Args:
            permissions: Lista de permisos del usuario
            required_permission: Permiso requerido

        Raises:
            PermissionDeniedError: Si no tiene el permiso
        """
        if not AuthService.check_permission(permissions, required_permission):
            raise PermissionDeniedError(
                "No autorizado para realizar esta acción",
                ErrorCodes.PERMISSION_DENIED,
                {"required_permission": required_permission}
            )

    @staticmethod
    def verify_company_access(
        role_name: str,
        user_company_ids: List[int],
        requested_company_id: int
    ) -> None:
        """
        Verifica acceso a una empresa específica.

        Args:
            role_name: Nombre del rol del usuario
            user_company_ids: Empresas asignadas al usuario
            requested_company_id: Empresa a la que intenta acceder

        Raises:
            PermissionDeniedError: Si no tiene acceso a la empresa
        """
        # Administradores tienen acceso a todo
        if role_name == "Administrador":
            return

        if requested_company_id not in user_company_ids:
            raise PermissionDeniedError(
                "ACCESO DENEGADO: No tienes permisos para esta empresa.",
                "AUTH_COMPANY_ACCESS_DENIED",
                {"company_id": requested_company_id}
            )

    # ==========================================================================
    # PREPARACIÓN DE RESPUESTAS
    # ==========================================================================

    @staticmethod
    def build_login_response(
        access_token: str,
        must_change_password: bool
    ) -> Dict[str, Any]:
        """
        Construye la respuesta de login.

        Args:
            access_token: Token JWT generado
            must_change_password: Si debe cambiar contraseña

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "must_change_password": must_change_password
        }
