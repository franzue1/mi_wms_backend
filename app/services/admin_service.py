# app/services/admin_service.py
"""
Service Layer para Administración.
Maneja la gestión CRUD de Usuarios, Roles, Permisos y Compañías.
"""

from typing import Optional, Dict, List, Any, Tuple, Set
from datetime import datetime
import re

from app.exceptions import (
    ValidationError,
    NotFoundError,
    DuplicateError,
    BusinessRuleError,
    PermissionDeniedError,
    ErrorCodes
)
from app.services.auth_service import AuthService


class AdminService:
    """
    Servicio de administración del sistema.
    Gestiona usuarios, roles, permisos y empresas.
    """

    # ==========================================================================
    # CONSTANTES - VALIDACIÓN
    # ==========================================================================

    # Longitud mínima de nombres
    MIN_NAME_LENGTH = 2
    MAX_NAME_LENGTH = 100

    # Longitud para descripciones
    MAX_DESCRIPTION_LENGTH = 500

    # Rol de administrador
    ADMIN_ROLE_NAME = "Administrador"
    ADMIN_ROLE_ID = 1

    # Permisos de administración
    PERM_MANAGE_USERS = "admin.can_manage_users"
    PERM_MANAGE_ROLES = "admin.can_manage_roles"
    PERM_MANAGE_COMPANIES = "admin.can_manage_companies"

    # ==========================================================================
    # VALIDACIÓN DE USUARIOS
    # ==========================================================================

    @staticmethod
    def validate_user_data(
        username: str,
        full_name: str,
        role_id: int,
        company_ids: Optional[List[int]] = None,
        password: Optional[str] = None,
        is_new: bool = True
    ) -> Dict[str, Any]:
        """
        Valida y normaliza los datos de usuario.

        Args:
            username: Nombre de usuario
            full_name: Nombre completo
            role_id: ID del rol
            company_ids: Lista de IDs de empresas asignadas
            password: Contraseña (requerida para usuarios nuevos)
            is_new: Si es un usuario nuevo

        Returns:
            Dict[str, Any]: Datos validados y normalizados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        # Validar username
        normalized_username = AuthService.validate_username(username)

        # Validar nombre completo
        if not full_name or len(full_name.strip()) < AdminService.MIN_NAME_LENGTH:
            raise ValidationError(
                f"El nombre completo debe tener al menos {AdminService.MIN_NAME_LENGTH} caracteres",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "full_name"}
            )

        if len(full_name) > AdminService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre completo no puede exceder {AdminService.MAX_NAME_LENGTH} caracteres",
                "ADMIN_NAME_TOO_LONG",
                {"max_length": AdminService.MAX_NAME_LENGTH}
            )

        # Validar rol
        if not role_id or role_id < 1:
            raise ValidationError(
                "El rol es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "role_id"}
            )

        # Validar contraseña para usuarios nuevos
        if is_new and password:
            AuthService.validate_password_strength(password)

        # Validar que tenga al menos una empresa
        if is_new and (not company_ids or len(company_ids) == 0):
            raise ValidationError(
                "El usuario debe estar asignado a al menos una empresa",
                "ADMIN_USER_NO_COMPANIES"
            )

        return {
            "username": normalized_username,
            "full_name": full_name.strip(),
            "role_id": role_id,
            "company_ids": company_ids or [],
        }

    @staticmethod
    def prepare_user_for_creation(
        username: str,
        password: str,
        full_name: str,
        role_id: int,
        company_ids: List[int],
        warehouse_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Prepara los datos del usuario para creación.

        Args:
            username: Nombre de usuario
            password: Contraseña en texto plano
            full_name: Nombre completo
            role_id: ID del rol
            company_ids: Lista de IDs de empresas
            warehouse_ids: Lista opcional de IDs de almacenes

        Returns:
            Dict[str, Any]: Datos listos para el repositorio
        """
        # Validar datos
        validated = AdminService.validate_user_data(
            username=username,
            full_name=full_name,
            role_id=role_id,
            company_ids=company_ids,
            password=password,
            is_new=True
        )

        # Hash de contraseña
        password_hash = AuthService.hash_password_sha256(password)

        return {
            "username": validated["username"],
            "password_hash": password_hash,
            "full_name": validated["full_name"],
            "role_id": validated["role_id"],
            "company_ids": validated["company_ids"],
            "warehouse_ids": warehouse_ids or [],
            "is_active": True,
            "must_change_password": True  # Forzar cambio en primer login
        }

    @staticmethod
    def prepare_user_for_update(
        full_name: Optional[str] = None,
        role_id: Optional[int] = None,
        is_active: Optional[bool] = None,
        new_password: Optional[str] = None,
        company_ids: Optional[List[int]] = None,
        warehouse_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Prepara los datos del usuario para actualización.

        Args:
            full_name: Nuevo nombre completo (opcional)
            role_id: Nuevo rol (opcional)
            is_active: Estado activo (opcional)
            new_password: Nueva contraseña (opcional)
            company_ids: Nuevas empresas (opcional)
            warehouse_ids: Nuevos almacenes (opcional)

        Returns:
            Dict[str, Any]: Datos a actualizar
        """
        updates = {}

        if full_name is not None:
            if len(full_name.strip()) < AdminService.MIN_NAME_LENGTH:
                raise ValidationError(
                    f"El nombre debe tener al menos {AdminService.MIN_NAME_LENGTH} caracteres",
                    ErrorCodes.MISSING_REQUIRED_FIELD,
                    {"field": "full_name"}
                )
            updates["full_name"] = full_name.strip()

        if role_id is not None:
            updates["role_id"] = role_id

        if is_active is not None:
            updates["is_active"] = is_active

        if new_password is not None:
            AuthService.validate_password_strength(new_password)
            updates["password_hash"] = AuthService.hash_password_sha256(new_password)
            updates["must_change_password"] = True

        if company_ids is not None:
            updates["company_ids"] = company_ids

        if warehouse_ids is not None:
            updates["warehouse_ids"] = warehouse_ids

        return updates

    # ==========================================================================
    # VALIDACIÓN DE ROLES
    # ==========================================================================

    @staticmethod
    def validate_role_data(
        name: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Valida los datos de un rol.

        Args:
            name: Nombre del rol
            description: Descripción opcional

        Returns:
            Dict[str, Any]: Datos validados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        if not name or len(name.strip()) < AdminService.MIN_NAME_LENGTH:
            raise ValidationError(
                f"El nombre del rol debe tener al menos {AdminService.MIN_NAME_LENGTH} caracteres",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )

        if len(name) > AdminService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre del rol no puede exceder {AdminService.MAX_NAME_LENGTH} caracteres",
                "ADMIN_ROLE_NAME_TOO_LONG",
                {"max_length": AdminService.MAX_NAME_LENGTH}
            )

        result = {
            "name": name.strip()
        }

        if description is not None:
            if len(description) > AdminService.MAX_DESCRIPTION_LENGTH:
                raise ValidationError(
                    f"La descripción no puede exceder {AdminService.MAX_DESCRIPTION_LENGTH} caracteres",
                    "ADMIN_DESCRIPTION_TOO_LONG",
                    {"max_length": AdminService.MAX_DESCRIPTION_LENGTH}
                )
            result["description"] = description.strip()

        return result

    @staticmethod
    def can_modify_role(role_id: int, role_name: str) -> bool:
        """
        Verifica si un rol puede ser modificado.

        Args:
            role_id: ID del rol
            role_name: Nombre del rol

        Returns:
            bool: True si puede modificarse
        """
        # El rol Administrador no puede modificarse
        if role_id == AdminService.ADMIN_ROLE_ID:
            return False
        if role_name == AdminService.ADMIN_ROLE_NAME:
            return False
        return True

    # ==========================================================================
    # VALIDACIÓN DE EMPRESAS
    # ==========================================================================

    @staticmethod
    def validate_company_data(
        name: str,
        country: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Valida los datos de una empresa.

        Args:
            name: Nombre de la empresa
            country: País (opcional)

        Returns:
            Dict[str, Any]: Datos validados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        if not name or len(name.strip()) < AdminService.MIN_NAME_LENGTH:
            raise ValidationError(
                f"El nombre de la empresa debe tener al menos {AdminService.MIN_NAME_LENGTH} caracteres",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )

        if len(name) > AdminService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre de la empresa no puede exceder {AdminService.MAX_NAME_LENGTH} caracteres",
                "ADMIN_COMPANY_NAME_TOO_LONG",
                {"max_length": AdminService.MAX_NAME_LENGTH}
            )

        result = {
            "name": name.strip()
        }

        if country is not None:
            result["country"] = country.strip()

        return result

    @staticmethod
    def check_company_can_be_deleted(
        has_warehouses: bool,
        has_products: bool,
        has_operations: bool
    ) -> Tuple[bool, Optional[str]]:
        """
        Verifica si una empresa puede ser eliminada.

        Args:
            has_warehouses: Si tiene almacenes con stock
            has_products: Si tiene productos
            has_operations: Si tiene operaciones

        Returns:
            Tuple[bool, Optional[str]]: (puede_eliminar, mensaje_error)
        """
        if has_operations:
            return False, "La empresa tiene operaciones registradas y no puede eliminarse."

        if has_products:
            return False, "La empresa tiene productos registrados. Elimine o transfiera los productos primero."

        if has_warehouses:
            return False, "La empresa tiene almacenes. Elimine los almacenes primero."

        return True, None

    # ==========================================================================
    # VALIDACIÓN DE PERMISOS
    # ==========================================================================

    @staticmethod
    def validate_permission_update(
        role_id: int,
        role_name: str,
        permission_id: int,
        action: str
    ) -> None:
        """
        Valida una actualización de permisos.

        Args:
            role_id: ID del rol
            role_name: Nombre del rol
            permission_id: ID del permiso
            action: 'add' o 'remove'

        Raises:
            ValidationError: Si la operación no es válida
            BusinessRuleError: Si se intenta modificar el rol admin
        """
        if action not in ('add', 'remove'):
            raise ValidationError(
                "La acción debe ser 'add' o 'remove'",
                "ADMIN_INVALID_PERMISSION_ACTION",
                {"action": action}
            )

        # El rol admin no puede modificarse
        if not AdminService.can_modify_role(role_id, role_name):
            raise BusinessRuleError(
                "No se pueden modificar los permisos del rol Administrador",
                "ADMIN_CANNOT_MODIFY_ADMIN_ROLE",
                {"role_id": role_id}
            )

    # ==========================================================================
    # FILTRADO DE DATOS SEGÚN ROL
    # ==========================================================================

    @staticmethod
    def filter_companies_for_user(
        all_companies: List[Dict[str, Any]],
        user_role_name: str,
        user_company_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """
        Filtra las empresas visibles según el rol del usuario.

        Args:
            all_companies: Lista de todas las empresas
            user_role_name: Nombre del rol del usuario
            user_company_ids: IDs de empresas del usuario

        Returns:
            List[Dict]: Empresas filtradas
        """
        # Administradores ven todas
        if user_role_name == AdminService.ADMIN_ROLE_NAME:
            return all_companies

        # Otros solo ven las asignadas
        return [c for c in all_companies if c['id'] in user_company_ids]

    @staticmethod
    def filter_warehouses_for_user(
        all_warehouses: List[Dict[str, Any]],
        user_warehouse_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """
        Filtra los almacenes visibles para el usuario.

        Args:
            all_warehouses: Lista de todos los almacenes
            user_warehouse_ids: IDs de almacenes del usuario

        Returns:
            List[Dict]: Almacenes filtrados
        """
        return [w for w in all_warehouses if w['id'] in user_warehouse_ids]

    # ==========================================================================
    # CONSTRUCCIÓN DE RESPUESTAS
    # ==========================================================================

    @staticmethod
    def build_user_response(user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construye la respuesta de usuario para la API.

        Args:
            user_data: Datos del usuario del repositorio

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "id": user_data.get("id"),
            "username": user_data.get("username"),
            "full_name": user_data.get("full_name"),
            "role_id": user_data.get("role_id"),
            "role_name": user_data.get("role_name"),
            "is_active": user_data.get("is_active", True),
            "must_change_password": user_data.get("must_change_password", False),
            "company_ids": user_data.get("company_ids", []),
            "warehouse_ids": user_data.get("warehouse_ids", [])
        }

    @staticmethod
    def build_role_response(role_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construye la respuesta de rol para la API.

        Args:
            role_data: Datos del rol del repositorio

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "id": role_data.get("id"),
            "name": role_data.get("name"),
            "description": role_data.get("description", "")
        }

    @staticmethod
    def build_company_response(company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construye la respuesta de empresa para la API.

        Args:
            company_data: Datos de la empresa del repositorio

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "id": company_data.get("id"),
            "name": company_data.get("name"),
            "country": company_data.get("country", "")
        }

    @staticmethod
    def build_permission_matrix(
        roles: List[Dict[str, Any]],
        permissions: List[Dict[str, Any]],
        role_permissions: Dict[int, List[int]]
    ) -> Dict[str, Any]:
        """
        Construye la matriz de permisos para la UI.

        Args:
            roles: Lista de roles
            permissions: Lista de permisos
            role_permissions: Mapeo {role_id: [permission_ids]}

        Returns:
            Dict[str, Any]: Matriz estructurada para la UI
        """
        return {
            "roles": roles,
            "permissions": permissions,
            "matrix": role_permissions
        }
