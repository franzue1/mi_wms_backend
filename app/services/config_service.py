# app/services/config_service.py
"""
Service Layer para Configuración General.
Maneja las tablas de configuración del sistema: categorías, unidades de medida, etc.
"""

from typing import Optional, Dict, List, Any, Tuple
import re

from app.exceptions import (
    ValidationError,
    NotFoundError,
    DuplicateError,
    BusinessRuleError,
    ErrorCodes
)


class ConfigService:
    """
    Servicio de configuración del sistema.
    Gestiona datos maestros como categorías y unidades de medida.
    """

    # ==========================================================================
    # CONSTANTES - VALIDACIÓN
    # ==========================================================================

    MIN_NAME_LENGTH = 1
    MAX_NAME_LENGTH = 100
    MAX_CODE_LENGTH = 20

    # ==========================================================================
    # VALIDACIÓN DE CATEGORÍAS DE PRODUCTOS
    # ==========================================================================

    @staticmethod
    def validate_product_category(
        name: str,
        company_id: int
    ) -> Dict[str, Any]:
        """
        Valida los datos de una categoría de producto.

        Args:
            name: Nombre de la categoría
            company_id: ID de la empresa

        Returns:
            Dict[str, Any]: Datos validados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        if not name or len(name.strip()) < ConfigService.MIN_NAME_LENGTH:
            raise ValidationError(
                "El nombre de la categoría es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )

        if len(name) > ConfigService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre no puede exceder {ConfigService.MAX_NAME_LENGTH} caracteres",
                "CONFIG_NAME_TOO_LONG",
                {"max_length": ConfigService.MAX_NAME_LENGTH}
            )

        if not company_id:
            raise ValidationError(
                "La empresa es requerida",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "company_id"}
            )

        return {
            "name": name.strip(),
            "company_id": company_id
        }

    # ==========================================================================
    # VALIDACIÓN DE UNIDADES DE MEDIDA
    # ==========================================================================

    @staticmethod
    def validate_uom(
        name: str,
        company_id: int,
        code: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Valida los datos de una unidad de medida.

        Args:
            name: Nombre de la UoM
            company_id: ID de la empresa
            code: Código corto opcional

        Returns:
            Dict[str, Any]: Datos validados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        if not name or len(name.strip()) < ConfigService.MIN_NAME_LENGTH:
            raise ValidationError(
                "El nombre de la unidad de medida es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )

        if len(name) > ConfigService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre no puede exceder {ConfigService.MAX_NAME_LENGTH} caracteres",
                "CONFIG_NAME_TOO_LONG",
                {"max_length": ConfigService.MAX_NAME_LENGTH}
            )

        if not company_id:
            raise ValidationError(
                "La empresa es requerida",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "company_id"}
            )

        result = {
            "name": name.strip(),
            "company_id": company_id
        }

        if code is not None:
            code = code.strip().upper()
            if len(code) > ConfigService.MAX_CODE_LENGTH:
                raise ValidationError(
                    f"El código no puede exceder {ConfigService.MAX_CODE_LENGTH} caracteres",
                    "CONFIG_CODE_TOO_LONG",
                    {"max_length": ConfigService.MAX_CODE_LENGTH}
                )
            result["code"] = code

        return result

    # ==========================================================================
    # VALIDACIÓN DE CATEGORÍAS DE ALMACÉN
    # ==========================================================================

    @staticmethod
    def validate_warehouse_category(
        name: str,
        company_id: int
    ) -> Dict[str, Any]:
        """
        Valida los datos de una categoría de almacén.

        Args:
            name: Nombre de la categoría
            company_id: ID de la empresa

        Returns:
            Dict[str, Any]: Datos validados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        if not name or len(name.strip()) < ConfigService.MIN_NAME_LENGTH:
            raise ValidationError(
                "El nombre de la categoría de almacén es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )

        if len(name) > ConfigService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre no puede exceder {ConfigService.MAX_NAME_LENGTH} caracteres",
                "CONFIG_NAME_TOO_LONG",
                {"max_length": ConfigService.MAX_NAME_LENGTH}
            )

        if not company_id:
            raise ValidationError(
                "La empresa es requerida",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "company_id"}
            )

        return {
            "name": name.strip(),
            "company_id": company_id
        }

    # ==========================================================================
    # VALIDACIÓN DE CATEGORÍAS DE PARTNER
    # ==========================================================================

    @staticmethod
    def validate_partner_category(
        name: str,
        company_id: int
    ) -> Dict[str, Any]:
        """
        Valida los datos de una categoría de partner.

        Args:
            name: Nombre de la categoría
            company_id: ID de la empresa

        Returns:
            Dict[str, Any]: Datos validados

        Raises:
            ValidationError: Si los datos son inválidos
        """
        if not name or len(name.strip()) < ConfigService.MIN_NAME_LENGTH:
            raise ValidationError(
                "El nombre de la categoría de contacto es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )

        if len(name) > ConfigService.MAX_NAME_LENGTH:
            raise ValidationError(
                f"El nombre no puede exceder {ConfigService.MAX_NAME_LENGTH} caracteres",
                "CONFIG_NAME_TOO_LONG",
                {"max_length": ConfigService.MAX_NAME_LENGTH}
            )

        if not company_id:
            raise ValidationError(
                "La empresa es requerida",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "company_id"}
            )

        return {
            "name": name.strip(),
            "company_id": company_id
        }

    # ==========================================================================
    # VERIFICACIÓN DE DEPENDENCIAS
    # ==========================================================================

    @staticmethod
    def check_category_can_be_deleted(
        has_products: bool,
        category_type: str = "product"
    ) -> Tuple[bool, Optional[str]]:
        """
        Verifica si una categoría puede ser eliminada.

        Args:
            has_products: Si tiene productos/elementos asociados
            category_type: Tipo de categoría

        Returns:
            Tuple[bool, Optional[str]]: (puede_eliminar, mensaje_error)
        """
        if has_products:
            type_names = {
                "product": "productos",
                "warehouse": "almacenes",
                "partner": "contactos"
            }
            entity = type_names.get(category_type, "elementos")
            return False, f"La categoría tiene {entity} asociados y no puede eliminarse."

        return True, None

    @staticmethod
    def check_uom_can_be_deleted(has_products: bool) -> Tuple[bool, Optional[str]]:
        """
        Verifica si una unidad de medida puede ser eliminada.

        Args:
            has_products: Si tiene productos asociados

        Returns:
            Tuple[bool, Optional[str]]: (puede_eliminar, mensaje_error)
        """
        if has_products:
            return False, "La unidad de medida tiene productos asociados y no puede eliminarse."

        return True, None

    # ==========================================================================
    # CONSTRUCCIÓN DE RESPUESTAS
    # ==========================================================================

    @staticmethod
    def build_category_response(
        category_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Construye la respuesta de categoría para la API.

        Args:
            category_data: Datos de la categoría del repositorio

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "id": category_data.get("id"),
            "name": category_data.get("name"),
            "company_id": category_data.get("company_id")
        }

    @staticmethod
    def build_uom_response(uom_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construye la respuesta de UoM para la API.

        Args:
            uom_data: Datos de la UoM del repositorio

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "id": uom_data.get("id"),
            "name": uom_data.get("name"),
            "code": uom_data.get("code"),
            "company_id": uom_data.get("company_id")
        }

    @staticmethod
    def build_config_list_response(
        items: List[Dict[str, Any]],
        total_count: int
    ) -> Dict[str, Any]:
        """
        Construye una respuesta de lista paginada.

        Args:
            items: Lista de elementos
            total_count: Conteo total

        Returns:
            Dict[str, Any]: Respuesta estructurada
        """
        return {
            "items": items,
            "total": total_count
        }
