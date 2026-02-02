# app/exceptions.py
"""
Excepciones de negocio para el WMS.
Todas las excepciones heredan de WMSBaseException para manejo centralizado.
"""

from typing import Dict, Any, Optional


class WMSBaseException(Exception):
    """Excepción base para todos los errores de negocio del WMS."""

    def __init__(self, message: str, code: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(WMSBaseException):
    """Error de validación de datos de entrada."""
    pass


class NotFoundError(WMSBaseException):
    """Recurso no encontrado."""
    pass


class DuplicateError(WMSBaseException):
    """Intento de crear un recurso duplicado."""
    pass


class BusinessRuleError(WMSBaseException):
    """Violación de regla de negocio."""
    pass


class PermissionDeniedError(WMSBaseException):
    """Usuario sin permisos suficientes."""
    pass


class ErrorCodes:
    """Códigos de error centralizados para toda la aplicación."""

    # Errores de Producto (PROD_xxx)
    PRODUCT_NOT_FOUND = "PROD_001"
    SKU_DUPLICATE = "PROD_002"
    SKU_INVALID_FORMAT = "PROD_003"
    PRODUCT_HAS_MOVEMENTS = "PROD_004"
    PRODUCT_INVALID_TYPE = "PROD_005"

    # Errores de Validación (VAL_xxx)
    INVALID_PRICE = "VAL_001"
    INVALID_TRACKING = "VAL_002"
    INVALID_OWNERSHIP = "VAL_003"
    MISSING_REQUIRED_FIELD = "VAL_004"
    INVALID_QUANTITY = "VAL_005"

    # Errores de CSV Import (CSV_xxx)
    CSV_MISSING_HEADERS = "CSV_001"
    CSV_INVALID_CATEGORY = "CSV_002"
    CSV_INVALID_UOM = "CSV_003"
    CSV_ROW_ERROR = "CSV_004"
    CSV_ENCODING_ERROR = "CSV_005"
    CSV_EMPTY_FILE = "CSV_006"
    CSV_INVALID_REFERENCES = "CSV_007"
    CSV_IMPORT_ERRORS = "CSV_008"

    # Errores de Almacén (WH_xxx)
    WAREHOUSE_NOT_FOUND = "WH_001"
    LOCATION_NOT_FOUND = "WH_002"
    INSUFFICIENT_STOCK = "WH_003"

    # Errores de Operaciones (OP_xxx)
    PICKING_NOT_FOUND = "OP_001"
    PICKING_ALREADY_DONE = "OP_002"
    PICKING_CANCELLED = "OP_003"
    INVALID_OPERATION_TYPE = "OP_004"
    INVALID_STATE = "OP_005"
    PICKING_EMPTY = "OP_006"
    STOCK_INSUFFICIENT = "OP_007"
    SERIAL_DUPLICATE = "OP_008"
    SERIAL_INVALID = "OP_009"
    TRACKING_MISMATCH = "OP_010"
    OWNERSHIP_MISMATCH = "OP_011"
    EMPLOYEE_REQUIRED = "OP_012"

    # Errores de Autenticación (AUTH_xxx)
    INVALID_CREDENTIALS = "AUTH_001"
    SESSION_EXPIRED = "AUTH_002"
    TOKEN_INVALID = "AUTH_003"

    # Errores de Permisos (PERM_xxx)
    PERMISSION_DENIED = "PERM_001"
    ROLE_NOT_FOUND = "PERM_002"

    # Errores de Partner (PART_xxx)
    PARTNER_NOT_FOUND = "PART_001"
    PARTNER_DUPLICATE_NAME = "PART_002"
    PARTNER_HAS_OPERATIONS = "PART_003"

    # Errores de Export (EXP_xxx)
    EXPORT_NO_DATA = "EXP_001"

    # Errores de Reportes (REP_xxx)
    REPORT_DATA_EMPTY = "REP_001"
    REPORT_INVALID_DATE_RANGE = "REP_002"
    REPORT_INVALID_FILTER = "REP_003"

    # Errores de Ajustes de Inventario (ADJ_xxx)
    ADJUSTMENT_NOT_FOUND = "ADJ_001"
    ADJUSTMENT_TYPE_NOT_CONFIGURED = "ADJ_002"
    ADJUSTMENT_VIRTUAL_LOCATION_MISSING = "ADJ_003"
    ADJUSTMENT_PHYSICAL_LOCATION_MISSING = "ADJ_004"
    ADJUSTMENT_INVALID_QUANTITY = "ADJ_005"
    ADJUSTMENT_STOCK_INSUFFICIENT = "ADJ_006"

    # Errores de Proyecto/Jerarquía (PROJ_xxx)
    PROJECT_NOT_FOUND = "PROJ_001"
    PROJECT_CODE_DUPLICATE = "PROJ_002"
    PROJECT_HAS_MOVEMENTS = "PROJ_003"
    PROJECT_INVALID_DATES = "PROJ_004"
    DIRECTION_NOT_FOUND = "PROJ_005"
    DIRECTION_HAS_CHILDREN = "PROJ_006"
    DIRECTION_DUPLICATE = "PROJ_007"
    MANAGEMENT_NOT_FOUND = "PROJ_008"
    MANAGEMENT_HAS_CHILDREN = "PROJ_009"
    MANAGEMENT_DUPLICATE = "PROJ_010"
    MACRO_PROJECT_NOT_FOUND = "PROJ_011"
    MACRO_PROJECT_HAS_CHILDREN = "PROJ_012"
    MACRO_PROJECT_DUPLICATE = "PROJ_013"
    HIERARCHY_MISSING_PARENT = "PROJ_014"
    HIERARCHY_INVALID_STRUCTURE = "PROJ_015"
    CODE_REQUIRED = "PROJ_016"
    PHASE_INVALID_TRANSITION = "PROJ_017"
