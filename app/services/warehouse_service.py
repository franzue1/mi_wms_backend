# app/services/warehouse_service.py
"""
Service Layer para Warehouses (Almacenes).
Contiene lógica de negocio, validación y normalización.
El repositorio solo ejecuta SQL puro.
"""

from typing import Optional, Dict, List, Any, Tuple
import io
import csv

from app.exceptions import (
    ValidationError,
    NotFoundError,
    DuplicateError,
    ErrorCodes
)


class WarehouseService:
    """
    Servicio de lógica de negocio para Warehouses.
    Maneja normalización, validación y procesamiento de CSV.
    """

    # Headers requeridos para importación CSV
    REQUIRED_CSV_HEADERS = {
        "code", "name", "status", "social_reason",
        "ruc", "email", "phone", "address", "category_name"
    }

    # Estados válidos
    VALID_STATUS_VALUES = {'activo', 'inactivo'}

    # ==========================================================================
    # NORMALIZACIÓN
    # ==========================================================================

    @staticmethod
    def normalize_name(name: Optional[str]) -> str:
        """
        Normaliza el nombre del almacén.

        Args:
            name: Nombre a normalizar

        Returns:
            str: Nombre normalizado (strip, preserva case)

        Raises:
            ValidationError: Si el nombre está vacío
        """
        if not name or not name.strip():
            raise ValidationError(
                "El nombre del almacén es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )
        return name.strip()

    @staticmethod
    def normalize_code(code: Optional[str]) -> str:
        """
        Normaliza el código del almacén a mayúsculas.

        Args:
            code: Código a normalizar

        Returns:
            str: Código normalizado en mayúsculas

        Raises:
            ValidationError: Si el código está vacío o excede 5 caracteres
        """
        if not code or not code.strip():
            raise ValidationError(
                "El código del almacén es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "code"}
            )

        code_upper = code.strip().upper()

        if len(code_upper) > 5:
            raise ValidationError(
                "El código no puede exceder 5 caracteres",
                "WH_CODE_TOO_LONG",
                {"field": "code", "max_length": 5}
            )

        return code_upper

    @staticmethod
    def normalize_status(status: Optional[str]) -> str:
        """
        Normaliza y valida el estado del almacén.

        Args:
            status: Estado a normalizar

        Returns:
            str: Estado normalizado ('activo' o 'inactivo')

        Raises:
            ValidationError: Si el estado es inválido
        """
        if not status or not status.strip():
            return "activo"  # Default

        status_lower = status.strip().lower()

        if status_lower not in WarehouseService.VALID_STATUS_VALUES:
            raise ValidationError(
                f"Estado inválido '{status}'. Debe ser 'activo' o 'inactivo'",
                "WH_INVALID_STATUS",
                {"field": "status", "valid_values": list(WarehouseService.VALID_STATUS_VALUES)}
            )

        return status_lower

    @staticmethod
    def normalize_ruc(ruc: Optional[str]) -> Optional[str]:
        """
        Normaliza el RUC (quita espacios).

        Args:
            ruc: RUC a normalizar

        Returns:
            Optional[str]: RUC normalizado o None
        """
        if not ruc or not ruc.strip():
            return None
        return ruc.strip()

    @staticmethod
    def normalize_social_reason(social_reason: Optional[str]) -> Optional[str]:
        """
        Normaliza la razón social.

        Args:
            social_reason: Razón social a normalizar

        Returns:
            Optional[str]: Razón social normalizada o None
        """
        if not social_reason or not social_reason.strip():
            return None
        return social_reason.strip()

    @staticmethod
    def normalize_email(email: Optional[str]) -> Optional[str]:
        """
        Normaliza el email (minúsculas, sin espacios).

        Args:
            email: Email a normalizar

        Returns:
            Optional[str]: Email normalizado o None
        """
        if not email or not email.strip():
            return None
        return email.strip().lower()

    @staticmethod
    def normalize_phone(phone: Optional[str]) -> Optional[str]:
        """
        Normaliza el teléfono.

        Args:
            phone: Teléfono a normalizar

        Returns:
            Optional[str]: Teléfono normalizado o None
        """
        if not phone or not phone.strip():
            return None
        return phone.strip()

    @staticmethod
    def normalize_address(address: Optional[str]) -> Optional[str]:
        """
        Normaliza la dirección.

        Args:
            address: Dirección a normalizar

        Returns:
            Optional[str]: Dirección normalizada o None
        """
        if not address or not address.strip():
            return None
        return address.strip()

    # ==========================================================================
    # VALIDACIÓN
    # ==========================================================================

    @staticmethod
    def validate_category_id(category_id: Optional[int]) -> int:
        """
        Valida que se haya proporcionado un ID de categoría.

        Args:
            category_id: ID de categoría

        Returns:
            int: ID de categoría validado

        Raises:
            ValidationError: Si la categoría no está especificada
        """
        if not category_id:
            raise ValidationError(
                "La categoría es requerida",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "category_id"}
            )
        return category_id

    @staticmethod
    def validate_ruc_format(ruc: Optional[str]) -> bool:
        """
        Valida el formato del RUC peruano (11 dígitos).

        Args:
            ruc: RUC a validar

        Returns:
            bool: True si es válido o vacío

        Raises:
            ValidationError: Si el RUC tiene formato inválido
        """
        if not ruc:
            return True

        clean_ruc = ruc.strip()
        if clean_ruc and len(clean_ruc) != 11:
            raise ValidationError(
                "El RUC debe tener 11 dígitos",
                "WH_INVALID_RUC",
                {"field": "ruc", "required_length": 11}
            )

        if clean_ruc and not clean_ruc.isdigit():
            raise ValidationError(
                "El RUC solo debe contener números",
                "WH_INVALID_RUC",
                {"field": "ruc"}
            )

        return True

    @staticmethod
    def validate_phone_format(phone: Optional[str]) -> bool:
        """
        Valida el formato del teléfono (9 dígitos para Perú).

        Args:
            phone: Teléfono a validar

        Returns:
            bool: True si es válido o vacío

        Raises:
            ValidationError: Si el teléfono tiene formato inválido
        """
        if not phone:
            return True

        clean_phone = phone.strip()
        if clean_phone and len(clean_phone) != 9:
            raise ValidationError(
                "El teléfono debe tener 9 dígitos",
                "WH_INVALID_PHONE",
                {"field": "phone", "required_length": 9}
            )

        return True

    # ==========================================================================
    # PREPARACIÓN DE DATOS
    # ==========================================================================

    @staticmethod
    def prepare_warehouse_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara y normaliza los datos de un almacén para crear/actualizar.

        Args:
            data: Diccionario con datos del almacén

        Returns:
            Dict[str, Any]: Datos normalizados listos para el repositorio
        """
        return {
            "name": WarehouseService.normalize_name(data.get("name")),
            "code": WarehouseService.normalize_code(data.get("code")),
            "category_id": WarehouseService.validate_category_id(data.get("category_id")),
            "status": WarehouseService.normalize_status(data.get("status")),
            "social_reason": WarehouseService.normalize_social_reason(data.get("social_reason")),
            "ruc": WarehouseService.normalize_ruc(data.get("ruc")),
            "email": WarehouseService.normalize_email(data.get("email")),
            "phone": WarehouseService.normalize_phone(data.get("phone")),
            "address": WarehouseService.normalize_address(data.get("address")),
        }

    @staticmethod
    def build_filter_dict(
        name: Optional[str] = None,
        code: Optional[str] = None,
        status: Optional[str] = None,
        category_name: Optional[str] = None,
        ruc: Optional[str] = None,
        address: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Construye un diccionario de filtros limpio.

        Args:
            name: Filtro por nombre
            code: Filtro por código
            status: Filtro por estado
            category_name: Filtro por nombre de categoría
            ruc: Filtro por RUC
            address: Filtro por dirección

        Returns:
            Dict[str, str]: Diccionario de filtros sin valores vacíos
        """
        filters = {
            "name": name,
            "code": code,
            "status": status,
            "category_name": category_name,
            "ruc": ruc,
            "address": address
        }
        return {k: v for k, v in filters.items() if v is not None and v != ""}

    # ==========================================================================
    # PROCESAMIENTO DE CSV
    # ==========================================================================

    @staticmethod
    def parse_csv_file(content: bytes) -> Tuple[List[Dict[str, str]], List[str]]:
        """
        Parsea el contenido de un archivo CSV.

        Args:
            content: Contenido del archivo en bytes

        Returns:
            Tuple[List[Dict], List[str]]: (filas parseadas, headers encontrados)

        Raises:
            ValidationError: Si el archivo está vacío o mal formado
        """
        try:
            content_decoded = content.decode('utf-8-sig')
            file_io = io.StringIO(content_decoded)
            reader = csv.DictReader(file_io, delimiter=';')

            rows = list(reader)
            if not rows:
                raise ValidationError(
                    "El archivo CSV está vacío",
                    ErrorCodes.CSV_EMPTY_FILE
                )

            headers = {h.lower().strip() for h in reader.fieldnames or []}
            return rows, list(headers)

        except UnicodeDecodeError:
            raise ValidationError(
                "Error de codificación. Use UTF-8.",
                ErrorCodes.CSV_ENCODING_ERROR
            )

    @staticmethod
    def validate_csv_headers(headers: List[str]) -> None:
        """
        Valida que el CSV tenga los headers requeridos.

        Args:
            headers: Lista de headers encontrados

        Raises:
            ValidationError: Si faltan headers requeridos
        """
        headers_set = {h.lower().strip() for h in headers}
        missing = WarehouseService.REQUIRED_CSV_HEADERS - headers_set

        if missing:
            raise ValidationError(
                f"Faltan columnas obligatorias: {', '.join(sorted(missing))}",
                ErrorCodes.CSV_MISSING_HEADERS,
                {"missing_headers": list(missing)}
            )

    @staticmethod
    def validate_csv_categories(
        rows: List[Dict[str, str]],
        valid_categories: set
    ) -> None:
        """
        Valida que todas las categorías del CSV existan.

        Args:
            rows: Filas del CSV
            valid_categories: Set de nombres de categorías válidas

        Raises:
            ValidationError: Si hay categorías inválidas
        """
        csv_categories = {
            row.get('category_name', '').strip()
            for row in rows
            if row.get('category_name', '').strip()
        }

        invalid = csv_categories - valid_categories
        if invalid:
            raise ValidationError(
                f"Categorías no existen: {', '.join(sorted(invalid))}",
                ErrorCodes.CSV_INVALID_REFERENCES,
                {"invalid_categories": list(invalid)}
            )

    @staticmethod
    def process_csv_row(
        row: Dict[str, str],
        row_num: int,
        category_map: Dict[str, int]
    ) -> Dict[str, Any]:
        """
        Procesa y valida una fila individual del CSV.

        Args:
            row: Fila del CSV
            row_num: Número de fila (para mensajes de error)
            category_map: Mapeo nombre_categoría -> id

        Returns:
            Dict[str, Any]: Datos normalizados para upsert

        Raises:
            ValidationError: Si la fila tiene datos inválidos
        """
        code = row.get('code', '').strip().upper()
        name = row.get('name', '').strip()
        status = row.get('status', '').strip().lower()
        category_name = row.get('category_name', '').strip()

        if not code:
            raise ValidationError(
                f"Fila {row_num}: código es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "code"}
            )

        if not name:
            raise ValidationError(
                f"Fila {row_num}: nombre es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "name"}
            )

        if not status or status not in WarehouseService.VALID_STATUS_VALUES:
            raise ValidationError(
                f"Fila {row_num}: estado inválido (debe ser 'activo' o 'inactivo')",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "status"}
            )

        if not category_name:
            raise ValidationError(
                f"Fila {row_num}: category_name es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "category_name"}
            )

        category_id = category_map.get(category_name)
        if category_id is None:
            raise ValidationError(
                f"Fila {row_num}: Categoría '{category_name}' no encontrada",
                ErrorCodes.CSV_INVALID_REFERENCES,
                {"row": row_num, "category": category_name}
            )

        return {
            "code": code,
            "name": name,
            "status": status,
            "category_id": category_id,
            "social_reason": row.get('social_reason', '').strip() or None,
            "ruc": row.get('ruc', '').strip() or None,
            "email": (row.get('email', '').strip().lower() or None) if row.get('email') else None,
            "phone": row.get('phone', '').strip() or None,
            "address": row.get('address', '').strip() or None,
        }

    # ==========================================================================
    # GENERACIÓN DE CSV
    # ==========================================================================

    @staticmethod
    def generate_csv_content(warehouses: List[Dict[str, Any]]) -> str:
        """
        Genera contenido CSV a partir de una lista de almacenes.

        Args:
            warehouses: Lista de diccionarios de almacenes

        Returns:
            str: Contenido CSV como string
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        headers = ["code", "name", "status", "social_reason", "ruc", "email", "phone", "address", "category_name"]
        writer.writerow(headers)

        for wh in warehouses:
            wh_dict = dict(wh) if hasattr(wh, 'keys') else wh
            writer.writerow([wh_dict.get(h, '') or '' for h in headers])

        return output.getvalue()
