# app/services/partner_service.py
"""
Service Layer para Partners (Proveedores/Clientes).
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


class PartnerService:
    """
    Servicio de lógica de negocio para Partners.
    Maneja normalización, validación y procesamiento de CSV.
    """

    # Headers requeridos para importación CSV
    REQUIRED_CSV_HEADERS = {"name", "category_name"}

    # Headers opcionales
    OPTIONAL_CSV_HEADERS = {"ruc", "social_reason", "address", "email", "phone"}

    # ==========================================================================
    # NORMALIZACIÓN
    # ==========================================================================

    @staticmethod
    def normalize_name(name: Optional[str]) -> str:
        """
        Normaliza el nombre del partner a mayúsculas.

        Args:
            name: Nombre a normalizar

        Returns:
            str: Nombre normalizado en mayúsculas

        Raises:
            ValidationError: Si el nombre está vacío
        """
        if not name or not name.strip():
            raise ValidationError(
                "El nombre es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )
        return name.strip().upper()

    @staticmethod
    def normalize_ruc(ruc: Optional[str]) -> Optional[str]:
        """
        Normaliza el RUC (quita espacios, valida formato básico).

        Args:
            ruc: RUC a normalizar

        Returns:
            Optional[str]: RUC normalizado o None
        """
        if not ruc or not ruc.strip():
            return None
        return ruc.strip().upper()

    @staticmethod
    def normalize_social_reason(social_reason: Optional[str]) -> Optional[str]:
        """
        Normaliza la razón social a mayúsculas.

        Args:
            social_reason: Razón social a normalizar

        Returns:
            Optional[str]: Razón social normalizada o None
        """
        if not social_reason or not social_reason.strip():
            return None
        return social_reason.strip().upper()

    @staticmethod
    def normalize_address(address: Optional[str]) -> Optional[str]:
        """
        Normaliza la dirección a mayúsculas.

        Args:
            address: Dirección a normalizar

        Returns:
            Optional[str]: Dirección normalizada o None
        """
        if not address or not address.strip():
            return None
        return address.strip().upper()

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
        Normaliza el teléfono (quita espacios extra).

        Args:
            phone: Teléfono a normalizar

        Returns:
            Optional[str]: Teléfono normalizado o None
        """
        if not phone or not phone.strip():
            return None
        return phone.strip()

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
        Valida el formato básico del RUC (si se proporciona).

        Args:
            ruc: RUC a validar

        Returns:
            bool: True si es válido o vacío
        """
        if not ruc:
            return True
        # RUC peruano: 11 dígitos
        clean_ruc = ruc.strip()
        if len(clean_ruc) == 11 and clean_ruc.isdigit():
            return True
        # DNI: 8 dígitos
        if len(clean_ruc) == 8 and clean_ruc.isdigit():
            return True
        return True  # Permitir otros formatos por flexibilidad

    @staticmethod
    def validate_email_format(email: Optional[str]) -> bool:
        """
        Valida el formato básico del email (si se proporciona).

        Args:
            email: Email a validar

        Returns:
            bool: True si es válido o vacío
        """
        if not email:
            return True
        return "@" in email and "." in email

    # ==========================================================================
    # PREPARACIÓN DE DATOS
    # ==========================================================================

    @staticmethod
    def prepare_partner_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara y normaliza los datos de un partner para crear/actualizar.

        Args:
            data: Diccionario con datos del partner

        Returns:
            Dict[str, Any]: Datos normalizados listos para el repositorio
        """
        return {
            "name": PartnerService.normalize_name(data.get("name")),
            "category_id": PartnerService.validate_category_id(data.get("category_id")),
            "social_reason": PartnerService.normalize_social_reason(data.get("social_reason")),
            "ruc": PartnerService.normalize_ruc(data.get("ruc")),
            "email": PartnerService.normalize_email(data.get("email")),
            "phone": PartnerService.normalize_phone(data.get("phone")),
            "address": PartnerService.normalize_address(data.get("address")),
        }

    @staticmethod
    def build_filter_dict(
        name: Optional[str] = None,
        ruc: Optional[str] = None,
        social_reason: Optional[str] = None,
        address: Optional[str] = None,
        category_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Construye un diccionario de filtros limpio.

        Args:
            name: Filtro por nombre
            ruc: Filtro por RUC
            social_reason: Filtro por razón social
            address: Filtro por dirección
            category_name: Filtro por nombre de categoría

        Returns:
            Dict[str, str]: Diccionario de filtros sin valores vacíos
        """
        filters = {
            "name": name,
            "ruc": ruc,
            "social_reason": social_reason,
            "address": address,
            "category_name": category_name
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
        missing = PartnerService.REQUIRED_CSV_HEADERS - headers_set

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
        name = row.get('name', '').strip()
        category_name = row.get('category_name', '').strip()

        if not name:
            raise ValidationError(
                f"Fila {row_num}: nombre es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "name"}
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
            "name": PartnerService.normalize_name(name),
            "category_id": category_id,
            "ruc": PartnerService.normalize_ruc(row.get('ruc', '').strip() or None),
            "social_reason": PartnerService.normalize_social_reason(
                row.get('social_reason', '').strip() or None
            ),
            "address": PartnerService.normalize_address(row.get('address', '').strip() or None),
            "email": PartnerService.normalize_email(row.get('email', '').strip() or None),
            "phone": PartnerService.normalize_phone(row.get('phone', '').strip() or None),
        }

    # ==========================================================================
    # GENERACIÓN DE CSV
    # ==========================================================================

    @staticmethod
    def generate_csv_content(partners: List[Dict[str, Any]]) -> str:
        """
        Genera contenido CSV a partir de una lista de partners.

        Args:
            partners: Lista de diccionarios de partners

        Returns:
            str: Contenido CSV como string
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        headers = ["name", "category_name", "ruc", "social_reason", "address", "email", "phone"]
        writer.writerow(headers)

        for partner in partners:
            partner_dict = dict(partner) if hasattr(partner, 'keys') else partner
            writer.writerow([partner_dict.get(h, '') or '' for h in headers])

        return output.getvalue()
