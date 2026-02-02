# app/services/location_service.py
"""
Service Layer para Locations (Ubicaciones).
Contiene lógica de negocio, validación y normalización.
El repositorio solo ejecuta SQL puro.
"""

from typing import Optional, Dict, List, Any, Tuple
import io
import csv

from app.exceptions import (
    ValidationError,
    NotFoundError,
    ErrorCodes
)


class LocationService:
    """
    Servicio de lógica de negocio para Locations.
    Maneja normalización, validación y procesamiento de CSV.
    """

    # Tipos de ubicación válidos
    VALID_LOCATION_TYPES = {
        'internal', 'vendor', 'customer',
        'inventory', 'production', 'transit'
    }

    # Mapeo de tipos para display
    LOCATION_TYPE_MAP = {
        "internal": "Ubicación Interna",
        "vendor": "Ubic. Proveedor (Virtual)",
        "customer": "Ubic. Cliente (Virtual)",
        "inventory": "Pérdida Inventario (Virtual)",
        "production": "Producción (Virtual)",
        "transit": "Tránsito (Virtual)",
    }

    # Mapeo inverso para importación
    LOCATION_TYPE_MAP_REVERSE = {
        "Ubicación Interna": "internal",
        "Ubic. Proveedor (Virtual)": "vendor",
        "Ubic. Cliente (Virtual)": "customer",
        "Pérdida Inventario (Virtual)": "inventory",
        "Producción (Virtual)": "production",
        "Tránsito (Virtual)": "transit",
    }

    # Headers requeridos para importación CSV
    REQUIRED_CSV_HEADERS = {"name", "type"}

    # ==========================================================================
    # NORMALIZACIÓN
    # ==========================================================================

    @staticmethod
    def normalize_name(name: Optional[str]) -> str:
        """
        Normaliza el nombre de la ubicación a mayúsculas.

        Args:
            name: Nombre a normalizar

        Returns:
            str: Nombre normalizado en mayúsculas

        Raises:
            ValidationError: Si el nombre está vacío
        """
        if not name or not name.strip():
            raise ValidationError(
                "El nombre de la ubicación es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )
        return name.strip().upper()

    @staticmethod
    def normalize_path(path: Optional[str]) -> str:
        """
        Normaliza el path de la ubicación a mayúsculas.

        Args:
            path: Path a normalizar

        Returns:
            str: Path normalizado en mayúsculas

        Raises:
            ValidationError: Si el path está vacío
        """
        if not path or not path.strip():
            raise ValidationError(
                "El path de la ubicación es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "path"}
            )
        return path.strip().upper()

    @staticmethod
    def normalize_type(loc_type: Optional[str]) -> str:
        """
        Normaliza y valida el tipo de ubicación.

        Args:
            loc_type: Tipo a normalizar

        Returns:
            str: Tipo normalizado

        Raises:
            ValidationError: Si el tipo es inválido
        """
        if not loc_type or not loc_type.strip():
            raise ValidationError(
                "El tipo de ubicación es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "type"}
            )

        type_lower = loc_type.strip().lower()

        if type_lower not in LocationService.VALID_LOCATION_TYPES:
            raise ValidationError(
                f"Tipo de ubicación '{loc_type}' inválido. "
                f"Valores válidos: {', '.join(LocationService.VALID_LOCATION_TYPES)}",
                "LOC_INVALID_TYPE",
                {"field": "type", "valid_values": list(LocationService.VALID_LOCATION_TYPES)}
            )

        return type_lower

    @staticmethod
    def normalize_category(category: Optional[str]) -> Optional[str]:
        """
        Normaliza la categoría de la ubicación.

        Args:
            category: Categoría a normalizar

        Returns:
            Optional[str]: Categoría normalizada o None
        """
        if not category or not category.strip():
            return None
        return category.strip()

    # ==========================================================================
    # VALIDACIÓN
    # ==========================================================================

    @staticmethod
    def validate_warehouse_for_internal(loc_type: str, warehouse_id: Optional[int]) -> Optional[int]:
        """
        Valida que las ubicaciones internas tengan un almacén asociado.

        Args:
            loc_type: Tipo de ubicación
            warehouse_id: ID del almacén

        Returns:
            Optional[int]: warehouse_id validado (None para virtuales)

        Raises:
            ValidationError: Si una ubicación interna no tiene almacén
        """
        if loc_type == 'internal':
            if not warehouse_id:
                raise ValidationError(
                    "Se requiere un Almacén Asociado para ubicaciones de tipo 'Interna'",
                    "LOC_WAREHOUSE_REQUIRED",
                    {"field": "warehouse_id"}
                )
            return warehouse_id
        else:
            # Ubicaciones virtuales no tienen almacén
            return None

    @staticmethod
    def generate_path_for_internal(warehouse_code: str, location_name: str) -> str:
        """
        Genera el path automático para ubicaciones internas.

        Args:
            warehouse_code: Código del almacén
            location_name: Nombre de la ubicación

        Returns:
            str: Path generado (WH_CODE/LOCATION_NAME)
        """
        if not warehouse_code:
            raise ValidationError(
                "No se puede generar Path: Almacén sin código",
                "LOC_WAREHOUSE_NO_CODE",
                {"field": "warehouse_code"}
            )
        return f"{warehouse_code.upper()}/{location_name.upper()}"

    # ==========================================================================
    # PREPARACIÓN DE DATOS
    # ==========================================================================

    @staticmethod
    def prepare_location_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara y normaliza los datos de una ubicación para crear/actualizar.

        Args:
            data: Diccionario con datos de la ubicación

        Returns:
            Dict[str, Any]: Datos normalizados listos para el repositorio
        """
        loc_type = LocationService.normalize_type(data.get("type"))
        warehouse_id = LocationService.validate_warehouse_for_internal(
            loc_type,
            data.get("warehouse_id")
        )

        return {
            "name": LocationService.normalize_name(data.get("name")),
            "path": LocationService.normalize_path(data.get("path")),
            "type": loc_type,
            "category": LocationService.normalize_category(data.get("category")),
            "warehouse_id": warehouse_id,
        }

    @staticmethod
    def build_filter_dict(
        path: Optional[str] = None,
        loc_type: Optional[str] = None,
        warehouse_name: Optional[str] = None,
        warehouse_status: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Construye un diccionario de filtros limpio.

        Args:
            path: Filtro por path
            loc_type: Filtro por tipo
            warehouse_name: Filtro por nombre de almacén
            warehouse_status: Filtro por estado del almacén

        Returns:
            Dict[str, str]: Diccionario de filtros sin valores vacíos
        """
        filters = {
            "path": path,
            "type": loc_type,
            "warehouse_name": warehouse_name,
            "warehouse_status": warehouse_status
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

            # Detectar delimitador
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(content_decoded[:1024], delimiters=";,")
            except csv.Error:
                dialect = csv.excel
                dialect.delimiter = ';'

            reader = csv.DictReader(file_io, dialect=dialect)

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
        missing = LocationService.REQUIRED_CSV_HEADERS - headers_set

        if missing:
            raise ValidationError(
                f"Faltan columnas obligatorias: {', '.join(sorted(missing))}",
                ErrorCodes.CSV_MISSING_HEADERS,
                {"missing_headers": list(missing)}
            )

    @staticmethod
    def process_csv_row(
        row: Dict[str, str],
        row_num: int,
        warehouse_map: Dict[str, int],
        warehouse_code_map: Dict[int, str]
    ) -> Dict[str, Any]:
        """
        Procesa y valida una fila individual del CSV.

        Args:
            row: Fila del CSV
            row_num: Número de fila (para mensajes de error)
            warehouse_map: Mapeo nombre_almacén (upper) -> id
            warehouse_code_map: Mapeo id_almacén -> código

        Returns:
            Dict[str, Any]: Datos normalizados para upsert

        Raises:
            ValidationError: Si la fila tiene datos inválidos
        """
        raw_path = row.get('path', '').strip()
        name = row.get('name', '').strip()
        type_str = row.get('type', '').strip()

        if not name:
            raise ValidationError(
                f"Fila {row_num}: nombre es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "name"}
            )

        if not type_str:
            raise ValidationError(
                f"Fila {row_num}: tipo es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "type"}
            )

        # Resolver tipo (puede venir como código o como texto display)
        type_code = LocationService.LOCATION_TYPE_MAP_REVERSE.get(type_str)
        if not type_code:
            if type_str.lower() in LocationService.VALID_LOCATION_TYPES:
                type_code = type_str.lower()
            else:
                raise ValidationError(
                    f"Fila {row_num}: Tipo '{type_str}' inválido",
                    ErrorCodes.CSV_ROW_ERROR,
                    {"row": row_num, "field": "type"}
                )

        # Validar almacén para ubicaciones internas
        warehouse_id = None
        if type_code == 'internal':
            wh_name = row.get('warehouse_name', '').strip()
            if not wh_name:
                raise ValidationError(
                    f"Fila {row_num}: warehouse_name es obligatorio para Ubicación Interna",
                    ErrorCodes.CSV_ROW_ERROR,
                    {"row": row_num, "field": "warehouse_name"}
                )

            warehouse_id = warehouse_map.get(wh_name.upper())
            if not warehouse_id:
                raise ValidationError(
                    f"Fila {row_num}: Almacén '{wh_name}' no existe",
                    ErrorCodes.CSV_INVALID_REFERENCES,
                    {"row": row_num, "warehouse": wh_name}
                )

        # Auto-generación de path
        final_path = raw_path.upper() if raw_path else None
        if not final_path:
            if type_code == 'internal' and warehouse_id:
                wh_code = warehouse_code_map.get(warehouse_id)
                if wh_code:
                    final_path = f"{wh_code}/{name.upper()}"
                else:
                    raise ValidationError(
                        f"Fila {row_num}: No se pudo generar Path (Almacén sin código)",
                        ErrorCodes.CSV_ROW_ERROR,
                        {"row": row_num}
                    )
            else:
                # Para virtuales, usamos el nombre como path
                final_path = name.upper()

        return {
            "name": name.upper(),
            "path": final_path,
            "type": type_code,
            "category": row.get('category', '').strip() or None,
            "warehouse_id": warehouse_id,
        }

    # ==========================================================================
    # GENERACIÓN DE CSV
    # ==========================================================================

    @staticmethod
    def generate_csv_content(locations: List[Dict[str, Any]]) -> str:
        """
        Genera contenido CSV a partir de una lista de ubicaciones.

        Args:
            locations: Lista de diccionarios de ubicaciones

        Returns:
            str: Contenido CSV como string
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        headers = ["path", "name", "type", "warehouse_name", "category"]
        writer.writerow(headers)

        for loc in locations:
            loc_dict = dict(loc) if hasattr(loc, 'keys') else loc
            type_display = LocationService.LOCATION_TYPE_MAP.get(
                loc_dict.get('type'),
                loc_dict.get('type', '')
            )
            writer.writerow([
                loc_dict.get('path', ''),
                loc_dict.get('name', ''),
                type_display,
                loc_dict.get('warehouse_name', '') or '',
                loc_dict.get('category', '') or ''
            ])

        return output.getvalue()
