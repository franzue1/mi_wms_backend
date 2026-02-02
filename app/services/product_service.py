# app/services/product_service.py
"""
Servicio de productos - Capa de lógica de negocio.
Maneja validaciones, transformaciones y orquestación.
El repositorio solo debe ejecutar SQL puro.
"""

from typing import Dict, Any, Optional, List, Tuple, Set
import csv
import io
from app.exceptions import ValidationError, ErrorCodes


class ProductService:
    """
    Servicio para lógica de negocio relacionada con productos.
    Centraliza validaciones y reglas de negocio.
    """

    # === Valores válidos para enums ===
    VALID_TRACKING_VALUES = {'none', 'lot', 'serial'}
    VALID_OWNERSHIP_VALUES = {'owned', 'consigned'}
    VALID_PRODUCT_TYPES = {'storable', 'consumable', 'service'}

    # === Métodos de Normalización ===

    @staticmethod
    def normalize_sku(sku: Optional[str]) -> str:
        """
        Normaliza SKU a formato uppercase sin espacios.

        Args:
            sku: SKU a normalizar

        Returns:
            SKU normalizado en uppercase

        Raises:
            ValidationError: Si SKU es vacío o None
        """
        if not sku or not sku.strip():
            raise ValidationError(
                "El SKU es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "sku"}
            )
        return sku.strip().upper()

    @staticmethod
    def normalize_name(name: Optional[str]) -> str:
        """
        Normaliza nombre de producto a formato uppercase sin espacios extra.

        Args:
            name: Nombre a normalizar

        Returns:
            Nombre normalizado en uppercase

        Raises:
            ValidationError: Si nombre es vacío o None
        """
        if not name or not name.strip():
            raise ValidationError(
                "El nombre es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name"}
            )
        return name.strip().upper()

    # === Métodos de Validación ===

    @staticmethod
    def validate_price(price: Any) -> float:
        """
        Valida que el precio sea un número no negativo.

        Args:
            price: Precio a validar (puede ser string, int o float)

        Returns:
            Precio como float validado

        Raises:
            ValidationError: Si precio es negativo o no es numérico
        """
        try:
            if isinstance(price, str):
                price = price.replace(',', '.')
            price_float = float(price) if price else 0.0
        except (ValueError, TypeError):
            raise ValidationError(
                f"El precio '{price}' no es un número válido",
                ErrorCodes.INVALID_PRICE,
                {"provided_value": str(price)}
            )

        if price_float < 0:
            raise ValidationError(
                "El precio no puede ser negativo",
                ErrorCodes.INVALID_PRICE,
                {"provided_value": price_float}
            )
        return price_float

    @staticmethod
    def validate_tracking(tracking: Optional[str]) -> str:
        """
        Valida que el valor de tracking sea válido.

        Args:
            tracking: Valor de tracking ('none', 'lot', 'serial')

        Returns:
            Valor de tracking validado (lowercase)

        Raises:
            ValidationError: Si tracking no es un valor permitido
        """
        if not tracking:
            return 'none'  # Valor por defecto

        tracking_lower = tracking.strip().lower()
        if tracking_lower not in ProductService.VALID_TRACKING_VALUES:
            raise ValidationError(
                f"Valor de tracking inválido: '{tracking}'",
                ErrorCodes.INVALID_TRACKING,
                {
                    "provided_value": tracking,
                    "allowed_values": list(ProductService.VALID_TRACKING_VALUES)
                }
            )
        return tracking_lower

    @staticmethod
    def validate_ownership(ownership: Optional[str]) -> str:
        """
        Valida que el valor de ownership sea válido.

        Args:
            ownership: Valor de ownership ('owned', 'consigned')

        Returns:
            Valor de ownership validado (lowercase)

        Raises:
            ValidationError: Si ownership no es un valor permitido
        """
        if not ownership:
            return 'owned'  # Valor por defecto

        ownership_lower = ownership.strip().lower()
        if ownership_lower not in ProductService.VALID_OWNERSHIP_VALUES:
            raise ValidationError(
                f"Valor de propiedad inválido: '{ownership}'",
                ErrorCodes.INVALID_OWNERSHIP,
                {
                    "provided_value": ownership,
                    "allowed_values": list(ProductService.VALID_OWNERSHIP_VALUES)
                }
            )
        return ownership_lower

    @staticmethod
    def validate_quantity(quantity: Any, field_name: str = "quantity") -> float:
        """
        Valida que la cantidad sea un número positivo.

        Args:
            quantity: Cantidad a validar
            field_name: Nombre del campo para mensajes de error

        Returns:
            Cantidad como float validado

        Raises:
            ValidationError: Si cantidad es <= 0 o no es numérico
        """
        try:
            if isinstance(quantity, str):
                quantity = quantity.replace(',', '.')
            qty_float = float(quantity)
        except (ValueError, TypeError):
            raise ValidationError(
                f"La cantidad '{quantity}' no es un número válido",
                ErrorCodes.INVALID_QUANTITY,
                {"field": field_name, "provided_value": str(quantity)}
            )

        if qty_float <= 0:
            raise ValidationError(
                f"La cantidad debe ser mayor a cero",
                ErrorCodes.INVALID_QUANTITY,
                {"field": field_name, "provided_value": qty_float}
            )
        return qty_float

    # === Métodos de Construcción de Filtros ===

    @staticmethod
    def build_filter_dict(**kwargs) -> Dict[str, Any]:
        """
        Construye diccionario de filtros limpio, eliminando valores None y vacíos.
        Consolida lógica duplicada de construcción de filtros.

        Args:
            **kwargs: Pares clave-valor para filtros

        Returns:
            Diccionario con solo los filtros con valores válidos
        """
        return {k: v for k, v in kwargs.items() if v is not None and v != ""}

    # === Métodos de Preparación de Datos ===

    @staticmethod
    def prepare_product_data(
        sku: str,
        name: str,
        tracking: Optional[str] = None,
        ownership: Optional[str] = None,
        standard_price: Any = 0,
        **extra_fields
    ) -> Dict[str, Any]:
        """
        Prepara y valida datos de producto para crear/actualizar.
        Centraliza la lógica de normalización y validación.

        Args:
            sku: SKU del producto
            name: Nombre del producto
            tracking: Tipo de tracking
            ownership: Tipo de propiedad
            standard_price: Precio estándar
            **extra_fields: Campos adicionales (category_id, uom_id, etc.)

        Returns:
            Diccionario con datos normalizados y validados
        """
        return {
            "sku": ProductService.normalize_sku(sku),
            "name": ProductService.normalize_name(name),
            "tracking": ProductService.validate_tracking(tracking),
            "ownership": ProductService.validate_ownership(ownership),
            "standard_price": ProductService.validate_price(standard_price),
            **extra_fields
        }

    # =========================================================================
    # CSV IMPORT/EXPORT LOGIC
    # =========================================================================

    # Headers requeridos para importación CSV
    REQUIRED_CSV_HEADERS = {"sku", "name", "category_name", "uom_name", "tracking", "ownership", "standard_price"}

    # Headers para exportación CSV
    EXPORT_CSV_HEADERS = ["sku", "name", "ownership", "standard_price", "category_name", "uom_name", "tracking"]

    @staticmethod
    def parse_csv_file(file_content: bytes) -> Tuple[List[Dict], Set[str]]:
        """
        Parsea contenido de archivo CSV y retorna filas y headers.

        Args:
            file_content: Contenido del archivo en bytes

        Returns:
            Tupla (lista de filas como dicts, set de headers encontrados)

        Raises:
            ValidationError: Si hay error de encoding o el archivo está vacío
        """
        try:
            content_decoded = file_content.decode('utf-8-sig')
            file_io = io.StringIO(content_decoded)
            reader = csv.DictReader(file_io, delimiter=';')
            rows = list(reader)

            if not rows:
                raise ValidationError(
                    "El archivo CSV está vacío",
                    ErrorCodes.CSV_EMPTY_FILE
                )

            headers = {h.lower().strip() for h in reader.fieldnames or []}
            return rows, headers

        except UnicodeDecodeError as e:
            raise ValidationError(
                f"Error de codificación del archivo: {e}",
                ErrorCodes.CSV_ENCODING_ERROR
            )

    @staticmethod
    def validate_csv_headers(headers: Set[str]) -> None:
        """
        Valida que todos los headers requeridos estén presentes.

        Args:
            headers: Set de headers encontrados en el CSV

        Raises:
            ValidationError: Si faltan headers requeridos
        """
        missing = ProductService.REQUIRED_CSV_HEADERS - headers
        if missing:
            raise ValidationError(
                f"Faltan columnas requeridas: {', '.join(sorted(missing))}",
                ErrorCodes.CSV_MISSING_HEADERS,
                {"missing_columns": list(missing)}
            )

    @staticmethod
    def validate_csv_references(
        rows: List[Dict],
        valid_categories: Dict[str, int],
        valid_uoms: Dict[str, int]
    ) -> None:
        """
        Valida que todas las referencias a categorías y UoM existan.

        Args:
            rows: Lista de filas del CSV
            valid_categories: Dict de {nombre: id} de categorías válidas
            valid_uoms: Dict de {nombre: id} de UoMs válidas

        Raises:
            ValidationError: Si hay categorías o UoMs inválidas
        """
        invalid_categories = set()
        invalid_uoms = set()

        for row in rows:
            cat_name = row.get('category_name', '').strip()
            uom_name = row.get('uom_name', '').strip()

            if cat_name and cat_name not in valid_categories:
                invalid_categories.add(cat_name)
            if uom_name and uom_name not in valid_uoms:
                invalid_uoms.add(uom_name)

        errors = []
        details = {}

        if invalid_categories:
            errors.append(f"Categorías no existen: {', '.join(sorted(invalid_categories))}")
            details["invalid_categories"] = list(invalid_categories)
        if invalid_uoms:
            errors.append(f"UdM no existen: {', '.join(sorted(invalid_uoms))}")
            details["invalid_uoms"] = list(invalid_uoms)

        if errors:
            raise ValidationError(
                ". ".join(errors),
                ErrorCodes.CSV_INVALID_CATEGORY,
                details
            )

    @staticmethod
    def process_csv_row(
        row: Dict,
        row_num: int,
        categories_map: Dict[str, int],
        uoms_map: Dict[str, int]
    ) -> Dict[str, Any]:
        """
        Procesa una fila de CSV y retorna datos de producto normalizados.

        Args:
            row: Diccionario con datos de la fila
            row_num: Número de fila (para mensajes de error)
            categories_map: Dict de {nombre: id} de categorías
            uoms_map: Dict de {nombre: id} de UoMs

        Returns:
            Diccionario con datos de producto validados y normalizados

        Raises:
            ValidationError: Si la fila tiene datos inválidos
        """
        sku = row.get('sku', '').strip()
        name = row.get('name', '').strip()

        if not sku or not name:
            raise ValidationError(
                f"Fila {row_num}: SKU y nombre son obligatorios",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "sku": sku, "name": name}
            )

        try:
            # Usar los validadores existentes
            validated_sku = ProductService.normalize_sku(sku)
            validated_name = ProductService.normalize_name(name)
            validated_price = ProductService.validate_price(row.get('standard_price', '0'))
            validated_tracking = ProductService.validate_tracking(row.get('tracking', 'none'))
            validated_ownership = ProductService.validate_ownership(row.get('ownership', 'owned'))

            category_name = row.get('category_name', '').strip()
            uom_name = row.get('uom_name', '').strip()

            return {
                "sku": validated_sku,
                "name": validated_name,
                "category_id": categories_map.get(category_name),
                "uom_id": uoms_map.get(uom_name),
                "tracking": validated_tracking,
                "ownership": validated_ownership,
                "standard_price": validated_price
            }

        except ValidationError as e:
            # Re-lanzar con contexto de fila
            raise ValidationError(
                f"Fila {row_num}: {e.message}",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, **e.details}
            )

    @staticmethod
    def generate_csv_content(products: List[Dict], delimiter: str = ';') -> str:
        """
        Genera contenido CSV a partir de lista de productos.

        Args:
            products: Lista de diccionarios de productos
            delimiter: Delimitador CSV (default ';' para Excel español)

        Returns:
            String con contenido CSV
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=delimiter)

        # Headers
        writer.writerow(ProductService.EXPORT_CSV_HEADERS)

        # Data
        for prod in products:
            prod_dict = dict(prod) if not isinstance(prod, dict) else prod
            writer.writerow([prod_dict.get(h, '') for h in ProductService.EXPORT_CSV_HEADERS])

        return output.getvalue()

    # =========================================================================
    # SKU TEXT PARSING LOGIC
    # =========================================================================

    @staticmethod
    def parse_sku_text(raw_text: str) -> Tuple[Dict[str, float], List[str]]:
        """
        Parsea texto con SKUs y cantidades en formato "SKU*QTY" o solo "SKU".

        Args:
            raw_text: Texto crudo con SKUs, uno por línea

        Returns:
            Tupla (dict de {sku_lowercase: cantidad_total}, lista de errores)

        Examples:
            "ABC123*5" -> {"abc123": 5.0}
            "ABC123" -> {"abc123": 1.0}
            "ABC123*2\\nABC123*3" -> {"abc123": 5.0}  # Se agregan cantidades
        """
        parsed_lines: Dict[str, float] = {}
        errors: List[str] = []

        lines = raw_text.strip().split('\n')

        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            sku = ""
            qty_str = "1"

            if '*' in line:
                parts = line.split('*')
                if len(parts) == 2:
                    sku = parts[0].strip().lower()
                    qty_str = parts[1].strip().replace(',', '.')
                else:
                    errors.append(f"Línea {i+1}: Formato inválido (demasiados '*').")
                    continue
            else:
                sku = line.lower()

            if not sku:
                errors.append(f"Línea {i+1}: SKU vacío.")
                continue

            try:
                quantity = float(qty_str)
                if quantity <= 0:
                    errors.append(f"Línea {i+1}: Cantidad debe ser positiva.")
                    continue
            except (ValueError, TypeError):
                errors.append(f"Línea {i+1}: Cantidad '{qty_str}' no es un número.")
                continue

            # Agregar cantidades para SKUs duplicados
            parsed_lines[sku] = parsed_lines.get(sku, 0) + quantity

        return parsed_lines, errors

    @staticmethod
    def build_sku_import_response(
        parsed_skus: Dict[str, float],
        found_products: List[Dict]
    ) -> Tuple[List[Dict], List[str]]:
        """
        Construye respuesta de importación SKU combinando productos encontrados con cantidades.

        Args:
            parsed_skus: Dict de {sku_lowercase: cantidad}
            found_products: Lista de productos encontrados en BD

        Returns:
            Tupla (lista de {product: {...}, quantity: float}, lista de errores)
        """
        # Crear mapa de productos por SKU lowercase
        found_map = {row['sku'].lower(): row for row in found_products}

        final_list = []
        errors = []

        for sku_lower, total_qty in parsed_skus.items():
            product_data = found_map.get(sku_lower)

            if product_data:
                final_list.append({
                    "product": dict(product_data),
                    "quantity": total_qty
                })
            else:
                errors.append(f"SKU '{sku_lower}' no encontrado, inactivo o no almacenable.")

        return final_list, errors
