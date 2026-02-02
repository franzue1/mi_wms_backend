# app/services/adjustment_service.py
"""
Service Layer para Ajustes de Inventario (Conteos Cíclicos).
Contiene lógica de negocio para validaciones de ajuste, cálculo de diferencias
físico vs teórico, y procesamiento de importaciones masivas.

Usa PickingService para validación de datos y generación de movimientos.
"""

from typing import Optional, Dict, List, Any, Tuple
from decimal import Decimal
from datetime import datetime
import io
import csv
import re

from app.exceptions import (
    ValidationError,
    NotFoundError,
    BusinessRuleError,
    ErrorCodes
)
from app.services.picking_service import PickingService


class AdjustmentService:
    """
    Servicio de lógica de negocio para Ajustes de Inventario.
    Maneja conteos cíclicos, diferencias de inventario y ajustes masivos.
    """

    # ==========================================================================
    # CONSTANTES - TIPOS DE AJUSTE
    # ==========================================================================

    REASON_CONTEO_CICLICO = "Conteo Cíclico"
    REASON_DIFERENCIA = "Diferencia de Inventario"
    REASON_DANO = "Producto Dañado"
    REASON_VENCIDO = "Producto Vencido"
    REASON_OTRO = "Otro"

    VALID_REASONS = {
        REASON_CONTEO_CICLICO,
        REASON_DIFERENCIA,
        REASON_DANO,
        REASON_VENCIDO,
        REASON_OTRO,
    }

    # ==========================================================================
    # CONSTANTES - CSV HEADERS
    # ==========================================================================

    IMPORT_CSV_HEADERS = {
        'sku', 'cantidad', 'ubicacion'  # Mínimos requeridos
    }

    OPTIONAL_CSV_HEADERS = {
        'referencia', 'razon', 'notas', 'costo',
        'series', 'serie', 'serial', 'lote'
    }

    EXPORT_CSV_HEADERS = [
        'referencia', 'razon', 'fecha', 'usuario', 'notas', 'estado',
        'ubicacion', 'sku', 'producto', 'cantidad', 'costo_unitario', 'series'
    ]

    # ==========================================================================
    # VALIDACIÓN DE DATOS DE AJUSTE
    # ==========================================================================

    @staticmethod
    def validate_adjustment_reason(reason: str) -> str:
        """
        Valida y normaliza la razón del ajuste.

        Args:
            reason: Razón del ajuste

        Returns:
            str: Razón normalizada

        Raises:
            ValidationError: Si la razón es inválida
        """
        if not reason:
            raise ValidationError(
                "La razón del ajuste es obligatoria",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "adjustment_reason"}
            )

        reason_stripped = reason.strip()

        # Permitimos razones personalizadas (Otro)
        if reason_stripped in AdjustmentService.VALID_REASONS:
            return reason_stripped

        # Si no está en las predefinidas, lo aceptamos como "otro"
        return reason_stripped

    @staticmethod
    def validate_adjustment_quantity(quantity: float, product_name: str) -> None:
        """
        Valida que la cantidad del ajuste sea válida (puede ser positiva o negativa).

        Args:
            quantity: Cantidad del ajuste
            product_name: Nombre del producto (para mensaje de error)

        Raises:
            ValidationError: Si la cantidad es cero
        """
        if quantity == 0:
            raise ValidationError(
                f"La cantidad del ajuste para '{product_name}' no puede ser cero",
                ErrorCodes.ADJUSTMENT_INVALID_QUANTITY,
                {"product": product_name}
            )

    @staticmethod
    def validate_physical_location(location_id: Optional[int]) -> None:
        """
        Valida que se haya seleccionado una ubicación física.

        Args:
            location_id: ID de la ubicación física

        Raises:
            ValidationError: Si no hay ubicación seleccionada
        """
        if not location_id:
            raise ValidationError(
                "Debe seleccionar una ubicación física afectada",
                ErrorCodes.ADJUSTMENT_PHYSICAL_LOCATION_MISSING,
                {"field": "location_dest_id"}
            )

    # ==========================================================================
    # CÁLCULO DE DIFERENCIAS (FÍSICO VS TEÓRICO)
    # ==========================================================================

    @staticmethod
    def calculate_adjustment_difference(
        theoretical_qty: float,
        physical_qty: float
    ) -> Tuple[float, str]:
        """
        Calcula la diferencia entre stock teórico y físico.

        Args:
            theoretical_qty: Stock teórico (lo que dice el sistema)
            physical_qty: Stock físico (lo contado)

        Returns:
            Tuple[float, str]: (diferencia, descripción)
            - Si physical > theoretical: diferencia positiva (entrada)
            - Si physical < theoretical: diferencia negativa (salida)
        """
        difference = physical_qty - theoretical_qty

        if difference > 0:
            description = f"Sobrante de {difference:.2f} unidades"
        elif difference < 0:
            description = f"Faltante de {abs(difference):.2f} unidades"
        else:
            description = "Sin diferencia"

        return difference, description

    @staticmethod
    def determine_adjustment_direction(quantity: float) -> Tuple[str, str]:
        """
        Determina la dirección del ajuste basándose en la cantidad.

        Args:
            quantity: Cantidad del ajuste (positiva para entrada, negativa para salida)

        Returns:
            Tuple[str, str]: (tipo, descripción)
            - "entrada": Stock entra al inventario
            - "salida": Stock sale del inventario
        """
        if quantity >= 0:
            return "entrada", "Ingreso de stock al inventario"
        else:
            return "salida", "Retiro de stock del inventario"

    @staticmethod
    def calculate_move_locations(
        virtual_location_id: int,
        physical_location_id: int,
        quantity: float
    ) -> Tuple[int, int]:
        """
        Calcula las ubicaciones origen y destino del movimiento basándose
        en la cantidad del ajuste.

        Para ajustes:
        - Positivo (entrada): Virtual → Físico
        - Negativo (salida): Físico → Virtual

        Args:
            virtual_location_id: ID de la ubicación virtual de ajuste
            physical_location_id: ID de la ubicación física afectada
            quantity: Cantidad del ajuste

        Returns:
            Tuple[int, int]: (location_src_id, location_dest_id)
        """
        if quantity >= 0:
            # Entrada: Virtual → Físico
            return virtual_location_id, physical_location_id
        else:
            # Salida: Físico → Virtual
            return physical_location_id, virtual_location_id

    # ==========================================================================
    # VALIDACIÓN DE STOCK PARA AJUSTES NEGATIVOS
    # ==========================================================================

    @staticmethod
    def validate_stock_for_negative_adjustment(
        product_name: str,
        adjustment_qty: float,
        current_physical_qty: float
    ) -> Optional[str]:
        """
        Valida que haya suficiente stock para un ajuste negativo.

        Args:
            product_name: Nombre del producto
            adjustment_qty: Cantidad del ajuste (siempre negativa aquí)
            current_physical_qty: Stock físico actual

        Returns:
            Optional[str]: Mensaje de error si no hay suficiente stock, None si OK
        """
        # Usamos la validación de PickingService
        return PickingService.validate_adjustment_stock(
            product_name,
            adjustment_qty,
            current_physical_qty
        )

    # ==========================================================================
    # PROCESAMIENTO DE CSV IMPORT
    # ==========================================================================

    @staticmethod
    def parse_adjustment_csv(content: bytes) -> Tuple[List[Dict[str, str]], List[str]]:
        """
        Parsea el contenido de un CSV de ajustes.
        Reutiliza el parser de PickingService.

        Args:
            content: Contenido del archivo en bytes

        Returns:
            Tuple[List[Dict], List[str]]: (filas parseadas, headers)
        """
        return PickingService.parse_csv_file(content)

    @staticmethod
    def validate_adjustment_csv_headers(headers: List[str]) -> None:
        """
        Valida que el CSV tenga los headers requeridos.

        Args:
            headers: Lista de headers encontrados

        Raises:
            ValidationError: Si faltan headers requeridos
        """
        headers_set = {h.lower().strip() for h in headers}
        missing = AdjustmentService.IMPORT_CSV_HEADERS - headers_set

        if missing:
            raise ValidationError(
                f"Faltan columnas obligatorias: {', '.join(sorted(missing))}",
                ErrorCodes.CSV_MISSING_HEADERS,
                {"missing_headers": list(missing)}
            )

    @staticmethod
    def normalize_csv_row(row: Dict[str, str]) -> Dict[str, str]:
        """
        Normaliza una fila del CSV (lowercase keys, strip values).

        Args:
            row: Fila del CSV

        Returns:
            Dict[str, str]: Fila normalizada
        """
        return {
            k.lower().strip(): (v.strip() if v else '')
            for k, v in row.items() if k
        }

    @staticmethod
    def validate_adjustment_csv_row(
        row: Dict[str, str],
        row_num: int
    ) -> Dict[str, Any]:
        """
        Valida y extrae datos de una fila de ajuste.
        Usa el validador de PickingService con ajustes específicos.

        Args:
            row: Fila del CSV normalizada
            row_num: Número de fila (para mensajes de error)

        Returns:
            Dict[str, Any]: Datos validados y normalizados

        Raises:
            ValidationError: Si la fila tiene datos inválidos
        """
        # Usamos el validador base de PickingService pero sin requerir razón
        # (la razón puede venir del grupo/referencia)
        return PickingService.validate_adjustment_row(
            row,
            row_num,
            require_reason=False
        )

    @staticmethod
    def group_rows_by_reference(
        rows: List[Dict[str, str]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Agrupa las filas del CSV por referencia para crear documentos separados.

        Args:
            rows: Lista de filas del CSV

        Returns:
            Dict[str, List]: Diccionario {referencia: [filas]}
        """
        from collections import defaultdict

        grouped = defaultdict(list)
        timestamp = datetime.now().strftime('%Y%m%d-%H%M')

        for i, row in enumerate(rows):
            normalized = AdjustmentService.normalize_csv_row(row)
            ref = normalized.get('referencia') or f"IMP-{timestamp}"

            grouped[ref].append({
                'data': normalized,
                'line': i + 2  # +2 por header y base 1
            })

        return dict(grouped)

    @staticmethod
    def parse_serials_from_row(row: Dict[str, str]) -> List[str]:
        """
        Extrae las series de una fila del CSV.
        Busca en múltiples campos posibles.

        Args:
            row: Fila del CSV normalizada

        Returns:
            List[str]: Lista de series encontradas
        """
        # Buscar en campos conocidos
        serials_str = (
            row.get('series') or
            row.get('serie') or
            row.get('serial') or
            row.get('lote') or
            ''
        )

        return PickingService.parse_serials_string(serials_str)

    # ==========================================================================
    # GENERACIÓN DE CSV EXPORT
    # ==========================================================================

    @staticmethod
    def generate_export_csv_content(data: List[Dict[str, Any]]) -> str:
        """
        Genera el contenido CSV para exportación de ajustes.
        Reutiliza el generador de PickingService.

        Args:
            data: Lista de diccionarios con datos de ajustes

        Returns:
            str: Contenido CSV como string
        """
        return PickingService.generate_adjustments_csv_content(data)

    # ==========================================================================
    # PREPARACIÓN DE DATOS PARA MOVIMIENTOS
    # ==========================================================================

    @staticmethod
    def prepare_adjustment_move_data(
        product_id: int,
        quantity: float,
        virtual_location_id: int,
        physical_location_id: int,
        cost: float = 0
    ) -> Dict[str, Any]:
        """
        Prepara los datos para crear un movimiento de ajuste.

        Args:
            product_id: ID del producto
            quantity: Cantidad (positiva o negativa)
            virtual_location_id: ID de ubicación virtual
            physical_location_id: ID de ubicación física
            cost: Costo unitario

        Returns:
            Dict[str, Any]: Datos del movimiento listos para inserción
        """
        # Calcular dirección del movimiento
        src_id, dest_id = AdjustmentService.calculate_move_locations(
            virtual_location_id,
            physical_location_id,
            quantity
        )

        return {
            'product_id': product_id,
            'product_uom_qty': abs(quantity),
            'quantity_done': abs(quantity),
            'location_src_id': src_id,
            'location_dest_id': dest_id,
            'price_unit': cost,
            'cost_at_adjustment': cost,
            'state': PickingService.STATE_DRAFT
        }

    @staticmethod
    def build_adjustment_header_prefix(company_id: int) -> str:
        """
        Construye el prefijo para el nombre del ajuste.

        Args:
            company_id: ID de la empresa

        Returns:
            str: Prefijo (ej: "C1/ADJ/")
        """
        return PickingService.build_picking_prefix(company_id, PickingService.TYPE_ADJ)

    @staticmethod
    def generate_adjustment_name(prefix: str, sequence: int) -> str:
        """
        Genera el nombre del ajuste.

        Args:
            prefix: Prefijo del nombre
            sequence: Número de secuencia

        Returns:
            str: Nombre completo (ej: "C1/ADJ/00001")
        """
        return PickingService.generate_picking_name(prefix, sequence)

    # ==========================================================================
    # VALIDACIÓN DE SERIES PARA AJUSTES
    # ==========================================================================

    @staticmethod
    def validate_serials_for_adjustment(
        tracking_type: str,
        serials: List[str],
        quantity: float,
        sku: str,
        row_num: int
    ) -> None:
        """
        Valida las series para un ajuste.

        Args:
            tracking_type: Tipo de tracking del producto ('serial', 'lot', 'none')
            serials: Lista de series proporcionadas
            quantity: Cantidad del ajuste
            sku: SKU del producto (para mensajes de error)
            row_num: Número de fila (para mensajes de error)

        Raises:
            ValidationError: Si hay problemas con las series
        """
        if tracking_type == 'none':
            return  # No requiere series

        if not serials:
            return  # Series opcionales en ajustes

        qty_abs = abs(quantity)

        if tracking_type == 'serial':
            # Para serial: debe haber exactamente una serie por unidad
            if len(serials) != int(qty_abs):
                raise ValidationError(
                    f"Fila {row_num}: SKU '{sku}' requiere {int(qty_abs)} series, "
                    f"se indicaron {len(serials)}.",
                    ErrorCodes.TRACKING_MISMATCH,
                    {
                        "row": row_num,
                        "sku": sku,
                        "expected": int(qty_abs),
                        "provided": len(serials)
                    }
                )

            # Normalizar y validar cada serie
            for serial in serials:
                PickingService.normalize_serial_name(serial)

        elif tracking_type == 'lot':
            # Para lote: puede ser un lote con toda la cantidad o varios
            for serial in serials:
                PickingService.normalize_serial_name(serial)

    # ==========================================================================
    # CONSTANTES - ESTADOS DE AJUSTE
    # ==========================================================================

    # Reutilizamos los estados de PickingService
    STATE_DRAFT = PickingService.STATE_DRAFT
    STATE_READY = PickingService.STATE_READY
    STATE_DONE = PickingService.STATE_DONE
    STATE_CANCELLED = PickingService.STATE_CANCELLED

    @staticmethod
    def can_edit_adjustment(state: str) -> bool:
        """
        Verifica si un ajuste puede ser editado.

        Args:
            state: Estado actual del ajuste

        Returns:
            bool: True si puede editarse
        """
        return state == AdjustmentService.STATE_DRAFT

    @staticmethod
    def can_validate_adjustment(state: str) -> bool:
        """
        Verifica si un ajuste puede ser validado.

        Args:
            state: Estado actual del ajuste

        Returns:
            bool: True si puede validarse
        """
        return state == AdjustmentService.STATE_READY
