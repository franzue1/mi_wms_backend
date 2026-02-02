# app/services/picking_service.py
"""
Service Layer para Pickings (Operaciones/Albaranes).
Contiene lógica de negocio, validación de estados y reglas de stock.
El repositorio solo ejecuta SQL puro.
"""

from typing import Optional, Dict, List, Any, Tuple, Set
from datetime import datetime
from decimal import Decimal
import re
import io
import csv

from app.exceptions import (
    ValidationError,
    NotFoundError,
    BusinessRuleError,
    ErrorCodes
)


class PickingService:
    """
    Servicio de lógica de negocio para Pickings.
    Maneja validación de estados, reglas de ownership, cálculos de stock y CSV.
    """

    # ==========================================================================
    # CONSTANTES - ESTADOS
    # ==========================================================================

    # Estados válidos del picking
    STATE_DRAFT = 'draft'
    STATE_READY = 'listo'
    STATE_DONE = 'done'
    STATE_CANCELLED = 'cancelled'

    VALID_STATES = {STATE_DRAFT, STATE_READY, STATE_DONE, STATE_CANCELLED}

    # Transiciones permitidas
    ALLOWED_TRANSITIONS = {
        STATE_DRAFT: {STATE_READY, STATE_CANCELLED},
        STATE_READY: {STATE_DRAFT, STATE_DONE, STATE_CANCELLED},
        STATE_DONE: set(),  # Estado final, no se puede cambiar
        STATE_CANCELLED: {STATE_DRAFT},  # Solo puede volver a borrador
    }

    # ==========================================================================
    # CONSTANTES - TIPOS DE OPERACIÓN
    # ==========================================================================

    # Códigos de tipo de picking
    TYPE_IN = 'IN'       # Entrada (Compras, Recepciones)
    TYPE_OUT = 'OUT'     # Salida (Ventas, Entregas)
    TYPE_INT = 'INT'     # Interno (Transferencias)
    TYPE_ADJ = 'ADJ'     # Ajuste de Inventario

    VALID_PICKING_TYPES = {TYPE_IN, TYPE_OUT, TYPE_INT, TYPE_ADJ}

    # ==========================================================================
    # CONSTANTES - REGLAS DE OWNERSHIP (Propiedad del Material)
    # ==========================================================================

    OWNERSHIP_OWNED = 'owned'
    OWNERSHIP_CONSIGNED = 'consigned'

    # Reglas de negocio: qué ownership permite cada tipo de operación
    # {operation_name: allowed_ownership}
    IMPORT_LOGIC_RULES = {
        "Compra Nacional": OWNERSHIP_OWNED,
        "Consignación Recibida": OWNERSHIP_CONSIGNED,
        "Devolución a Proveedor": OWNERSHIP_OWNED,
        "Devolución a Cliente": OWNERSHIP_CONSIGNED,
    }

    # ==========================================================================
    # CONSTANTES - TRACKING
    # ==========================================================================

    TRACKING_NONE = 'none'
    TRACKING_LOT = 'lot'
    TRACKING_SERIAL = 'serial'

    VALID_TRACKING_TYPES = {TRACKING_NONE, TRACKING_LOT, TRACKING_SERIAL}

    # ==========================================================================
    # CONSTANTES - CSV HEADERS
    # ==========================================================================

    # Headers para exportación de operaciones
    EXPORT_CSV_HEADERS = [
        'picking_name', 'picking_type_code', 'state', 'custom_operation_type',
        'project_name', 'almacen_origen', 'ubicacion_origen',
        'almacen_destino', 'ubicacion_destino',
        'partner_ref', 'purchase_order', 'date_transfer', 'responsible_user',
        'employee_name', 'operations_instructions', 'warehouse_observations',
        'comentarios', 'product_sku', 'product_name', 'quantity', 'price_unit', 'serial'
    ]

    # Headers para importación de ajustes
    ADJUSTMENT_CSV_HEADERS = [
        'referencia', 'razon', 'ubicacion', 'sku', 'cantidad', 'costo', 'series'
    ]

    REQUIRED_ADJUSTMENT_HEADERS = {'razon', 'ubicacion', 'sku', 'cantidad'}

    # ==========================================================================
    # CONSTANTES - CAMPOS PERMITIDOS PARA UPDATE
    # ==========================================================================

    ALLOWED_PICKING_FIELDS_TO_UPDATE = {
        'partner_id', 'partner_ref', 'purchase_order', 'date_transfer',
        'custom_operation_type', 'project_id', 'scheduled_date', 'notes',
        'location_src_id', 'location_dest_id', 'employee_id',
        'adjustment_reason', 'loss_confirmation',
        'operations_instructions', 'warehouse_observations'
    }

    # ==========================================================================
    # VALIDACIÓN DE ESTADOS Y TRANSICIONES
    # ==========================================================================

    @staticmethod
    def validate_state_transition(current_state: str, new_state: str) -> bool:
        """
        Valida si una transición de estado es permitida.

        Args:
            current_state: Estado actual del picking
            new_state: Estado al que se quiere transicionar

        Returns:
            bool: True si la transición es válida

        Raises:
            ValidationError: Si la transición no está permitida
        """
        if current_state not in PickingService.VALID_STATES:
            raise ValidationError(
                f"Estado actual '{current_state}' no es válido",
                ErrorCodes.INVALID_STATE,
                {"current_state": current_state}
            )

        if new_state not in PickingService.VALID_STATES:
            raise ValidationError(
                f"Estado destino '{new_state}' no es válido",
                ErrorCodes.INVALID_STATE,
                {"new_state": new_state}
            )

        allowed = PickingService.ALLOWED_TRANSITIONS.get(current_state, set())

        if new_state not in allowed:
            raise BusinessRuleError(
                f"Transición no permitida: '{current_state}' → '{new_state}'",
                "PICK_INVALID_TRANSITION",
                {"current_state": current_state, "new_state": new_state}
            )

        return True

    @staticmethod
    def can_mark_as_ready(picking_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Verifica si un picking puede marcarse como 'listo'.

        Args:
            picking_data: Diccionario con datos del picking

        Returns:
            Tuple[bool, Optional[str]]: (puede_marcar, mensaje_error)
        """
        state = picking_data.get('state')

        if state == PickingService.STATE_DONE:
            return False, "El documento ya está validado (done)."

        if state == PickingService.STATE_CANCELLED:
            return False, "El documento está cancelado."

        if state == PickingService.STATE_READY:
            return True, None  # Ya está listo

        return True, None

    @staticmethod
    def can_validate(picking_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Verifica si un picking puede validarse (pasar a 'done').

        Args:
            picking_data: Diccionario con datos del picking

        Returns:
            Tuple[bool, Optional[str]]: (puede_validar, mensaje_error)
        """
        state = picking_data.get('state')

        if state == PickingService.STATE_DONE:
            return False, "TRANQUILO: Este documento ya fue validado exitosamente."

        if state == PickingService.STATE_CANCELLED:
            return False, "El documento está cancelado."

        if state == PickingService.STATE_DRAFT:
            return False, "Debe marcar como 'Listo' antes de validar."

        return True, None

    @staticmethod
    def can_return_to_draft(picking_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Verifica si un picking puede regresar a borrador.

        Args:
            picking_data: Diccionario con datos del picking

        Returns:
            Tuple[bool, Optional[str]]: (puede_regresar, mensaje_error)
        """
        state = picking_data.get('state')

        if state == PickingService.STATE_DONE:
            return False, "Imposible regresar a borrador: El documento YA FUE VALIDADO."

        if state == PickingService.STATE_DRAFT:
            return True, "Ya está en borrador."

        return True, None

    # ==========================================================================
    # VALIDACIÓN DE INTEGRIDAD PARA TRANSICIÓN A 'LISTO'
    # ==========================================================================

    @staticmethod
    def validate_header_for_ready(picking_data: Dict[str, Any], type_code: str) -> None:
        """
        Auditoría COMPLETA del documento antes de permitir la transición a 'listo'.
        Recopila TODOS los errores y los devuelve en un solo mensaje.

        Args:
            picking_data: Diccionario con datos del picking
            type_code: Código del tipo (IN, OUT, INT, ADJ)

        Raises:
            ValidationError: Con lista completa de campos faltantes

        Note:
            - El campo 'remission_number' (Guía de Remisión) NO se exige aquí,
              solo se requiere al momento de VALIDAR (done).
            - Las validaciones son CONTEXTUALES por tipo de operación.
        """
        errors = []

        # =====================================================================
        # VALIDACIONES ESPECÍFICAS POR TIPO DE OPERACIÓN
        # (Cada tipo tiene su propia lógica de negocio)
        # =====================================================================

        if type_code == PickingService.TYPE_IN:
            # -----------------------------------------------------------------
            # RECEPCIONES (IN): Mercancía que ENTRA al almacén
            # Campos críticos: Proveedor, Almacén Destino, Fecha, Orden Compra
            # Nota: location_src_id es una ubicación virtual de proveedor,
            #       normalmente se auto-asigna y no es crítica para el usuario.
            # -----------------------------------------------------------------

            if not picking_data.get('partner_id'):
                errors.append("Proveedor (Contacto) es obligatorio")

            if not picking_data.get('location_dest_id'):
                errors.append("Almacén/Ubicación Destino es obligatorio")

            if not picking_data.get('scheduled_date'):
                errors.append("Fecha Programada es obligatoria")

            if not picking_data.get('date_transfer'):
                errors.append("Fecha de Traslado es obligatoria")

            if not picking_data.get('purchase_order'):
                errors.append("Orden de Compra es obligatoria")

        elif type_code == PickingService.TYPE_OUT:
            # -----------------------------------------------------------------
            # SALIDAS (OUT): Mercancía que SALE del almacén
            # Campos críticos: Ubicación Origen (de dónde sale), Fecha Traslado
            # -----------------------------------------------------------------

            if not picking_data.get('location_src_id'):
                errors.append("Ubicación Origen es obligatoria")

            if not picking_data.get('location_dest_id'):
                errors.append("Ubicación Destino es obligatoria")

            if not picking_data.get('date_transfer'):
                errors.append("Fecha de Traslado es obligatoria")

        elif type_code == PickingService.TYPE_INT:
            # -----------------------------------------------------------------
            # TRANSFERENCIAS (INT): Movimiento INTERNO entre ubicaciones
            # Campos críticos: Ambas ubicaciones, Fecha
            # Nota: La selección de proyecto es validada en el frontend.
            #       "Sin Proyecto / Stock General" se guarda como project_id = NULL.
            # -----------------------------------------------------------------

            if not picking_data.get('location_src_id'):
                errors.append("Ubicación Origen es obligatoria")

            if not picking_data.get('location_dest_id'):
                errors.append("Ubicación Destino es obligatoria")

            if not picking_data.get('date_transfer'):
                errors.append("Fecha de Traslado es obligatoria")

        elif type_code == PickingService.TYPE_ADJ:
            # -----------------------------------------------------------------
            # AJUSTES (ADJ): Correcciones de inventario
            # Campos críticos: Ubicación afectada, Razón, Fecha contable
            # -----------------------------------------------------------------

            if not picking_data.get('location_src_id'):
                errors.append("Ubicación es obligatoria")

            if not picking_data.get('adjustment_reason'):
                errors.append("Razón del Ajuste es obligatoria")

            if not picking_data.get('scheduled_date'):
                errors.append("Fecha Contable es obligatoria")

        else:
            # Tipo desconocido - validación genérica mínima
            if not picking_data.get('location_src_id'):
                errors.append("Ubicación Origen es obligatoria")
            if not picking_data.get('location_dest_id'):
                errors.append("Ubicación Destino es obligatoria")

        # =====================================================================
        # REPORTE FINAL: Lanzar error con TODOS los campos faltantes
        # =====================================================================
        if errors:
            # Construir mensaje amigable con viñetas
            error_list = "\n".join(f"  • {e}" for e in errors)
            raise ValidationError(
                f"🛑 CAMPOS INCOMPLETOS PARA MARCAR COMO 'LISTO':\n\n"
                f"{error_list}\n\n"
                f"Complete todos los campos antes de continuar.",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"missing_fields": errors, "type_code": type_code}
            )

    # ==========================================================================
    # VALIDACIÓN DE REGLAS DE NEGOCIO
    # ==========================================================================

    @staticmethod
    def validate_ownership_for_operation(
        operation_name: str,
        ownership: str,
        product_name: str
    ) -> None:
        """
        Valida que el ownership del producto sea compatible con el tipo de operación.

        Args:
            operation_name: Nombre del tipo de operación (ej: "Compra Nacional")
            ownership: Tipo de propiedad del producto ('owned' o 'consigned')
            product_name: Nombre del producto (para mensaje de error)

        Raises:
            BusinessRuleError: Si el ownership no es compatible
        """
        if operation_name not in PickingService.IMPORT_LOGIC_RULES:
            return  # Operación no tiene regla específica

        required_ownership = PickingService.IMPORT_LOGIC_RULES[operation_name]
        actual_ownership = ownership or PickingService.OWNERSHIP_OWNED

        if actual_ownership != required_ownership:
            if operation_name == "Compra Nacional" and actual_ownership != PickingService.OWNERSHIP_OWNED:
                msg = f"No puedes comprar '{product_name}' porque es material Consignado."
            elif operation_name == "Consignación Recibida" and actual_ownership != PickingService.OWNERSHIP_CONSIGNED:
                msg = f"'{product_name}' es material Propio, no puedes recibirlo como Consignación."
            elif operation_name == "Devolución a Proveedor" and actual_ownership != PickingService.OWNERSHIP_OWNED:
                msg = f"No puedes devolver '{product_name}' a proveedor porque es Consignado (usa Dev. a Cliente)."
            elif operation_name == "Devolución a Cliente" and actual_ownership != PickingService.OWNERSHIP_CONSIGNED:
                msg = f"No puedes devolver '{product_name}' a cliente porque es Propio (usa Dev. a Proveedor)."
            else:
                msg = f"Producto '{product_name}' no compatible con operación '{operation_name}'."

            raise BusinessRuleError(
                f"Regla de Negocio: {msg}",
                "PICK_OWNERSHIP_MISMATCH",
                {"operation": operation_name, "ownership": actual_ownership, "product": product_name}
            )

    @staticmethod
    def validate_cuadrilla_rule(
        location_categories: List[str],
        employee_id: Optional[int]
    ) -> None:
        """
        Valida que operaciones con 'Cuadrilla Interna' tengan empleado responsable.

        Args:
            location_categories: Lista de categorías de las ubicaciones (origen/destino)
            employee_id: ID del empleado responsable

        Raises:
            BusinessRuleError: Si la operación requiere empleado y no tiene uno asignado
        """
        if 'CUADRILLA INTERNA' in location_categories:
            if not employee_id:
                raise BusinessRuleError(
                    "🛑 REGLA DE NEGOCIO:\n\nEsta operación involucra a una 'Cuadrilla Interna'.\n"
                    "Es OBLIGATORIO indicar el Empleado Responsable (Técnico/Chofer).",
                    "PICK_EMPLOYEE_REQUIRED",
                    {"reason": "cuadrilla_interna"}
                )

    @staticmethod
    def validate_picking_has_lines(moves_count: int) -> None:
        """
        Valida que el picking tenga al menos una línea (movimiento).

        Args:
            moves_count: Cantidad de movimientos/líneas

        Raises:
            ValidationError: Si el picking está vacío
        """
        if moves_count == 0:
            raise ValidationError(
                "El albarán está vacío. Agregue productos primero.",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "moves"}
            )

    # ==========================================================================
    # VALIDACIÓN DE SERIES/LOTES
    # ==========================================================================

    @staticmethod
    def normalize_serial_name(serial_name: str) -> str:
        """
        Normaliza el nombre de una serie/lote.

        Args:
            serial_name: Nombre a normalizar

        Returns:
            str: Nombre normalizado (mayúsculas, sin espacios)

        Raises:
            ValidationError: Si el nombre es inválido
        """
        if not serial_name:
            raise ValidationError(
                "El nombre de la serie/lote es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "serial_name"}
            )

        # Limpieza agresiva
        cleaned = str(serial_name).strip().replace(" ", "").upper()

        # Validación de longitud
        if len(cleaned) > 30:
            raise ValidationError(
                f"La serie '{cleaned[:15]}...' es demasiado larga (Máximo 30 caracteres).",
                "SERIAL_TOO_LONG",
                {"serial": cleaned[:15], "max_length": 30}
            )

        # Validación de caracteres (Whitelist)
        if not re.match(r'^[A-Z0-9\-_/\.]+$', cleaned):
            raise ValidationError(
                f"La serie {repr(cleaned)} contiene caracteres inválidos. "
                "Solo se permiten letras, números y guiones.",
                "SERIAL_INVALID_CHARS",
                {"serial": cleaned}
            )

        return cleaned

    @staticmethod
    def validate_serial_quantity(tracking_type: str, serial_name: str, quantity: float) -> None:
        """
        Valida que las series tengan cantidad 1.

        Args:
            tracking_type: Tipo de tracking ('serial', 'lot', 'none')
            serial_name: Nombre de la serie
            quantity: Cantidad asignada

        Raises:
            ValidationError: Si una serie tiene cantidad diferente a 1
        """
        if tracking_type == PickingService.TRACKING_SERIAL and quantity > 1:
            raise ValidationError(
                f"Error: La serie '{serial_name}' tiene cantidad {quantity}. Debe ser 1.",
                "SERIAL_QTY_INVALID",
                {"serial": serial_name, "quantity": quantity}
            )

    @staticmethod
    def validate_tracking_totals(
        product_name: str,
        expected_qty: float,
        tracking_qty: float
    ) -> None:
        """
        Valida que la cantidad de tracking coincida con la cantidad del movimiento.

        Args:
            product_name: Nombre del producto
            expected_qty: Cantidad esperada del movimiento
            tracking_qty: Suma de las cantidades de tracking

        Raises:
            ValidationError: Si las cantidades no coinciden
        """
        if abs(expected_qty - tracking_qty) > 0.001:
            raise ValidationError(
                f"Error en '{product_name}': Cantidad ({expected_qty}) vs Series ({tracking_qty}) no coinciden.",
                "TRACKING_QTY_MISMATCH",
                {"product": product_name, "expected": expected_qty, "tracking": tracking_qty}
            )

    @staticmethod
    def check_duplicate_serials_in_transaction(
        product_id: int,
        serial_name: str,
        processed_serials: Set[Tuple[int, str]]
    ) -> None:
        """
        Verifica que una serie no esté duplicada dentro de la misma transacción.

        Args:
            product_id: ID del producto
            serial_name: Nombre de la serie
            processed_serials: Set de (product_id, serial_name) ya procesados

        Raises:
            ValidationError: Si la serie ya fue procesada en esta transacción
        """
        key = (product_id, serial_name)
        if key in processed_serials:
            raise ValidationError(
                f"Serie duplicada en esta operación: {serial_name}",
                "SERIAL_DUPLICATE_IN_TX",
                {"serial": serial_name, "product_id": product_id}
            )

    # ==========================================================================
    # CÁLCULOS DE STOCK
    # ==========================================================================

    @staticmethod
    def calculate_available_stock(
        physical_qty: float,
        reserved_qty: float
    ) -> float:
        """
        Calcula el stock disponible real.

        Disponible = Físico - Reservado

        Args:
            physical_qty: Cantidad física en ubicación
            reserved_qty: Cantidad reservada por otros pickings en estado 'listo'

        Returns:
            float: Cantidad disponible (mínimo 0)
        """
        available = physical_qty - reserved_qty
        return max(0.0, available)

    @staticmethod
    def validate_stock_for_demand(
        product_name: str,
        qty_needed: float,
        available: float,
        physical: float,
        reserved: float
    ) -> Optional[str]:
        """
        Valida si hay suficiente stock para una demanda específica.

        Args:
            product_name: Nombre del producto
            qty_needed: Cantidad requerida
            available: Stock disponible
            physical: Stock físico
            reserved: Stock reservado

        Returns:
            Optional[str]: Mensaje de error si no hay suficiente, None si OK
        """
        if available < qty_needed:
            return (
                f"- {product_name}: Requerido {qty_needed} > Disponible {available} "
                f"(Físico: {physical} - Reservado Global: {reserved})"
            )
        return None

    @staticmethod
    def validate_adjustment_stock(
        product_name: str,
        qty_adjustment: float,
        physical_qty: float
    ) -> Optional[str]:
        """
        Valida stock para ajustes negativos.

        Args:
            product_name: Nombre del producto
            qty_adjustment: Cantidad del ajuste (negativa para salidas)
            physical_qty: Stock físico actual

        Returns:
            Optional[str]: Mensaje de error si no hay suficiente, None si OK
        """
        if qty_adjustment < 0 and physical_qty < abs(qty_adjustment):
            return f"- {product_name}: Físico {physical_qty} < Ajuste {abs(qty_adjustment)}"
        return None

    @staticmethod
    def calculate_weighted_average_cost(
        current_qty: float,
        current_price: float,
        incoming_qty: float,
        incoming_price: float
    ) -> float:
        """
        Calcula el nuevo precio promedio ponderado (WAC).

        Args:
            current_qty: Cantidad actual en stock
            current_price: Precio actual del producto
            incoming_qty: Cantidad entrante
            incoming_price: Precio de compra de lo entrante

        Returns:
            float: Nuevo precio promedio
        """
        if incoming_qty <= 0 or incoming_price < 0:
            return current_price

        # Protección contra stocks negativos teóricos
        safe_current_qty = max(0.0, current_qty)

        new_total_qty = safe_current_qty + incoming_qty
        total_value = (safe_current_qty * current_price) + (incoming_qty * incoming_price)

        if new_total_qty > 0:
            return total_value / new_total_qty
        return incoming_price

    # ==========================================================================
    # PREPARACIÓN DE DATOS
    # ==========================================================================

    @staticmethod
    def prepare_picking_header_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara y filtra los datos del header del picking para actualización.
        Solo permite campos de la whitelist.

        Args:
            data: Diccionario con datos a actualizar

        Returns:
            Dict[str, Any]: Diccionario filtrado con solo campos permitidos
        """
        return {
            k: v for k, v in data.items()
            if k in PickingService.ALLOWED_PICKING_FIELDS_TO_UPDATE
        }

    @staticmethod
    def prepare_move_data(
        move: Dict[str, Any],
        header_location_src: Optional[int],
        header_location_dest: Optional[int],
        header_partner_id: Optional[int],
        header_project_id: Optional[int]
    ) -> Dict[str, Any]:
        """
        Prepara los datos de un movimiento/línea.

        Args:
            move: Datos del movimiento
            header_location_src: Ubicación origen del header
            header_location_dest: Ubicación destino del header
            header_partner_id: Partner del header
            header_project_id: Proyecto del header

        Returns:
            Dict[str, Any]: Datos normalizados del movimiento
        """
        qty = float(move.get('quantity', 0))

        return {
            'product_id': move.get('product_id'),
            'product_uom_qty': qty,
            'quantity_done': qty,
            'location_src_id': move.get('location_src_id') or header_location_src,
            'location_dest_id': move.get('location_dest_id') or header_location_dest,
            'price_unit': move.get('price_unit', 0),
            'partner_id': header_partner_id,
            'project_id': header_project_id,
            'state': PickingService.STATE_DRAFT,
        }

    @staticmethod
    def build_picking_filter_dict(
        state: Optional[str] = None,
        name: Optional[str] = None,
        partner_ref: Optional[str] = None,
        custom_operation_type: Optional[str] = None,
        purchase_order: Optional[str] = None,
        responsible_user: Optional[str] = None,
        project_name: Optional[str] = None,
        employee_name: Optional[str] = None,
        date_transfer_from: Optional[str] = None,
        date_transfer_to: Optional[str] = None,
        src_path_display: Optional[str] = None,
        dest_path_display: Optional[str] = None,
        warehouse_src_name: Optional[str] = None,
        warehouse_dest_name: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Construye un diccionario de filtros limpio para la consulta de pickings.

        Returns:
            Dict[str, str]: Diccionario de filtros sin valores vacíos
        """
        filters = {
            'p.state': state,
            'p.name': name,
            'p.partner_ref': partner_ref,
            'p.custom_operation_type': custom_operation_type,
            'p.purchase_order': purchase_order,
            'p.responsible_user': responsible_user,
            'project_name': project_name,
            'employee_name': employee_name,
            'date_transfer_from': date_transfer_from,
            'date_transfer_to': date_transfer_to,
            'src_path_display': src_path_display,
            'dest_path_display': dest_path_display,
            'w_src.name': warehouse_src_name,
            'w_dest.name': warehouse_dest_name,
        }
        return {k: v for k, v in filters.items() if v is not None and v != ""}

    # ==========================================================================
    # GENERACIÓN DE SECUENCIAS
    # ==========================================================================

    @staticmethod
    def generate_picking_name(prefix: str, current_sequence: int) -> str:
        """
        Genera el nombre del picking con formato estándar.

        Args:
            prefix: Prefijo (ej: "C1/IN/")
            current_sequence: Número de secuencia actual

        Returns:
            str: Nombre generado (ej: "C1/IN/00001")
        """
        return f"{prefix}{str(current_sequence).zfill(5)}"

    @staticmethod
    def build_picking_prefix(company_id: int, type_code: str) -> str:
        """
        Construye el prefijo para el nombre del picking.

        Args:
            company_id: ID de la empresa
            type_code: Código del tipo (IN, OUT, INT, ADJ)

        Returns:
            str: Prefijo (ej: "C1/IN/")
        """
        return f"C{company_id}/{type_code}/"

    @staticmethod
    def build_remission_number(sequence: int) -> str:
        """
        Construye el número de guía de remisión.

        Args:
            sequence: Número de secuencia

        Returns:
            str: Número de remisión (ej: "GR-00001")
        """
        return f"GR-{str(sequence).zfill(5)}"

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

            headers = [h.lower().strip() for h in reader.fieldnames or []]
            return rows, headers

        except UnicodeDecodeError:
            raise ValidationError(
                "Error de codificación. Use UTF-8.",
                ErrorCodes.CSV_ENCODING_ERROR
            )

    @staticmethod
    def validate_adjustment_csv_headers(headers: List[str]) -> None:
        """
        Valida que el CSV de ajustes tenga los headers requeridos.

        Args:
            headers: Lista de headers encontrados

        Raises:
            ValidationError: Si faltan headers requeridos
        """
        headers_set = {h.lower().strip() for h in headers}
        missing = PickingService.REQUIRED_ADJUSTMENT_HEADERS - headers_set

        if missing:
            raise ValidationError(
                f"Faltan columnas obligatorias: {', '.join(sorted(missing))}",
                ErrorCodes.CSV_MISSING_HEADERS,
                {"missing_headers": list(missing)}
            )

    @staticmethod
    def parse_serials_string(serials_str: str) -> List[str]:
        """
        Parsea una cadena de series separadas por comas, punto y coma o saltos de línea.

        Args:
            serials_str: Cadena con series separadas

        Returns:
            List[str]: Lista de series limpias
        """
        if not serials_str:
            return []
        return [s.strip() for s in re.split(r'[;,\n]', serials_str) if s.strip()]

    @staticmethod
    def generate_operations_csv_content(
        data: List[Dict[str, Any]],
        headers: Optional[List[str]] = None
    ) -> str:
        """
        Genera contenido CSV a partir de una lista de operaciones.

        Args:
            data: Lista de diccionarios de operaciones
            headers: Headers opcionales (usa EXPORT_CSV_HEADERS por defecto)

        Returns:
            str: Contenido CSV como string
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        csv_headers = headers or PickingService.EXPORT_CSV_HEADERS
        writer.writerow(csv_headers)

        for row in data:
            row_dict = dict(row) if hasattr(row, 'keys') else row
            writer.writerow([row_dict.get(h, '') for h in csv_headers])

        return output.getvalue()

    @staticmethod
    def generate_adjustments_csv_content(data: List[Dict[str, Any]]) -> str:
        """
        Genera contenido CSV para exportación de ajustes.

        Args:
            data: Lista de diccionarios de ajustes

        Returns:
            str: Contenido CSV como string
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        headers = [
            'referencia', 'razon', 'fecha', 'usuario', 'notas', 'estado',
            'ubicacion', 'sku', 'producto', 'cantidad', 'costo_unitario', 'series'
        ]
        writer.writerow(headers)

        for row in data:
            row_dict = dict(row) if hasattr(row, 'keys') else row
            writer.writerow([
                row_dict.get('referencia', ''),
                row_dict.get('razon', ''),
                row_dict.get('fecha', ''),
                row_dict.get('usuario', ''),
                row_dict.get('notas', ''),
                row_dict.get('estado', ''),
                row_dict.get('ubicacion', ''),
                row_dict.get('sku', ''),
                row_dict.get('producto', ''),
                row_dict.get('cantidad', ''),
                row_dict.get('costo_unitario', ''),
                row_dict.get('series', ''),
            ])

        return output.getvalue()

    # ==========================================================================
    # HELPERS DE VALIDACIÓN PARA IMPORTACIÓN
    # ==========================================================================

    @staticmethod
    def validate_adjustment_row(
        row: Dict[str, str],
        row_num: int,
        require_reason: bool = True
    ) -> Dict[str, Any]:
        """
        Valida y normaliza una fila de ajuste de inventario.

        Args:
            row: Fila del CSV
            row_num: Número de fila (para mensajes de error)
            require_reason: Si la razón es obligatoria

        Returns:
            Dict[str, Any]: Datos normalizados

        Raises:
            ValidationError: Si la fila tiene datos inválidos
        """
        sku = row.get('sku', '').strip()
        qty_str = row.get('cantidad', '').strip()
        loc_path = row.get('ubicacion', '').strip()
        reason = row.get('razon', '').strip()

        if not sku:
            raise ValidationError(
                f"Fila {row_num}: SKU es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "sku"}
            )

        if not qty_str:
            raise ValidationError(
                f"Fila {row_num}: Cantidad es obligatoria",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "cantidad"}
            )

        if not loc_path:
            raise ValidationError(
                f"Fila {row_num}: Ubicación es obligatoria",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "ubicacion"}
            )

        if require_reason and not reason:
            raise ValidationError(
                f"Fila {row_num}: La 'razon' es OBLIGATORIA",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "razon"}
            )

        try:
            qty = float(qty_str)
        except ValueError:
            raise ValidationError(
                f"Fila {row_num}: Cantidad '{qty_str}' no es un número válido",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "cantidad", "value": qty_str}
            )

        cost_str = row.get('costo', '0').strip()
        try:
            cost = float(cost_str) if cost_str else 0
        except ValueError:
            cost = 0

        return {
            'sku': sku,
            'quantity': qty,
            'location_path': loc_path,
            'reason': reason,
            'cost': cost,
            'notes': row.get('notas', '').strip(),
            'reference': row.get('referencia', '').strip(),
            'serials_str': row.get('series', '').strip() or row.get('serie', '').strip() or row.get('serial', '').strip() or row.get('lote', '').strip(),
        }
