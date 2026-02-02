# app/services/work_order_service.py
"""
Service Layer para Work Orders (Órdenes de Trabajo / Liquidaciones).
Orquesta la coordinación entre OTs y sus Pickings asociados.
Inyecta PickingService para validaciones de stock y series.
"""

from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime, date
import io
import csv

from app.exceptions import (
    ValidationError,
    NotFoundError,
    BusinessRuleError,
    ErrorCodes
)
from app.services.picking_service import PickingService


class WorkOrderService:
    """
    Servicio de lógica de negocio para Work Orders.
    Orquesta la creación/actualización de OTs y sus pickings de liquidación.
    """

    # ==========================================================================
    # CONSTANTES - FASES/ESTADOS
    # ==========================================================================

    PHASE_PENDING = 'Sin Liquidar'
    PHASE_IN_PROGRESS = 'En Liquidación'
    PHASE_PENDING_DOCS = 'Pendiente Documentación'
    PHASE_LIQUIDATED = 'Liquidado'

    VALID_PHASES = {PHASE_PENDING, PHASE_IN_PROGRESS, PHASE_PENDING_DOCS, PHASE_LIQUIDATED}

    # Transiciones de fase permitidas
    ALLOWED_PHASE_TRANSITIONS = {
        PHASE_PENDING: {PHASE_IN_PROGRESS, PHASE_LIQUIDATED},
        PHASE_IN_PROGRESS: {PHASE_PENDING, PHASE_PENDING_DOCS, PHASE_LIQUIDATED},
        PHASE_PENDING_DOCS: {PHASE_IN_PROGRESS, PHASE_LIQUIDATED},
        PHASE_LIQUIDATED: set(),  # Estado final, inmutable
    }

    # ==========================================================================
    # CONSTANTES - TIPOS DE PICKING ASOCIADOS
    # ==========================================================================

    PICKING_TYPE_CONSUMO = 'OUT'  # Consumo de materiales
    PICKING_TYPE_RETIRO = 'RET'   # Retiro de materiales

    # ==========================================================================
    # CONSTANTES - CAMPOS PERMITIDOS PARA UPDATE
    # ==========================================================================

    ALLOWED_WO_FIELDS_TO_UPDATE = {
        'customer_name', 'address', 'warehouse_id', 'date_attended',
        'service_type', 'job_type', 'phase', 'project_id'
    }

    # ==========================================================================
    # CONSTANTES - CSV HEADERS
    # ==========================================================================

    EXPORT_CSV_HEADERS = [
        'ot_number', 'customer_name', 'address', 'service_type',
        'job_type', 'phase', 'project_name'
    ]

    REQUIRED_IMPORT_HEADERS = {'ot_number', 'customer_name'}

    # Variantes aceptadas para columna de proyecto en importación
    PROJECT_COLUMN_VARIANTS = ['project_name', 'project', 'proyecto', 'obra']

    # ==========================================================================
    # VALIDACIÓN DE ESTADOS Y TRANSICIONES
    # ==========================================================================

    @staticmethod
    def validate_phase(phase: str) -> str:
        """
        Valida que la fase sea válida.

        Args:
            phase: Fase a validar

        Returns:
            str: Fase validada

        Raises:
            ValidationError: Si la fase no es válida
        """
        if phase not in WorkOrderService.VALID_PHASES:
            raise ValidationError(
                f"Fase '{phase}' no es válida. Valores permitidos: {', '.join(WorkOrderService.VALID_PHASES)}",
                ErrorCodes.INVALID_STATE,
                {"phase": phase, "valid_phases": list(WorkOrderService.VALID_PHASES)}
            )
        return phase

    @staticmethod
    def validate_phase_transition(current_phase: str, new_phase: str) -> bool:
        """
        Valida si una transición de fase es permitida.

        Args:
            current_phase: Fase actual
            new_phase: Nueva fase

        Returns:
            bool: True si la transición es válida

        Raises:
            BusinessRuleError: Si la transición no está permitida
        """
        if current_phase not in WorkOrderService.VALID_PHASES:
            raise ValidationError(
                f"Fase actual '{current_phase}' no es válida",
                ErrorCodes.INVALID_STATE,
                {"current_phase": current_phase}
            )

        if new_phase not in WorkOrderService.VALID_PHASES:
            raise ValidationError(
                f"Fase destino '{new_phase}' no es válida",
                ErrorCodes.INVALID_STATE,
                {"new_phase": new_phase}
            )

        allowed = WorkOrderService.ALLOWED_PHASE_TRANSITIONS.get(current_phase, set())

        if new_phase not in allowed:
            raise BusinessRuleError(
                f"Transición de fase no permitida: '{current_phase}' → '{new_phase}'",
                "WO_INVALID_PHASE_TRANSITION",
                {"current_phase": current_phase, "new_phase": new_phase}
            )

        return True

    @staticmethod
    def can_modify(wo_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Verifica si una OT puede ser modificada.

        Args:
            wo_data: Diccionario con datos de la OT

        Returns:
            Tuple[bool, Optional[str]]: (puede_modificar, mensaje_error)
        """
        phase = wo_data.get('phase')

        if phase == WorkOrderService.PHASE_LIQUIDATED:
            return False, "Acción denegada: La Orden de Trabajo está LIQUIDADA y no se puede modificar."

        return True, None

    @staticmethod
    def can_delete(wo_data: Dict[str, Any], has_validated_pickings: bool) -> Tuple[bool, Optional[str]]:
        """
        Verifica si una OT puede ser eliminada.

        Args:
            wo_data: Diccionario con datos de la OT
            has_validated_pickings: Si tiene pickings con state='done'

        Returns:
            Tuple[bool, Optional[str]]: (puede_eliminar, mensaje_error)
        """
        phase = wo_data.get('phase')
        ot_number = wo_data.get('ot_number', 'N/A')

        if phase == WorkOrderService.PHASE_LIQUIDATED:
            return False, f"🚫 Acción Bloqueada: La OT '{ot_number}' está LIQUIDADA. No se puede eliminar la historia."

        if has_validated_pickings:
            return False, "🚫 Acción Bloqueada: Esta OT tiene movimientos de inventario ya procesados. Debe anularlos primero (si es posible) o crear una devolución."

        return True, None

    @staticmethod
    def can_liquidate(wo_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Verifica si una OT puede ser liquidada.

        Args:
            wo_data: Diccionario con datos de la OT

        Returns:
            Tuple[bool, Optional[str]]: (puede_liquidar, mensaje_error)
        """
        phase = wo_data.get('phase')

        if phase == WorkOrderService.PHASE_LIQUIDATED:
            return False, "La OT ya había sido liquidada exitosamente por una petición anterior."

        return True, None

    # ==========================================================================
    # VALIDACIÓN DE DATOS
    # ==========================================================================

    @staticmethod
    def validate_ot_number(ot_number: Optional[str]) -> str:
        """
        Valida y normaliza el número de OT.

        Args:
            ot_number: Número de OT a validar

        Returns:
            str: Número de OT normalizado

        Raises:
            ValidationError: Si el número está vacío
        """
        if not ot_number or not ot_number.strip():
            raise ValidationError(
                "El número de Orden de Trabajo es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "ot_number"}
            )
        return ot_number.strip()

    @staticmethod
    def validate_customer_name(customer_name: Optional[str]) -> str:
        """
        Valida y normaliza el nombre del cliente.

        Args:
            customer_name: Nombre a validar

        Returns:
            str: Nombre normalizado

        Raises:
            ValidationError: Si el nombre está vacío
        """
        if not customer_name or not customer_name.strip():
            raise ValidationError(
                "El nombre del cliente es requerido",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "customer_name"}
            )
        return customer_name.strip()

    @staticmethod
    def validate_attention_date(date_value: Any) -> Optional[date]:
        """
        Valida y parsea la fecha de atención.

        Args:
            date_value: Valor de fecha (str, date, datetime, o None)

        Returns:
            Optional[date]: Fecha parseada o None

        Raises:
            ValidationError: Si el formato es inválido
        """
        if not date_value:
            return None

        if isinstance(date_value, date):
            return date_value

        if isinstance(date_value, datetime):
            return date_value.date()

        if isinstance(date_value, str):
            # Intentar varios formatos
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt).date()
                except ValueError:
                    continue

            raise ValidationError(
                f"Formato de fecha '{date_value}' no reconocido. Use YYYY-MM-DD o DD/MM/YYYY",
                "WO_INVALID_DATE_FORMAT",
                {"value": date_value}
            )

        raise ValidationError(
            f"Tipo de fecha no soportado: {type(date_value)}",
            "WO_INVALID_DATE_TYPE",
            {"value": str(date_value)}
        )

    # ==========================================================================
    # PREPARACIÓN DE DATOS
    # ==========================================================================

    @staticmethod
    def prepare_wo_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara y valida los datos de una OT para crear/actualizar.

        Args:
            data: Diccionario con datos de la OT

        Returns:
            Dict[str, Any]: Datos preparados y validados
        """
        return {
            'ot_number': WorkOrderService.validate_ot_number(data.get('ot_number')),
            'customer_name': WorkOrderService.validate_customer_name(data.get('customer_name')),
            'address': (data.get('address') or '').strip(),
            'service_type': (data.get('service_type') or '').strip() or None,
            'job_type': (data.get('job_type') or '').strip() or None,
            'project_id': data.get('project_id'),
        }

    @staticmethod
    def prepare_wo_update_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara y filtra los datos para actualización de OT.
        Solo permite campos de la whitelist.

        Args:
            data: Diccionario con datos a actualizar

        Returns:
            Dict[str, Any]: Diccionario filtrado con solo campos permitidos
        """
        filtered = {}
        for k, v in data.items():
            if k in WorkOrderService.ALLOWED_WO_FIELDS_TO_UPDATE:
                # No incluir valores None a menos que sea project_id (puede ser desasignado)
                if v is not None or k == 'project_id':
                    filtered[k] = v

        return filtered

    @staticmethod
    def prepare_liquidation_picking_data(
        warehouse_id: int,
        date_attended: Any,
        service_act_number: Optional[str],
        lines_data: List[Dict[str, Any]],
        location_src_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Prepara los datos para crear/actualizar un picking de liquidación.

        Args:
            warehouse_id: ID del almacén
            date_attended: Fecha de atención
            service_act_number: Número de acta de servicio
            lines_data: Lista de líneas (productos)
            location_src_id: ID de ubicación origen (opcional)

        Returns:
            Dict[str, Any]: Datos preparados para el picking
        """
        return {
            'warehouse_id': warehouse_id,
            'date_attended_db': WorkOrderService.validate_attention_date(date_attended),
            'service_act_number': (service_act_number or '').strip() or None,
            'lines_data': lines_data,
            'location_src_id': location_src_id,
        }

    @staticmethod
    def build_wo_filter_dict(
        ot_number: Optional[str] = None,
        customer_name: Optional[str] = None,
        address: Optional[str] = None,
        service_type: Optional[str] = None,
        job_type: Optional[str] = None,
        phase: Optional[str] = None,
        warehouse_name: Optional[str] = None,
        location_src_path: Optional[str] = None,
        service_act_number: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Construye un diccionario de filtros limpio para consulta de OTs.

        Returns:
            Dict[str, str]: Diccionario de filtros sin valores vacíos
        """
        filters = {
            'ot_number': ot_number,
            'customer_name': customer_name,
            'address': address,
            'service_type': service_type,
            'job_type': job_type,
            'phase': phase,
            'warehouse_name': warehouse_name,
            'location_src_path': location_src_path,
            'service_act_number': service_act_number,
            'project_name': project_name,
        }
        return {k: v for k, v in filters.items() if v is not None and v != ""}

    # ==========================================================================
    # VALIDACIÓN DE LIQUIDACIÓN (usa PickingService)
    # ==========================================================================

    @staticmethod
    def validate_liquidation_lines(
        lines: List[Dict[str, Any]],
        context: str = "consumo"
    ) -> List[str]:
        """
        Valida las líneas de una liquidación antes de procesar.

        Args:
            lines: Lista de líneas a validar
            context: Contexto ("consumo" o "retiro")

        Returns:
            List[str]: Lista de errores encontrados (vacía si todo OK)
        """
        errors = []

        for i, line in enumerate(lines):
            row_num = i + 1
            product_id = line.get('product_id')
            quantity = line.get('quantity', 0)

            if not product_id:
                errors.append(f"Línea {row_num} ({context}): Falta el producto")
                continue

            try:
                qty = float(quantity)
                if qty <= 0:
                    errors.append(f"Línea {row_num} ({context}): Cantidad debe ser mayor a 0")
            except (ValueError, TypeError):
                errors.append(f"Línea {row_num} ({context}): Cantidad inválida")

            # Validar tracking si aplica
            tracking_data = line.get('tracking_data')
            tracking_type = line.get('tracking')

            if tracking_type and tracking_type != PickingService.TRACKING_NONE and tracking_data:
                total_tracking = sum(tracking_data.values())
                if abs(qty - total_tracking) > 0.001:
                    errors.append(
                        f"Línea {row_num} ({context}): Cantidad ({qty}) no coincide con series ({total_tracking})"
                    )

        return errors

    @staticmethod
    def validate_liquidation_has_lines(
        consumo_lines: List[Dict[str, Any]],
        retiro_lines: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Valida que la liquidación tenga al menos líneas de consumo.

        Args:
            consumo_lines: Líneas de consumo
            retiro_lines: Líneas de retiro (opcional)

        Raises:
            ValidationError: Si no hay líneas de consumo
        """
        if not consumo_lines:
            raise ValidationError(
                "La liquidación debe tener al menos una línea de consumo",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "consumo_lines"}
            )

    # ==========================================================================
    # PROCESAMIENTO DE CSV
    # ==========================================================================

    @staticmethod
    def parse_csv_file(content: bytes) -> Tuple[List[Dict[str, str]], List[str]]:
        """
        Parsea el contenido de un archivo CSV de OTs.

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
    def validate_csv_headers(headers: List[str]) -> None:
        """
        Valida que el CSV tenga los headers requeridos.

        Args:
            headers: Lista de headers encontrados

        Raises:
            ValidationError: Si faltan headers requeridos
        """
        headers_set = {h.lower().strip() for h in headers}
        missing = WorkOrderService.REQUIRED_IMPORT_HEADERS - headers_set

        if missing:
            raise ValidationError(
                f"Faltan columnas obligatorias: {', '.join(sorted(missing))}",
                ErrorCodes.CSV_MISSING_HEADERS,
                {"missing_headers": list(missing)}
            )

    @staticmethod
    def extract_project_name_from_row(row: Dict[str, str]) -> Optional[str]:
        """
        Extrae el nombre del proyecto de una fila CSV.
        Busca en múltiples variantes de nombre de columna.

        Args:
            row: Fila del CSV

        Returns:
            Optional[str]: Nombre del proyecto o None
        """
        clean_row = {k.lower().strip(): v for k, v in row.items() if k}

        for variant in WorkOrderService.PROJECT_COLUMN_VARIANTS:
            value = clean_row.get(variant)
            if value and value.strip():
                return value.strip()

        return None

    @staticmethod
    def process_csv_row(
        row: Dict[str, str],
        row_num: int
    ) -> Dict[str, Any]:
        """
        Procesa y valida una fila individual del CSV.

        Args:
            row: Fila del CSV
            row_num: Número de fila (para mensajes de error)

        Returns:
            Dict[str, Any]: Datos normalizados

        Raises:
            ValidationError: Si la fila tiene datos inválidos
        """
        clean_row = {k.lower().strip(): v.strip() for k, v in row.items() if k}

        ot_number = clean_row.get('ot_number', '').strip()
        customer_name = clean_row.get('customer_name', '').strip()

        if not ot_number:
            raise ValidationError(
                f"Fila {row_num}: El número de OT es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "ot_number"}
            )

        if not customer_name:
            raise ValidationError(
                f"Fila {row_num}: El nombre del cliente es obligatorio",
                ErrorCodes.CSV_ROW_ERROR,
                {"row": row_num, "field": "customer_name"}
            )

        return {
            'ot_number': ot_number,
            'customer_name': customer_name,
            'address': clean_row.get('address', '').strip() or None,
            'service_type': clean_row.get('service_type', '').strip() or None,
            'job_type': clean_row.get('job_type', '').strip() or None,
            'project_name': WorkOrderService.extract_project_name_from_row(row),
        }

    @staticmethod
    def generate_csv_content(work_orders: List[Dict[str, Any]]) -> str:
        """
        Genera contenido CSV a partir de una lista de OTs.

        Args:
            work_orders: Lista de diccionarios de OTs

        Returns:
            str: Contenido CSV como string
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        writer.writerow(WorkOrderService.EXPORT_CSV_HEADERS)

        for wo in work_orders:
            wo_dict = dict(wo) if hasattr(wo, 'keys') else wo
            writer.writerow([
                wo_dict.get('ot_number', ''),
                wo_dict.get('customer_name', ''),
                wo_dict.get('address', ''),
                wo_dict.get('service_type', ''),
                wo_dict.get('job_type', ''),
                wo_dict.get('phase', ''),
                wo_dict.get('project_name', ''),
            ])

        return output.getvalue()

    # ==========================================================================
    # ORQUESTACIÓN DE LIQUIDACIÓN
    # ==========================================================================

    @staticmethod
    def prepare_full_liquidation_data(
        wo_id: int,
        consumo_data: Dict[str, Any],
        retiro_data: Optional[Dict[str, Any]],
        company_id: int,
        user_name: str
    ) -> Dict[str, Any]:
        """
        Prepara todos los datos necesarios para una liquidación completa.
        Este método NO ejecuta la transacción, solo prepara los datos.

        Args:
            wo_id: ID de la Work Order
            consumo_data: Datos del picking de consumo
            retiro_data: Datos del picking de retiro (opcional)
            company_id: ID de la empresa
            user_name: Usuario que ejecuta

        Returns:
            Dict[str, Any]: Datos preparados para la transacción
        """
        # Validar que hay líneas de consumo
        consumo_lines = consumo_data.get('lines_data', [])
        retiro_lines = retiro_data.get('lines_data', []) if retiro_data else []

        WorkOrderService.validate_liquidation_has_lines(consumo_lines, retiro_lines)

        # Validar líneas
        errors = []
        errors.extend(WorkOrderService.validate_liquidation_lines(consumo_lines, "consumo"))
        if retiro_lines:
            errors.extend(WorkOrderService.validate_liquidation_lines(retiro_lines, "retiro"))

        if errors:
            raise ValidationError(
                "Errores en las líneas de liquidación:\n- " + "\n- ".join(errors),
                "WO_LIQUIDATION_LINE_ERRORS",
                {"errors": errors}
            )

        return {
            'wo_id': wo_id,
            'company_id': company_id,
            'user_name': user_name,
            'consumo_data': consumo_data,
            'retiro_data': retiro_data,
            'consumo_lines': consumo_lines,
            'retiro_lines': retiro_lines,
        }
