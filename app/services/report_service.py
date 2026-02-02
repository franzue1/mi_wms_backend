# app/services/report_service.py
"""
Servicio de Reportes y Dashboard.
Centraliza la lógica de negocio para cálculos de KPIs, antigüedad, cobertura y Kardex.
"""

from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime, date

from app.database.repositories import report_repo
from app.exceptions import ValidationError, NotFoundError, ErrorCodes

getcontext().prec = 28


class ReportService:
    """
    Servicio para lógica de negocio de reportes.
    El repositorio maneja SQL puro, este servicio procesa y enriquece los datos.
    """

    # --- CONSTANTES DE AGING ---
    AGING_BUCKETS = {
        '0-30': (0, 30),
        '31-60': (31, 60),
        '61-90': (61, 90),
        '+90': (91, 9999),
    }

    AGING_BUCKET_LABELS = {
        '0-30': '0-30 días',
        '31-60': '31-60 días',
        '61-90': '61-90 días',
        '+90': '+90 días',
        'Sin Fecha': 'Sin Fecha',
    }

    # --- CONSTANTES DE COBERTURA ---
    COVERAGE_INFINITY = 999  # Valor para productos sin consumo

    # --- CONSTANTES DE KARDEX ---
    TWO_PLACES = Decimal('0.01')
    FOUR_PLACES = Decimal('0.0001')

    # =========================================================================
    # VALIDACIONES
    # =========================================================================

    @staticmethod
    def validate_date_range(date_from: date, date_to: date) -> None:
        """
        Valida que el rango de fechas sea válido.

        Raises:
            ValidationError: Si date_from > date_to
        """
        if date_from > date_to:
            raise ValidationError(
                message="La fecha inicial no puede ser mayor a la fecha final",
                code=ErrorCodes.PROJECT_INVALID_DATES,
                details={"date_from": str(date_from), "date_to": str(date_to)}
            )

    @staticmethod
    def validate_history_days(history_days: int) -> int:
        """
        Valida y normaliza los días de historial para reportes de cobertura.

        Returns:
            int: Días normalizados (mínimo 1)
        """
        return max(1, history_days)

    # =========================================================================
    # AGING (ANTIGÜEDAD)
    # =========================================================================

    @staticmethod
    def get_aging_bucket_key(days: int) -> str:
        """
        Determina el bucket de antigüedad basado en días.

        Args:
            days: Número de días de antigüedad

        Returns:
            str: Key del bucket ('0-30', '31-60', '61-90', '+90')
        """
        if days is None:
            return 'Sin Fecha'
        if days <= 30:
            return '0-30'
        if days <= 60:
            return '31-60'
        if days <= 90:
            return '61-90'
        return '+90'

    @staticmethod
    def get_aging_bucket_label(bucket_key: str) -> str:
        """Obtiene la etiqueta legible de un bucket de antigüedad."""
        return ReportService.AGING_BUCKET_LABELS.get(bucket_key, bucket_key)

    @staticmethod
    def enrich_aging_details(raw_details: List[Dict]) -> List[Dict]:
        """
        Enriquece los datos de antigüedad con bucket calculado.

        Args:
            raw_details: Lista de diccionarios del repositorio

        Returns:
            Lista enriquecida con aging_bucket_label
        """
        enriched = []
        for row in raw_details:
            item = dict(row)
            days = item.get('aging_days')
            bucket_key = ReportService.get_aging_bucket_key(days)
            item['aging_bucket'] = ReportService.get_aging_bucket_label(bucket_key)
            enriched.append(item)
        return enriched

    # =========================================================================
    # COBERTURA
    # =========================================================================

    @staticmethod
    def calculate_coverage_days(stock: float, daily_consumption: float) -> float:
        """
        Calcula los días de cobertura de stock.

        Args:
            stock: Cantidad en stock
            daily_consumption: Consumo promedio diario

        Returns:
            float: Días de cobertura (999 si no hay consumo)
        """
        if daily_consumption <= 0:
            return ReportService.COVERAGE_INFINITY
        return stock / daily_consumption

    @staticmethod
    def get_coverage_status(days: float, history_days: int) -> str:
        """
        Determina el estado de cobertura (crítico, advertencia, ok).

        Args:
            days: Días de cobertura
            history_days: Período de análisis (30 o 90)

        Returns:
            str: 'critical', 'warning', 'ok', 'infinity'
        """
        if days == ReportService.COVERAGE_INFINITY:
            return 'infinity'

        if history_days <= 30:
            if days < 7:
                return 'critical'
            if days < 21:
                return 'warning'
        else:
            if days < 15:
                return 'critical'
            if days < 45:
                return 'warning'
        return 'ok'

    # =========================================================================
    # KARDEX - PROCESAMIENTO DE EXPORTACIÓN
    # =========================================================================

    @staticmethod
    def process_kardex_export_data(
        company_id: int,
        date_from_db: str,
        date_to_db: str,
        warehouse_id: Optional[str],
        product_filter: Optional[str],
        date_from_display: str
    ) -> List[Dict[str, Any]]:
        """
        Procesa los datos del Kardex para exportación CSV.
        Calcula saldos progresivos usando costo promedio ponderado.

        Esta es la función "pesada" que antes estaba en el API.

        Args:
            company_id: ID de la empresa
            date_from_db: Fecha desde en formato YYYY-MM-DD
            date_to_db: Fecha hasta en formato YYYY-MM-DD
            warehouse_id: ID del almacén o 'all'
            product_filter: Filtro de producto (SKU o nombre)
            date_from_display: Fecha desde en formato DD/MM/YYYY para mostrar

        Returns:
            Lista de diccionarios con los datos procesados para CSV

        Raises:
            NotFoundError: Si no hay datos para exportar
        """
        D = Decimal
        TWO_PLACES = ReportService.TWO_PLACES
        FOUR_PLACES = ReportService.FOUR_PLACES

        # 1. Obtener saldo inicial de TODOS los productos
        summary_initial = report_repo.get_kardex_summary(
            company_id, '1900-01-01', date_from_db, product_filter, warehouse_id
        )

        product_states = {}
        for item_row in summary_initial:
            item = dict(item_row)
            product_states[item['product_id']] = {
                'qty': D(str(item.get('final_balance', 0.0) or 0.0)),
                'val': D(str(item.get('final_value', 0.0) or 0.0)),
                'sku': item['sku'],
                'name': item['product_name'],
                'category_name': item.get('category_name')
            }

        # 2. Obtener TODOS los movimientos en el rango
        raw_moves = report_repo.get_full_product_kardex_data(
            company_id, date_from_db, date_to_db, warehouse_id, product_filter
        )

        final_data = []
        group_id_counter = 0
        current_product_id = None
        state = {}

        # Si no hay movimientos, exportar solo los saldos iniciales
        if not raw_moves:
            for group_id, (pid, state_data) in enumerate(product_states.items(), 1):
                if state_data['qty'] != D('0') or state_data['val'] != D('0'):
                    final_data.append({
                        'GroupID': group_id,
                        'SKU': state_data['sku'],
                        'Producto': state_data['name'],
                        'Categoría': state_data.get('category_name') or '',
                        'Fecha': date_from_display,
                        'Referencia': 'SALDO INICIAL',
                        'Saldo Cant': state_data['qty'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
                        'Saldo Valorizado': state_data['val'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                    })
            return final_data

        # 3. Procesar movimientos y calcular saldos
        for move_row in raw_moves:
            move = dict(move_row)
            p_id = move['product_id']

            if p_id != current_product_id:
                current_product_id = p_id
                group_id_counter += 1
                state = product_states.get(p_id, {
                    'qty': D('0'),
                    'val': D('0'),
                    'sku': move['product_sku'],
                    'name': move['product_name'],
                    'category_name': move.get('category_name')
                })

                if state['qty'] != D('0') or state['val'] != D('0'):
                    final_data.append({
                        'GroupID': group_id_counter,
                        'SKU': state['sku'],
                        'Producto': state['name'],
                        'Categoría': state.get('category_name') or '',
                        'Fecha': date_from_display,
                        'Referencia': 'SALDO INICIAL',
                        'Saldo Cant': state['qty'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
                        'Saldo Valorizado': state['val'].quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                    })

            current_qty = state['qty']
            current_val = state['val']

            quantity_in = D(str(move.get('quantity_in', 0.0) or 0.0))
            quantity_out = D(str(move.get('quantity_out', 0.0) or 0.0))
            cost_at_adjustment_raw = move.get('cost_at_adjustment')
            cost_at_adjustment = D(str(cost_at_adjustment_raw)) if cost_at_adjustment_raw is not None else None

            valor_entrada_calc = D('0')
            valor_salida_calc = D('0')
            precio_unit_salida = D('0')
            precio_unit_entrada = D('0')
            current_avg_cost = (current_val / current_qty) if current_qty > D('0') else D('0')

            if quantity_out > D('0'):
                if cost_at_adjustment is not None and cost_at_adjustment > D('0'):
                    precio_unit_salida = cost_at_adjustment.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                    valor_salida_calc = (quantity_out * precio_unit_salida).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                else:
                    precio_unit_salida = current_avg_cost.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                    valor_salida_calc = (quantity_out * precio_unit_salida).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                current_qty -= quantity_out
                current_val -= valor_salida_calc

            elif quantity_in > D('0'):
                price_unit_in_raw = move.get('price_unit')
                if price_unit_in_raw is not None and D(str(price_unit_in_raw)) > D('0'):
                    precio_unit_entrada = D(str(price_unit_in_raw)).quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                    valor_entrada_calc = (quantity_in * precio_unit_entrada).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                else:
                    precio_unit_entrada = current_avg_cost.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)
                    valor_entrada_calc = (quantity_in * precio_unit_entrada).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                current_qty += quantity_in
                current_val += valor_entrada_calc

            # Limpiar cantidades pequeñas
            if current_qty.compare(D('0.005')) < 0:
                current_qty = D('0')
                current_val = D('0')

            state['qty'] = current_qty
            state['val'] = current_val

            # Formatear fecha
            fecha_str = ''
            if move.get('date'):
                try:
                    fecha_str = move['date'].strftime("%d/%m/%Y %H:%M")
                except AttributeError:
                    fecha_str = str(move['date'])

            fecha_traslado_str = ''
            if move.get('date_transfer'):
                try:
                    fecha_traslado_str = move['date_transfer'].strftime("%d/%m/%Y")
                except AttributeError:
                    fecha_traslado_str = str(move['date_transfer'])

            final_data.append({
                'GroupID': group_id_counter,
                'SKU': move['product_sku'],
                'Producto': move['product_name'],
                'Categoría': move.get('category_name') or '',
                'Fecha': fecha_str,
                'Fecha Traslado': fecha_traslado_str,
                'Referencia': move['operation_ref'],
                'Tipo Operacion': move['custom_operation_type'],
                'Almacen Origen': move.get('almacen_origen') or (move.get('partner_name') if move.get('type_code') == 'IN' else "-"),
                'Ubicacion Origen': move.get('ubicacion_origen') or "-",
                'Almacen Destino': move.get('almacen_destino') or (move.get('partner_name') if move.get('type_code') == 'OUT' else "-"),
                'Ubicacion Destino': move.get('ubicacion_destino') or "-",
                'Razón Ajuste': move.get('adjustment_reason') or '',
                'Almacen Afectado': move.get('affected_warehouse') or '',
                'Guia Remision / Acta': move.get('partner_ref') or '',
                'Proveedor / Cliente / OT': move.get('partner_name') or '',
                'Orden de Compra': move.get('purchase_order') or '',
                'Entrada Cant': quantity_in.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if quantity_in > D('0') else '',
                'Precio Unit. Entrada': precio_unit_entrada.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP) if quantity_in > D('0') else '',
                'Valor Entrada': valor_entrada_calc.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if valor_entrada_calc > D('0') else '',
                'Salida Cant': quantity_out.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if quantity_out > D('0') else '',
                'Precio Unit. Salida': precio_unit_salida.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP) if quantity_out > D('0') else '',
                'Valor Salida': valor_salida_calc.quantize(TWO_PLACES, rounding=ROUND_HALF_UP) if valor_salida_calc > D('0') else '',
                'Saldo Cant': current_qty.quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
                'Saldo Valorizado': current_val.quantize(TWO_PLACES, rounding=ROUND_HALF_UP),
            })

        return final_data

    # =========================================================================
    # GENERACIÓN DE CSV
    # =========================================================================

    @staticmethod
    def get_kardex_csv_headers() -> List[str]:
        """Retorna los headers para el CSV de Kardex detallado."""
        return [
            'GroupID', 'SKU', 'Producto', 'Categoría', 'Fecha', 'Fecha Traslado', 'Referencia',
            'Tipo Operacion', 'Almacen Origen', 'Ubicacion Origen', 'Almacen Destino', 'Ubicacion Destino',
            'Razón Ajuste', 'Almacen Afectado', 'Guia Remision / Acta',
            'Proveedor / Cliente / OT', 'Orden de Compra',
            'Entrada Cant', 'Precio Unit. Entrada', 'Valor Entrada',
            'Salida Cant', 'Precio Unit. Salida', 'Valor Salida',
            'Saldo Cant', 'Saldo Valorizado'
        ]

    @staticmethod
    def generate_kardex_csv_content(data: List[Dict]) -> str:
        """
        Genera el contenido CSV del Kardex.

        Args:
            data: Lista de diccionarios procesados

        Returns:
            str: Contenido CSV como string

        Raises:
            NotFoundError: Si no hay datos
        """
        import csv
        import io

        if not data:
            raise NotFoundError(
                message="No se encontraron movimientos para exportar",
                code=ErrorCodes.EXPORT_NO_DATA
            )

        headers = ReportService.get_kardex_csv_headers()

        output = io.StringIO(newline='')
        writer = csv.DictWriter(output, fieldnames=headers, delimiter=';', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)

        return output.getvalue()

    @staticmethod
    def generate_stock_summary_csv_content(
        data: List[Dict],
        headers_map: Dict[str, str]
    ) -> str:
        """
        Genera contenido CSV para reportes de stock.

        Args:
            data: Lista de diccionarios con datos
            headers_map: Mapeo {key: header_label}

        Returns:
            str: Contenido CSV
        """
        import csv
        import io

        if not data:
            raise NotFoundError(
                message="No hay datos para exportar",
                code=ErrorCodes.EXPORT_NO_DATA
            )

        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')

        # Escribir headers
        writer.writerow(headers_map.values())

        # Escribir datos
        for row_dict in data:
            csv_row = [row_dict.get(key, '') for key in headers_map.keys()]
            writer.writerow(csv_row)

        return output.getvalue()

    # =========================================================================
    # DASHBOARD KPIs
    # =========================================================================

    @staticmethod
    def aggregate_ownership_values(ownership_stats: List[Dict]) -> Tuple[float, float]:
        """
        Agrega valores de propiedad (propio vs consignado).

        Args:
            ownership_stats: Lista de stats del repositorio

        Returns:
            Tuple[float, float]: (owned_value, consigned_value)
        """
        own_val = 0.0
        cons_val = 0.0

        for stat in ownership_stats:
            if stat.get('type') == 'Propio':
                own_val += float(stat.get('value', 0) or 0)
            elif stat.get('type') == 'Consignado':
                cons_val += float(stat.get('value', 0) or 0)

        return own_val, cons_val

    @staticmethod
    def format_throughput_for_chart(throughput_data: List[Tuple]) -> List[Dict]:
        """
        Formatea datos de throughput para gráficos.

        Args:
            throughput_data: Lista de tuplas (date, count)

        Returns:
            Lista de dicts con format para chart
        """
        return [
            {"day": day.strftime("%a"), "count": count}
            for day, count in throughput_data
        ]

    # =========================================================================
    # FILTROS Y NORMALIZACIÓN
    # =========================================================================

    @staticmethod
    def normalize_warehouse_filter(warehouse_id: Optional[str]) -> Optional[str]:
        """
        Normaliza el filtro de almacén.

        Args:
            warehouse_id: 'all', ID numérico como string, o None

        Returns:
            None si es 'all' o vacío, el ID si es válido
        """
        if not warehouse_id or warehouse_id == 'all':
            return None
        return warehouse_id

    @staticmethod
    def build_report_filters(
        warehouse_id: Optional[int] = None,
        location_id: Optional[int] = None,
        sku: Optional[str] = None,
        product_name: Optional[str] = None,
        category_name: Optional[str] = None,
        location_name: Optional[str] = None,
        lot_name: Optional[str] = None,
        project_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Construye un diccionario de filtros limpio (sin valores None).

        Returns:
            Dict con solo los filtros que tienen valor
        """
        filters = {
            'warehouse_id': warehouse_id,
            'location_id': location_id,
            'sku': sku,
            'product_name': product_name,
            'category_name': category_name,
            'location_name': location_name,
            'lot_name': lot_name,
            'project_name': project_name,
        }
        return {k: v for k, v in filters.items() if v is not None and v != ''}
