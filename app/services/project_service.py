# app/services/project_service.py
"""
Servicio de Proyectos y Jerarquía.
Centraliza toda la lógica de negocio para la gestión de:
- Direcciones (Nivel 1)
- Gerencias (Nivel 2)
- Macro Proyectos (Nivel 3)
- Obras/Proyectos (Nivel 4)
"""

from typing import Dict, Any, List, Optional, Tuple
import csv
import io
import re
from datetime import date

from app.exceptions import (
    ValidationError,
    NotFoundError,
    BusinessRuleError,
    DuplicateError,
    ErrorCodes
)


class ProjectService:
    """
    Servicio que encapsula toda la lógica de negocio de Proyectos y Jerarquía.
    El repositorio solo debe ejecutar SQL puro.
    """

    # =====================================================
    # CONSTANTES DE JERARQUÍA
    # =====================================================

    # Niveles de la jerarquía
    LEVEL_DIRECTION = 1
    LEVEL_MANAGEMENT = 2
    LEVEL_MACRO_PROJECT = 3
    LEVEL_PROJECT = 4

    LEVEL_NAMES = {
        LEVEL_DIRECTION: "Dirección",
        LEVEL_MANAGEMENT: "Gerencia",
        LEVEL_MACRO_PROJECT: "Proyecto (Macro)",
        LEVEL_PROJECT: "Obra"
    }

    # =====================================================
    # CONSTANTES DE FASE DE OBRA
    # =====================================================

    PHASE_NOT_STARTED = "Sin Iniciar"
    PHASE_IN_PROGRESS = "En Instalación"
    PHASE_LIQUIDATED = "Liquidado"
    PHASE_RETURNING = "En Devolución"
    PHASE_TO_INVOICE = "Por Facturar"
    PHASE_INVOICED = "Facturado"

    VALID_PHASES = [
        PHASE_NOT_STARTED,
        PHASE_IN_PROGRESS,
        PHASE_LIQUIDATED,
        PHASE_RETURNING,
        PHASE_TO_INVOICE,
        PHASE_INVOICED
    ]

    # Estados de obra
    STATUS_ACTIVE = "active"
    STATUS_CLOSED = "closed"

    VALID_STATUSES = [STATUS_ACTIVE, STATUS_CLOSED]

    # =====================================================
    # CONSTANTES DE CSV
    # =====================================================

    # Headers requeridos para importación de jerarquía
    HIERARCHY_REQUIRED_HEADERS = ["dir_name"]

    # Mapeo de headers para importación de jerarquía
    HIERARCHY_HEADER_MAPPING = {
        'dir_name': ['direccion', 'direction', 'area', 'dirección'],
        'dir_code': ['cod dir', 'cod direccion', 'codigo direccion', 'código dirección'],
        'mgmt_name': ['gerencia', 'management', 'departamento'],
        'mgmt_code': ['cod ger', 'cod gerencia', 'codigo gerencia', 'código gerencia'],
        'macro_name': ['proyecto (macro)', 'proyecto', 'macro', 'project', 'macro proyecto'],
        'macro_code': ['cod proy', 'cod proyecto', 'codigo proyecto', 'código proyecto'],
        'cost_center': ['centro de costo', 'centro costo', 'ceco', 'cc']
    }

    # Headers requeridos para importación de obras
    PROJECT_REQUIRED_HEADERS = ['name', 'code', 'macro_name']

    # Mapeo de headers para importación de obras
    PROJECT_HEADER_MAPPING = {
        'name': ['name', 'nombre', 'nombre de obra', 'obra', 'proyecto'],
        'code': ['code', 'codigo', 'código', 'codigo pep', 'pep', 'id pep', 'código pep'],
        'macro_name': ['macro_name', 'macro', 'proyecto (macro)', 'macro proyecto', 'contrato marco'],
        'status': ['status', 'estado'],
        'phase': ['phase', 'fase', 'etapa'],
        'address': ['address', 'direccion', 'dirección', 'direccion fisica', 'ubicacion'],
        'department': ['department', 'departamento', 'dpto', 'dpto.'],
        'province': ['province', 'provincia', 'prov', 'prov.'],
        'district': ['district', 'distrito', 'dist'],
        'budget': ['budget', 'presupuesto', 'presupuesto (s/)', 'monto'],
        'start_date': ['start_date', 'inicio', 'f. inicio', 'fecha inicio'],
        'end_date': ['end_date', 'fin', 'f. fin', 'fecha fin', 'termino']
    }

    # =====================================================
    # MÉTODOS DE NORMALIZACIÓN
    # =====================================================

    @staticmethod
    def normalize_name(name: str) -> str:
        """
        Normaliza un nombre: quita espacios extra y convierte a mayúsculas.
        """
        if not name:
            return ""
        return " ".join(name.strip().upper().split())

    @staticmethod
    def normalize_code(code: Optional[str]) -> Optional[str]:
        """
        Normaliza un código: quita espacios y convierte a mayúsculas.
        """
        if not code:
            return None
        return code.strip().upper()

    @staticmethod
    def normalize_address(address: Optional[str]) -> Optional[str]:
        """
        Normaliza una dirección física: quita espacios extra y convierte a mayúsculas.
        """
        if not address:
            return None
        return " ".join(address.strip().upper().split())

    @staticmethod
    def normalize_header(header: str) -> str:
        """
        Normaliza un header de CSV para comparación flexible.
        """
        return header.lower().replace('ó', 'o').replace('é', 'e').replace('.', '').strip()

    # =====================================================
    # MÉTODOS DE VALIDACIÓN
    # =====================================================

    @staticmethod
    def validate_name(name: str, entity_type: str = "entidad") -> None:
        """
        Valida que un nombre no esté vacío y tenga formato correcto.
        """
        if not name or not name.strip():
            raise ValidationError(
                f"El nombre de {entity_type} es obligatorio.",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"field": "name", "entity_type": entity_type}
            )

    @staticmethod
    def validate_code(code: str, required: bool = False, entity_type: str = "entidad") -> None:
        """
        Valida que un código tenga formato correcto.
        """
        if required and (not code or not code.strip()):
            raise ValidationError(
                f"El código de {entity_type} es obligatorio.",
                ErrorCodes.CODE_REQUIRED,
                {"field": "code", "entity_type": entity_type}
            )

        if code and code.strip():
            clean_code = code.strip()
            if not re.match(r"^[a-zA-Z0-9_./-]*$", clean_code):
                raise ValidationError(
                    f"El código '{clean_code}' contiene caracteres inválidos. Solo se permiten letras, números, guiones, puntos y barras.",
                    ErrorCodes.CSV_ROW_ERROR,
                    {"code": clean_code}
                )

    @staticmethod
    def validate_uppercase_for_import(value: str, field_name: str, line_ref: str) -> None:
        """
        Valida que un valor nuevo esté en mayúsculas (para importación estricta).
        """
        if value and value != value.upper():
            raise ValidationError(
                f"{line_ref}: El nuevo valor '{value}' para '{field_name}' debe estar en MAYÚSCULAS.",
                ErrorCodes.HIERARCHY_INVALID_STRUCTURE,
                {"field": field_name, "value": value, "line": line_ref}
            )

    @staticmethod
    def validate_dates(start_date: Optional[date], end_date: Optional[date]) -> None:
        """
        Valida coherencia entre fechas de inicio y fin.
        """
        if start_date and end_date and start_date > end_date:
            raise ValidationError(
                "La Fecha de Inicio no puede ser posterior a la Fecha de Fin.",
                ErrorCodes.PROJECT_INVALID_DATES,
                {"start_date": str(start_date), "end_date": str(end_date)}
            )

    @staticmethod
    def validate_budget(budget: Any) -> float:
        """
        Valida y convierte el presupuesto a float.
        """
        if budget is None:
            return 0.0

        try:
            if isinstance(budget, str):
                clean = budget.replace("S/", "").replace(",", ".").replace(" ", "")
                return float(clean) if clean else 0.0
            return float(budget)
        except (ValueError, TypeError):
            raise ValidationError(
                f"Presupuesto inválido: '{budget}'",
                ErrorCodes.INVALID_QUANTITY,
                {"budget": str(budget)}
            )

    @staticmethod
    def validate_phase(phase: str) -> None:
        """
        Valida que la fase sea válida.
        """
        if phase and phase not in ProjectService.VALID_PHASES:
            raise ValidationError(
                f"Fase inválida: '{phase}'. Fases válidas: {ProjectService.VALID_PHASES}",
                ErrorCodes.PHASE_INVALID_TRANSITION,
                {"phase": phase, "valid_phases": ProjectService.VALID_PHASES}
            )

    @staticmethod
    def validate_status(status: str) -> None:
        """
        Valida que el estado sea válido.
        """
        if status and status not in ProjectService.VALID_STATUSES:
            raise ValidationError(
                f"Estado inválido: '{status}'. Estados válidos: {ProjectService.VALID_STATUSES}",
                ErrorCodes.CSV_ROW_ERROR,
                {"status": status, "valid_statuses": ProjectService.VALID_STATUSES}
            )

    # =====================================================
    # VALIDACIONES DE JERARQUÍA
    # =====================================================

    @staticmethod
    def validate_hierarchy_structure(
        dir_name: Optional[str],
        mgmt_name: Optional[str],
        macro_name: Optional[str],
        line_ref: str
    ) -> None:
        """
        Valida que la estructura jerárquica sea coherente.
        Un hijo no puede existir sin su padre.
        """
        # Caso 1: Tiene Gerencia o Proyecto, pero no tiene Dirección
        if not dir_name:
            if mgmt_name or macro_name:
                raise ValidationError(
                    f"{line_ref}: Estructura rota. Ha definido Gerencia/Proyecto pero falta la 'Dirección' (Padre Supremo).",
                    ErrorCodes.HIERARCHY_MISSING_PARENT,
                    {"line": line_ref, "missing": "direction"}
                )

        # Caso 2: Tiene Proyecto, pero no tiene Gerencia
        if macro_name and not mgmt_name:
            raise ValidationError(
                f"{line_ref}: Estructura rota. Quiere crear el Proyecto '{macro_name}' pero falta la 'Gerencia' (Padre).",
                ErrorCodes.HIERARCHY_MISSING_PARENT,
                {"line": line_ref, "macro_name": macro_name, "missing": "management"}
            )

    @staticmethod
    def validate_name_consistency(
        input_name: str,
        db_name: str,
        entity_type: str,
        line_ref: str
    ) -> None:
        """
        Valida que el nombre ingresado coincida exactamente con el existente en BD.
        """
        if input_name != db_name:
            raise ValidationError(
                f"{line_ref}: El/La {entity_type} '{input_name}' difiere de lo existente '{db_name}'. Use mayúsculas exactas.",
                ErrorCodes.HIERARCHY_INVALID_STRUCTURE,
                {"line": line_ref, "input": input_name, "existing": db_name, "entity": entity_type}
            )

    # =====================================================
    # VALIDACIONES DE INTEGRIDAD (DELETE)
    # =====================================================

    @staticmethod
    def validate_direction_can_delete(has_children: bool, direction_name: str = "") -> None:
        """
        Valida que una Dirección pueda ser eliminada.
        """
        if has_children:
            raise BusinessRuleError(
                "No se puede eliminar: Esta Dirección tiene Gerencias asociadas.",
                ErrorCodes.DIRECTION_HAS_CHILDREN,
                {"direction": direction_name}
            )

    @staticmethod
    def validate_management_can_delete(has_children: bool, management_name: str = "") -> None:
        """
        Valida que una Gerencia pueda ser eliminada.
        """
        if has_children:
            raise BusinessRuleError(
                "No se puede eliminar: Esta Gerencia tiene Proyectos (Macros) asociados.",
                ErrorCodes.MANAGEMENT_HAS_CHILDREN,
                {"management": management_name}
            )

    @staticmethod
    def validate_macro_can_delete(has_children: bool, macro_name: str = "") -> None:
        """
        Valida que un Macro Proyecto pueda ser eliminado.
        """
        if has_children:
            raise BusinessRuleError(
                "No se puede eliminar: Este Proyecto tiene Obras activas.",
                ErrorCodes.MACRO_PROJECT_HAS_CHILDREN,
                {"macro_project": macro_name}
            )

    @staticmethod
    def validate_project_can_delete(has_movements: bool, project_name: str = "") -> Tuple[bool, str]:
        """
        Valida que una Obra pueda ser eliminada.
        Retorna (can_hard_delete, message).
        """
        if has_movements:
            return False, "La obra tiene historial. Se ha marcado como 'Cerrado'."
        return True, "Obra eliminada."

    # =====================================================
    # MÉTODOS DE PARSEO CSV
    # =====================================================

    @staticmethod
    def parse_csv_file(content: bytes) -> Tuple[List[Dict], List[str]]:
        """
        Parsea un archivo CSV y retorna las filas y los headers.
        Detecta automáticamente el delimitador.
        """
        try:
            decoded = content.decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                decoded = content.decode('latin-1')
            except Exception:
                raise ValidationError(
                    "No se pudo decodificar el archivo. Use UTF-8 o Latin-1.",
                    ErrorCodes.CSV_ENCODING_ERROR
                )

        # Detectar delimitador
        first_line = decoded.split('\n')[0]
        delimiter = ';' if ';' in first_line else ','

        reader = csv.DictReader(io.StringIO(decoded), delimiter=delimiter)
        rows = list(reader)
        headers = reader.fieldnames or []

        if not rows:
            raise ValidationError(
                "El archivo CSV está vacío.",
                ErrorCodes.CSV_EMPTY_FILE
            )

        return rows, headers

    @staticmethod
    def resolve_csv_columns(headers: List[str], key_mapping: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Resuelve qué columna del CSV corresponde a cada clave interna.
        """
        # Crear mapa normalizado: { "nombre de obra": "Nombre de Obra" }
        headers_map = {ProjectService.normalize_header(h): h for h in headers}

        resolved_cols = {}
        for internal_key, synonyms in key_mapping.items():
            # Buscar si algún sinónimo existe en los headers del CSV
            match = next((h for h in synonyms if h in headers_map), None)
            if match:
                resolved_cols[internal_key] = headers_map[match]

        return resolved_cols

    @staticmethod
    def parse_date_strict(date_str: str, field_name: str) -> Optional[str]:
        """
        Parsea una fecha de forma estricta.
        Acepta formatos: YYYY-MM-DD o DD/MM/YYYY
        Retorna formato ISO: YYYY-MM-DD
        """
        if not date_str or not date_str.strip():
            return None

        d = date_str.strip()

        # Formato ISO: YYYY-MM-DD
        if "-" in d:
            parts = d.split("-")
            if len(parts) == 3 and len(parts[0]) == 4:
                return d

        # Formato local: DD/MM/YYYY
        if "/" in d:
            parts = d.split("/")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1]}-{parts[0]}"

        raise ValidationError(
            f"Fecha inválida en '{field_name}': '{d}'. Use YYYY-MM-DD o DD/MM/YYYY",
            ErrorCodes.CSV_ROW_ERROR,
            {"field": field_name, "value": d}
        )

    # =====================================================
    # PROCESAMIENTO DE FILAS CSV
    # =====================================================

    @staticmethod
    def process_hierarchy_row(row: Dict, resolved_cols: Dict[str, str], line_num: int) -> Dict[str, Any]:
        """
        Procesa una fila del CSV de jerarquía.
        """
        line_ref = f"Fila {line_num}"

        def get_val(key: str) -> str:
            csv_header = resolved_cols.get(key)
            return row.get(csv_header, '').strip() if csv_header else ''

        # Extraer datos
        dir_name = get_val('dir_name')
        dir_code = get_val('dir_code')
        mgmt_name = get_val('mgmt_name')
        mgmt_code = get_val('mgmt_code')
        macro_name = get_val('macro_name')
        macro_code = get_val('macro_code')
        cost_center = get_val('cost_center')

        # Validar estructura
        ProjectService.validate_hierarchy_structure(dir_name, mgmt_name, macro_name, line_ref)

        # Normalizar códigos
        return {
            'dir_name': dir_name,
            'dir_code': ProjectService.normalize_code(dir_code),
            'mgmt_name': mgmt_name,
            'mgmt_code': ProjectService.normalize_code(mgmt_code),
            'macro_name': macro_name,
            'macro_code': ProjectService.normalize_code(macro_code),
            'cost_center': ProjectService.normalize_code(cost_center),
            'line_ref': line_ref
        }

    @staticmethod
    def process_project_row(
        row: Dict,
        resolved_cols: Dict[str, str],
        macros_map: Dict[str, int],
        line_num: int
    ) -> Dict[str, Any]:
        """
        Procesa y valida una fila del CSV de obras/proyectos.
        """
        line_ref = f"Fila {line_num}"

        def get_val(key: str) -> str:
            csv_header = resolved_cols.get(key)
            return row.get(csv_header, '').strip() if csv_header else ''

        # 1. Validar Nombre
        name = get_val('name')
        if not name:
            if not any(row.values()):
                return None  # Fila vacía, saltamos
            raise ValidationError(
                f"{line_ref}: El campo 'Nombre de Obra' es obligatorio.",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"line": line_ref, "field": "name"}
            )

        # 2. Validar Código PEP
        code_val = get_val('code')
        if not code_val:
            raise ValidationError(
                f"{line_ref}: El 'Código PEP' es obligatorio para la obra '{name}'.",
                ErrorCodes.CODE_REQUIRED,
                {"line": line_ref, "name": name}
            )

        ProjectService.validate_code(code_val, required=True, entity_type="obra")

        # 3. Validar Proyecto (Macro)
        macro_name_raw = get_val('macro_name')
        if not macro_name_raw:
            raise ValidationError(
                f"{line_ref}: El campo 'Proyecto (Macro)' es obligatorio para la obra '{name}'.",
                ErrorCodes.MISSING_REQUIRED_FIELD,
                {"line": line_ref, "field": "macro_name", "name": name}
            )

        macro_clean = macro_name_raw.strip().upper()
        if macro_clean not in macros_map:
            raise ValidationError(
                f"{line_ref}: El Proyecto '{macro_name_raw}' NO EXISTE en el sistema. Créelo primero en Jerarquía.",
                ErrorCodes.MACRO_PROJECT_NOT_FOUND,
                {"line": line_ref, "macro_name": macro_name_raw}
            )

        macro_id = macros_map[macro_clean]

        # 4. Validar Fechas
        start_date = ProjectService.parse_date_strict(get_val('start_date'), 'Inicio')
        end_date = ProjectService.parse_date_strict(get_val('end_date'), 'Fin')

        # 5. Validar Presupuesto
        budget = ProjectService.validate_budget(get_val('budget'))

        # 6. Validar estado y fase
        status_val = get_val('status') or ProjectService.STATUS_ACTIVE
        phase_val = get_val('phase') or ProjectService.PHASE_NOT_STARTED

        return {
            "name": ProjectService.normalize_name(name),
            "code": ProjectService.normalize_code(code_val),
            "macro_project_id": macro_id,
            "address": ProjectService.normalize_address(get_val('address')),
            "status": status_val,
            "phase": phase_val,
            "start_date": start_date,
            "end_date": end_date,
            "budget": budget,
            "department": get_val('department'),
            "province": get_val('province'),
            "district": get_val('district')
        }

    # =====================================================
    # GENERACIÓN DE CSV (EXPORT)
    # =====================================================

    @staticmethod
    def generate_hierarchy_csv_content(hierarchy_data: List[Dict]) -> str:
        """
        Genera el contenido CSV para exportar la jerarquía.
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';', lineterminator='\n')

        # Headers
        headers = [
            "Dirección", "Cód. Dir",
            "Gerencia", "Cód. Ger",
            "Proyecto (Macro)", "Cód. Proy", "Centro de Costo"
        ]
        writer.writerow(headers)

        # Rows
        def clean(val):
            return str(val).strip() if val else ""

        for row in hierarchy_data:
            writer.writerow([
                clean(row.get('dir_name')),
                clean(row.get('dir_code')),
                clean(row.get('mgmt_name')),
                clean(row.get('mgmt_code')),
                clean(row.get('macro_name')),
                clean(row.get('macro_code')),
                clean(row.get('cost_center'))
            ])

        return output.getvalue()

    @staticmethod
    def generate_projects_csv_content(projects: List[Dict]) -> str:
        """
        Genera el contenido CSV para exportar las obras/proyectos.
        """
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';', lineterminator='\n')

        # Headers
        headers = [
            "Código PEP", "Nombre de Obra",
            "Dirección", "Gerencia", "Proyecto (Macro)",
            "Estado", "Fase",
            "Dirección Física", "Departamento", "Provincia", "Distrito",
            "Presupuesto (S/)", "Inicio", "Fin",
            "En Custodia (S/)", "Liquidado (S/)"
        ]
        writer.writerow(headers)

        # Rows
        for p in projects:
            row = [
                p.get('code') or "",
                p.get('name'),
                p.get('direction_name') or "",
                p.get('management_name') or "",
                p.get('macro_name') or "",
                p.get('status'),
                p.get('phase'),
                p.get('address') or "",
                p.get('department') or "",
                p.get('province') or "",
                p.get('district') or "",
                f"{float(p.get('budget', 0)):.2f}".replace('.', ','),
                p.get('start_date') or "",
                p.get('end_date') or "",
                f"{float(p.get('stock_value', 0)):.2f}".replace('.', ','),
                f"{float(p.get('liquidated_value', 0)):.2f}".replace('.', ',')
            ]
            writer.writerow(row)

        return output.getvalue()

    # =====================================================
    # MÉTODOS DE TRANSICIÓN DE FASE
    # =====================================================

    @staticmethod
    def determine_phase_transition(
        current_phase: str,
        internal_stock: float,
        has_consumption: bool
    ) -> Optional[str]:
        """
        Determina si debe haber transición de fase basado en el stock.
        Retorna la nueva fase o None si no hay cambio.
        """
        stock_is_zero = internal_stock <= 0.001

        # Regla A: De 'Sin Iniciar' a 'En Instalación' (Si recibe material)
        if current_phase == ProjectService.PHASE_NOT_STARTED and internal_stock > 0:
            return ProjectService.PHASE_IN_PROGRESS

        # Regla B: De 'Liquidado' a 'En Devolución' (Si le sobró material)
        elif current_phase == ProjectService.PHASE_LIQUIDATED and internal_stock > 0:
            return ProjectService.PHASE_RETURNING

        # Regla C: De 'Liquidado' a 'Por Facturar' (Si quedó limpio en 0)
        elif current_phase == ProjectService.PHASE_LIQUIDATED and stock_is_zero:
            return ProjectService.PHASE_TO_INVOICE

        # Regla D: De 'En Devolución' a 'Por Facturar' (Cuando termina de devolver)
        elif current_phase == ProjectService.PHASE_RETURNING and stock_is_zero:
            return ProjectService.PHASE_TO_INVOICE

        # Regla E: De 'En Instalación' a 'Por Facturar' (Liquidó todo de golpe)
        elif current_phase == ProjectService.PHASE_IN_PROGRESS and stock_is_zero and has_consumption:
            return ProjectService.PHASE_TO_INVOICE

        return None

    # =====================================================
    # BUILDERS DE FILTROS
    # =====================================================

    @staticmethod
    def build_project_filter_dict(
        status: Optional[str] = None,
        search: Optional[str] = None,
        direction_id: Optional[int] = None,
        management_id: Optional[int] = None,
        filter_code: Optional[str] = None,
        filter_macro: Optional[str] = None,
        filter_dept: Optional[str] = None,
        filter_prov: Optional[str] = None,
        filter_dist: Optional[str] = None,
        filter_direction: Optional[str] = None,
        filter_management: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Construye un diccionario de filtros normalizado para proyectos.
        """
        filters = {}

        if status:
            filters['status'] = status
        if search:
            filters['search'] = search.strip()
        if direction_id:
            filters['direction_id'] = direction_id
        if management_id:
            filters['management_id'] = management_id
        if filter_code:
            filters['filter_code'] = filter_code.strip()
        if filter_macro:
            filters['filter_macro'] = filter_macro.strip()
        if filter_dept:
            filters['filter_dept'] = filter_dept
        if filter_prov:
            filters['filter_prov'] = filter_prov
        if filter_dist:
            filters['filter_dist'] = filter_dist
        if filter_direction:
            filters['filter_direction'] = filter_direction.strip()
        if filter_management:
            filters['filter_management'] = filter_management.strip()

        return filters

    # =====================================================
    # VALIDACIONES DE CREACIÓN/ACTUALIZACIÓN
    # =====================================================

    @staticmethod
    def validate_direction_data(name: str, code: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Valida y normaliza datos para crear/actualizar una Dirección.
        """
        ProjectService.validate_name(name, "Dirección")
        clean_name = ProjectService.normalize_name(name)
        clean_code = ProjectService.normalize_code(code)
        return clean_name, clean_code

    @staticmethod
    def validate_management_data(
        name: str,
        direction_id: int,
        code: Optional[str] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Valida y normaliza datos para crear/actualizar una Gerencia.
        """
        ProjectService.validate_name(name, "Gerencia")
        if not direction_id:
            raise ValidationError(
                "La Dirección padre es obligatoria para crear una Gerencia.",
                ErrorCodes.HIERARCHY_MISSING_PARENT,
                {"field": "direction_id"}
            )
        clean_name = ProjectService.normalize_name(name)
        clean_code = ProjectService.normalize_code(code)
        return clean_name, clean_code

    @staticmethod
    def validate_macro_data(
        name: str,
        management_id: int,
        code: Optional[str] = None,
        cost_center: Optional[str] = None
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Valida y normaliza datos para crear/actualizar un Macro Proyecto.
        """
        ProjectService.validate_name(name, "Proyecto (Macro)")
        if not management_id:
            raise ValidationError(
                "La Gerencia padre es obligatoria para crear un Proyecto.",
                ErrorCodes.HIERARCHY_MISSING_PARENT,
                {"field": "management_id"}
            )
        clean_name = ProjectService.normalize_name(name)
        clean_code = ProjectService.normalize_code(code)
        clean_cc = ProjectService.normalize_code(cost_center)
        return clean_name, clean_code, clean_cc

    @staticmethod
    def validate_project_data(
        name: str,
        code: str,
        macro_project_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Valida y normaliza datos para crear/actualizar una Obra.
        """
        ProjectService.validate_name(name, "Obra")
        ProjectService.validate_code(code, required=True, entity_type="Obra")

        if not macro_project_id:
            raise ValidationError(
                "El Proyecto padre es obligatorio para crear una Obra.",
                ErrorCodes.HIERARCHY_MISSING_PARENT,
                {"field": "macro_project_id"}
            )

        ProjectService.validate_dates(start_date, end_date)

        return {
            'name': ProjectService.normalize_name(name),
            'code': ProjectService.normalize_code(code),
            'address': ProjectService.normalize_address(address)
        }
