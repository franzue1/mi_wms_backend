# app/schemas.py
from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime, date

# --- Schemas para Productos ---

class ProductBase(BaseModel):
    name: str
    sku: str
    category_id: Optional[int] = None
    uom_id: Optional[int] = None
    tracking: Optional[str] = "none"
    ownership: Optional[str] = "owned"
    standard_price: Optional[float] = 0.0

class ProductCreate(ProductBase):
    pass

class ProductUpdate(BaseModel):
    name: Optional[str] = None
    sku: Optional[str] = None
    category_id: Optional[int] = None
    uom_id: Optional[int] = None
    tracking: Optional[str] = None
    ownership: Optional[str] = None
    standard_price: Optional[float] = None

class ProductResponse(ProductBase):
    id: int
    company_id: int
    category_name: Optional[str] = None
    uom_name: Optional[str] = None

    class Config:
        from_attributes = True # <-- ESTA ES LA CORRECCIÓN (antes orm_mode)
# --- Schemas para Almacenes ---
class WarehouseBase(BaseModel):
    name: str
    code: str
    category_id: int
    status: Optional[str] = "activo"
    social_reason: Optional[str] = None
    ruc: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None

class WarehouseCreate(WarehouseBase):
    pass

class WarehouseUpdate(BaseModel):
    # Todos opcionales para la actualización
    name: Optional[str] = None
    code: Optional[str] = None
    category_id: Optional[int] = None
    status: Optional[str] = None
    social_reason: Optional[str] = None
    ruc: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None

class WarehouseResponse(WarehouseBase):
    id: int
    company_id: int
    category_name: Optional[str] = None # Campo del JOIN

    class Config:
        from_attributes = True # (La corrección de Pydantic v2)

class WarehouseSimple(BaseModel):
    """Schema simple para dropdowns."""
    id: int
    name: str
    code: str

    class Config:
        from_attributes = True

# --- Schemas para Socios (Proveedores/Clientes) ---
class PartnerBase(BaseModel):
    name: str
    category_id: int
    social_reason: Optional[str] = None
    ruc: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None

class PartnerCreate(PartnerBase):
    pass

class PartnerUpdate(BaseModel):
    # Todos opcionales para la actualización
    name: Optional[str] = None
    category_id: Optional[int] = None
    social_reason: Optional[str] = None
    ruc: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None

class PartnerResponse(PartnerBase):
    id: int
    company_id: int
    category_name: Optional[str] = None # Campo del JOIN

    class Config:
        from_attributes = True

# --- Schemas para Ubicaciones ---
class LocationBase(BaseModel):
    name: str
    path: str
    type: str
    category: Optional[str] = None
    warehouse_id: Optional[int] = None

class LocationCreate(LocationBase):
    pass

class LocationUpdate(BaseModel):
    # Todos opcionales para la actualización
    name: Optional[str] = None
    path: Optional[str] = None
    type: Optional[str] = None
    category: Optional[str] = None
    warehouse_id: Optional[int] = None

class LocationResponse(LocationBase):
    id: int
    company_id: int
    warehouse_name: Optional[str] = None # Campo del JOIN

    class Config:
        from_attributes = True

# --- Schemas para Reportes ---

class StockReportResponse(BaseModel):
    # Definimos todos los campos que la consulta devuelve
    product_id: int
    sku: str
    product_name: str
    category_name: Optional[str] = None
    warehouse_id: int
    warehouse_name: str
    location_id: int
    location_name: str
    uom_name: Optional[str] = None
    physical_quantity: float
    reserved_quantity: float
    available_quantity: float

    class Config:
        from_attributes = True

# --- Schemas para Operaciones (Pickings) ---
class StockMoveResponse(BaseModel):
    """ Schema para una línea de movimiento (stock_move) """
    id: int
    product_id: int
    sku: str
    name: str # product name
    product_uom_qty: float
    tracking: str
    uom_name: Optional[str] = None
    price_unit: Optional[float] = None
    cost_at_adjustment: Optional[float] = None

    class Config:
        from_attributes = True

class PickingResponse(BaseModel):
    """ Schema para la cabecera de un albarán (picking) """
    id: int
    company_id: int
    name: str
    state: str
    picking_type_id: int
    type_code: str # 'IN', 'OUT', 'INT'
    location_src_id: Optional[int] = None
    location_dest_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    responsible_user: Optional[str] = None
    custom_operation_type: Optional[str] = None
    partner_id: Optional[int] = None
    partner_ref: Optional[str] = None
    purchase_order: Optional[str] = None
    date_transfer: Optional[date] = None
    service_act_number: Optional[str] = None
    attention_date: Optional[date] = None
    # Lista de líneas de movimiento
    moves: List[StockMoveResponse] = []

    class Config:
        from_attributes = True

class ValidateRequest(BaseModel):
    """ Schema para el body de la petición de validación """
    # { "move_id": { "serial_name": qty, "serial_2": qty }, ... }
    moves_with_tracking: dict[int, dict[str, float]]

# --- Schemas para Liquidaciones (Work Orders) ---
class WorkOrderBase(BaseModel):
    ot_number: str
    customer_name: str
    address: Optional[str] = None
    service_type: Optional[str] = None
    job_type: Optional[str] = None

class WorkOrderCreate(WorkOrderBase):
    pass

class WorkOrderResponse(WorkOrderBase):
    id: int
    company_id: int
    phase: str
    date_registered: datetime
    warehouse_name: Optional[str] = None
    location_src_path: Optional[str] = None
    service_act_number: Optional[str] = None
    attention_date_str: Optional[str] = None

    class Config:
        from_attributes = True

# Schemas para guardar/liquidar (más complejos)
class StockMoveLineData(BaseModel):
    # Esto es solo un marcador, la clave es el nombre de la serie/lote
    # y el valor es la cantidad (float).
    pass

class StockMoveData(BaseModel):
    product_id: int
    quantity: float
    tracking_data: Optional[dict[str, float]] = {}
    cost_at_adjustment: Optional[float] = 0.0

class PickingSaveData(BaseModel):
    warehouse_id: int
    location_src_id: Optional[int] = None # Requerido para 'OUT'
    date_attended_db: Optional[date] = None
    service_act_number: Optional[str] = None
    lines_data: List[StockMoveData] = []

class WorkOrderSaveRequest(BaseModel):
    # Campos de la OT a actualizar
    wo_updates: dict
    # Datos para el picking 'OUT' (Consumo)
    consumo_data: PickingSaveData
    # Datos para el picking 'RET' (Retiro), puede ser None
    retiro_data: Optional[PickingSaveData] = None

# --- Schemas para Ajustes de Inventario ---
class AdjustmentListResponse(BaseModel):
    """Schema para la fila de la lista de Ajustes."""
    id: int
    company_id: int
    name: str
    state: str
    date: Optional[str] = None
    responsible_user: Optional[str] = None
    adjustment_reason: Optional[str] = None
    notes: Optional[str] = None

    class Config:
        from_attributes = True

class AdjustmentSaveRequest(BaseModel):
    """Datos necesarios para guardar un borrador de ajuste."""
    header_data: dict # Ej: {"location_src_id": 5, "adjustment_reason": "Merma"}
    lines_data: List[StockMoveData] # Reutilizamos el schema de líneas de Liquidaciones

# --- Schemas para Dashboard (VERSIÓN ACTUALIZADA) ---
class DashboardKPIs(BaseModel):
    IN: int = 0
    OUT: int = 0
    INT: int = 0

class InventoryValueKPIs(BaseModel):
    total: float = 0.0
    pri: float = 0.0 # Almacen Principal
    tec: float = 0.0 # Contratista

class ThroughputDataPoint(BaseModel):
    # Usamos string para la fecha (ej. "Mon", "Tue") o un objeto date
    day: str
    count: int

class AgingDataPoint(BaseModel):
    # Usamos un dict { "0-30 días": 10, "+90 días": 5 }
    # Pydantic puede manejar un Dict[str, float]
    pass # Usaremos un Dict[str, float] directamente

class DashboardResponse(BaseModel):
    # Los KPIs que ya teníamos
    pending_kpis: DashboardKPIs
    value_kpis: InventoryValueKPIs
    
    pending_ots: int = 0
    throughput_chart: List[ThroughputDataPoint] = []
    aging_chart: Dict[str, float] = {} # ej: {"0-30 días": 10.0, ...}

# --- Schema para Reporte de Antigüedad ---

class AgingDetailResponse(BaseModel):
    sku: str
    product_name: str
    lot_name: str
    warehouse_id: int
    warehouse_name: str
    entry_date: Optional[date] = None
    aging_days: Optional[int] = 0
    quantity: float
    unit_cost: float
    total_value: float

    class Config:
        from_attributes = True

# --- Schema para Reporte de Cobertura ---

class CoverageReportResponse(BaseModel):
    sku: str
    product_name: str
    current_stock: float
    total_consumption: float
    avg_daily_consumption: float
    coverage_days: float

    class Config:
        from_attributes = True

# --- Schemas para Configuración ---
# Genérico para tablas simples (ID, Name)
class ConfigBase(BaseModel):
    name: str

class ConfigCreate(ConfigBase):
    pass

class ConfigResponse(ConfigBase):
    id: int

    class Config:
        from_attributes = True

# Genérico para tablas con Descripción (Roles)
class ConfigDescBase(BaseModel):
    name: str
    description: Optional[str] = None

class ConfigDescCreate(ConfigDescBase):
    pass

class ConfigDescResponse(ConfigDescBase):
    id: int

    class Config:
        from_attributes = True

# --- Schemas para Administración (RBAC) ---

class PermissionResponse(BaseModel):
    id: int
    key: str
    description: Optional[str] = None
    
    class Config:
        from_attributes = True

class UserBase(BaseModel):
    username: str
    full_name: Optional[str] = None
    role_id: int
    is_active: bool = True

class UserCreate(UserBase):
    password: str # Contraseña en texto plano
    company_ids: List[int] = []

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    role_id: Optional[int] = None
    is_active: Optional[bool] = None
    password: Optional[str] = None # Para cambiar la contraseña (opcional)
    company_ids: List[int] = []

class UserResponse(UserBase):
    id: int
    role_name: Optional[str] = None # Campo del JOIN
    # NOTA: 'hashed_password' se omite intencionalmente por seguridad
    company_ids: List[int] = []

    class Config:
        from_attributes = True

class PermissionMatrix(BaseModel):
    # { "role_id_1": {"perm_key_1": true, "perm_key_2": false}, ... }
    # Usamos un Dict[int, Dict[str, bool]] para mapear role_id -> perm_key -> bool
    matrix: Dict[int, Dict[str, bool]]

class PermissionMatrixUpdate(BaseModel):
    permission_id: int
    has_permission: bool
# --- Schemas para Reporte Kardex ---

class KardexSummaryResponse(BaseModel):
    product_id: int
    sku: str
    product_name: str
    category_name: Optional[str] = None
    initial_balance: float
    initial_value: float
    total_in: float
    total_value_in: float
    total_out: float
    total_value_out: float
    final_balance: float
    final_value: float

    class Config:
        from_attributes = True

class KardexDetailResponse(BaseModel):
    # Campos que vienen de la consulta get_product_kardex
    id: int # ID del Picking
    date: datetime
    operation_ref: str
    custom_operation_type: Optional[str] = None
    purchase_order: Optional[str] = None
    adjustment_reason: Optional[str] = None
    type_code: str
    product_sku: str
    product_name: str
    category_name: Optional[str] = None
    date_transfer: Optional[date] = None
    partner_ref: Optional[str] = None
    partner_name: Optional[str] = None
    affected_warehouse: Optional[str] = None
    quantity_in: float
    quantity_out: float
    initial_value_in: float
    price_unit: Optional[float] = None
    cost_at_adjustment: Optional[float] = None
    almacen_origen: Optional[str] = None
    ubicacion_origen: Optional[str] = None
    almacen_destino: Optional[str] = None
    ubicacion_destino: Optional[str] = None
    location_src_path: Optional[str] = None
    location_dest_path: Optional[str] = None

    class Config:
        from_attributes = True

class StockDetailResponse(BaseModel):
    """
    Schema para el Reporte de Stock Detallado (por Serie/Lote).
    Corresponde a la Pestaña 2 de la vista de reportes.
    """
    product_id: int
    location_id: int
    lot_id: Optional[int] = None
    warehouse_name: str
    location_name: str
    sku: str
    product_name: str
    category_name: Optional[str] = None
    lot_name: Optional[str] = None
    physical_quantity: float
    reserved_quantity: float
    uom_name: Optional[str] = None

    class Config:
        from_attributes = True # Para que funcione con los objetos de la BD

class LiquidationDropdowns(BaseModel):
    """
    Schemas para los dropdowns de la vista de liquidación.
    (Versión mejorada y movida al final del archivo)
    """
    warehouses: List[WarehouseSimple] = []
    locations: List[LocationResponse] = []
    all_products: List[ProductResponse] = []

    class Config:
        from_attributes = True

class LiquidationDetailsResponse(BaseModel):
    """
    El JSON 'combo' completo para la vista de detalle de liquidación.
    """
    wo_data: WorkOrderResponse
    picking_consumo: Optional[PickingResponse] = None
    moves_consumo: List[StockMoveResponse] = []
    serials_consumo: Dict[int, Dict[str, float]] = {}
    
    picking_retiro: Optional[PickingResponse] = None
    moves_retiro: List[StockMoveResponse] = []
    serials_retiro: Dict[int, Dict[str, float]] = {}
    
    dropdowns: LiquidationDropdowns
    
    class Config:
        from_attributes = True

class StockCheckRequest(BaseModel):
    location_id: int
    product_ids: List[int]