#app/database/repositories/project_repo.py
import psycopg2
from ..core import execute_query, execute_commit_query

# --- 1. DIRECCIONES (Nivel 1) ---

def get_directions(company_id: int):
    return execute_query(
        "SELECT id, name, code FROM directions WHERE company_id = %s AND status = 'active' ORDER BY name",
        (company_id,), fetchall=True
    )

def create_direction(company_id: int, name: str, code: str = None):
    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    # ----------------
    try:
        return execute_commit_query(
            "INSERT INTO directions (company_id, name, code) VALUES (%s, %s, %s) RETURNING id",
            (company_id, clean_name, clean_code), fetchone=True
        )[0]
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Dirección con el nombre '{clean_name}'.")

def update_direction(direction_id: int, name: str, code: str):
    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    # ----------------
    try:
        execute_commit_query("UPDATE directions SET name=%s, code=%s WHERE id=%s", (clean_name, clean_code, direction_id))
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Dirección con el nombre '{clean_name}'.")

def delete_direction(direction_id: int):
    # Verificar si tiene hijos (Gerencias)
    check = execute_query("SELECT COUNT(*) as c FROM managements WHERE direction_id=%s", (direction_id,), fetchone=True)
    if check['c'] > 0:
        raise ValueError("No se puede eliminar: Esta Dirección tiene Gerencias asociadas.")
    execute_commit_query("DELETE FROM directions WHERE id=%s", (direction_id,))

# --- 2. GERENCIAS (Nivel 2) ---

def get_managements(company_id: int, direction_id: int = None):
    query = "SELECT id, name, code, direction_id FROM managements WHERE company_id = %s AND status = 'active'"
    params = [company_id]
    if direction_id:
        query += " AND direction_id = %s"
        params.append(direction_id)
    query += " ORDER BY name"
    return execute_query(query, tuple(params), fetchall=True)

def create_management(company_id: int, name: str, direction_id: int, code: str = None):
    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    # ----------------
    try:
        return execute_commit_query(
            "INSERT INTO managements (company_id, direction_id, name, code) VALUES (%s, %s, %s, %s) RETURNING id",
            (company_id, direction_id, clean_name, clean_code), fetchone=True
        )[0]
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Gerencia con el nombre '{clean_name}'.")

def update_management(mgmt_id: int, name: str, direction_id: int, code: str):
    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    # ----------------
    try:
        execute_commit_query("UPDATE managements SET name=%s, direction_id=%s, code=%s WHERE id=%s", (clean_name, direction_id, clean_code, mgmt_id))
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Gerencia con el nombre '{clean_name}'.")

def delete_management(mgmt_id: int):
    check = execute_query("SELECT COUNT(*) as c FROM macro_projects WHERE management_id=%s", (mgmt_id,), fetchone=True)
    if check['c'] > 0:
        raise ValueError("No se puede eliminar: Esta Gerencia tiene Macro Proyectos asociados.")
    execute_commit_query("DELETE FROM managements WHERE id=%s", (mgmt_id,))

# --- 3. MACRO PROYECTOS (Nivel 3) ---

def get_macro_projects(company_id: int, management_id: int = None):
    query = """
        SELECT mp.id, mp.name, mp.code, mp.management_id, m.name as management_name 
        FROM macro_projects mp
        JOIN managements m ON mp.management_id = m.id
        WHERE mp.company_id = %s AND mp.status = 'active'
    """
    params = [company_id]
    if management_id:
        query += " AND mp.management_id = %s"
        params.append(management_id)
    query += " ORDER BY mp.name"
    return execute_query(query, tuple(params), fetchall=True)

def create_macro_project(company_id: int, name: str, management_id: int, code: str = None):
    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    # ----------------
    try:
        return execute_commit_query(
            "INSERT INTO macro_projects (company_id, management_id, name, code) VALUES (%s, %s, %s, %s) RETURNING id",
            (company_id, management_id, clean_name, clean_code), fetchone=True
        )[0]
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe un Macro Proyecto con el nombre '{clean_name}'.")

def update_macro_project(macro_id: int, name: str, management_id: int, code: str):
    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    # ----------------
    try:
        execute_commit_query("UPDATE macro_projects SET name=%s, management_id=%s, code=%s WHERE id=%s", (clean_name, management_id, clean_code, macro_id))
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe un Macro Proyecto con el nombre '{clean_name}'.")

def delete_macro_project(macro_id: int):
    check = execute_query("SELECT COUNT(*) as c FROM projects WHERE macro_project_id=%s", (macro_id,), fetchone=True)
    if check['c'] > 0:
        raise ValueError("No se puede eliminar: Este Macro Proyecto tiene Obras activas.")
    execute_commit_query("DELETE FROM macro_projects WHERE id=%s", (macro_id,))

# --- 4. OBRAS / PROYECTOS (Nivel 4 - Entidad Operativa) ---

def get_projects(company_id: int, status: str = None, search: str = None, 
                 direction_id: int = None, management_id: int = None,
                 limit: int = 100, offset: int = 0):
    """
    Lista Obras con KPIs Operativos:
    - stock_value: Valor actual en almacén (Custodia).
    - liquidated_value: Valor acumulado de lo ya instalado/consumido.
    """
    query = """
        WITH ProjectStock AS (
            -- Lo que está VIVO en el inventario (En Custodia)
            SELECT 
                p.id,
                COALESCE(SUM(sq.quantity * prod.standard_price), 0) as stock_value
            FROM projects p
            LEFT JOIN stock_quants sq ON p.id = sq.project_id
            LEFT JOIN products prod ON sq.product_id = prod.id
            WHERE p.company_id = %s
            GROUP BY p.id
        ),
        ProjectConsumed AS (
            -- Lo que ya se LIQUIDÓ (Salió del proyecto hacia Cliente/Consumo)
            SELECT 
                sm.project_id,
                COALESCE(SUM(sm.quantity_done * sm.price_unit), 0) as liquidated_value
            FROM stock_moves sm
            JOIN locations l_dest ON sm.location_dest_id = l_dest.id
            WHERE sm.state = 'done' 
              AND l_dest.category IN ('CLIENTE', 'CONTRATA CLIENTE') -- Salidas a cliente
              AND sm.project_id IS NOT NULL
            GROUP BY sm.project_id
        )
        SELECT 
            p.id, p.name, p.code, p.status, p.phase, p.address,
            p.department, p.province, p.district,
            p.budget, p.start_date, p.end_date,
            p.macro_project_id,
            
            mp.name as macro_name,
            m.name as management_name,
            d.name as direction_name,
            
            -- KPIs CALCULADOS
            COALESCE(ps.stock_value, 0) as stock_value,
            COALESCE(pc.liquidated_value, 0) as liquidated_value

        FROM projects p
        LEFT JOIN macro_projects mp ON p.macro_project_id = mp.id
        LEFT JOIN managements m ON mp.management_id = m.id
        LEFT JOIN directions d ON m.direction_id = d.id
        LEFT JOIN ProjectStock ps ON p.id = ps.id
        LEFT JOIN ProjectConsumed pc ON p.id = pc.project_id
        WHERE p.company_id = %s
    """
    params = [company_id, company_id]
    
    # ... (El resto de los filtros se mantiene IGUAL) ...
    if direction_id:
        query += " AND m.direction_id = %s"
        params.append(direction_id)
    
    if management_id:
        query += " AND p.management_id = %s"
        params.append(management_id)

    if status:
        query += " AND p.status = %s"
        params.append(status)
    
    if search:
        query += " AND (p.name ILIKE %s OR p.code ILIKE %s)"
        search_term = f"%{search}%"
        params.extend([search_term, search_term])
    
    query += " ORDER BY p.status, p.name LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    return execute_query(query, tuple(params), fetchall=True)

def create_project(company_id: int, name: str, macro_project_id: int, code: str = None, address: str = None, 
                   department: str = None, province: str = None, district: str = None, # <--- NUEVOS
                   budget: float = 0, start_date=None, end_date=None):

    # --- LIMPIEZA ---
    clean_name = name.strip().upper()
    clean_code = code.strip().upper() if code else None
    clean_address = address.strip().upper() if address else None

    try:
        return execute_commit_query(
            """INSERT INTO projects (
                   company_id, macro_project_id, name, code, address, 
                   department, province, district, 
                   budget, start_date, end_date, 
                   status, phase
               ) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', 'Sin Iniciar') 
               RETURNING id""",
            (company_id, macro_project_id, clean_name, clean_code, clean_address, 
             department, province, district, 
             budget, start_date, end_date), 
            fetchone=True
        )[0]

    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Obra con el nombre '{clean_name}'.")

def update_project(project_id: int, data: dict):
    """Actualiza campos editables con limpieza automática."""
    allowed = {
        'name', 'code', 'address', 'status', 'phase', 'macro_project_id', 
        'budget', 'start_date', 'end_date',
        'department', 'province', 'district' # <--- NUEVOS
    }
    updates = {}
    for k, v in data.items():
        if k in allowed:
            # Si el campo es texto (nombre, codigo, direccion), lo limpiamos
            if k in ['name', 'code', 'address'] and isinstance(v, str):
                updates[k] = v.strip().upper()
            else:
                updates[k] = v
    if not updates: return
    set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
    params = list(updates.values()) + [project_id]
    try:
        execute_commit_query(f"UPDATE projects SET {set_clause} WHERE id = %s", tuple(params))
    except psycopg2.errors.UniqueViolation:
         raise ValueError("El nombre o código ya está en uso por otra obra.")

def delete_project(project_id: int):
    # Verificar uso (Integridad Referencial lógica)
    usage = execute_query(
        "SELECT COUNT(*) as c FROM stock_moves WHERE project_id = %s UNION ALL SELECT COUNT(*) as c FROM stock_quants WHERE project_id = %s", 
        (project_id, project_id), fetchall=True
    )
    if sum(row['c'] for row in usage) > 0:
        execute_commit_query("UPDATE projects SET status = 'closed' WHERE id = %s", (project_id,))
        return False, "La obra tiene historial. Se ha marcado como 'Cerrado'."
    
    execute_commit_query("DELETE FROM projects WHERE id = %s", (project_id,))
    return True, "Obra eliminada."

# --- MÁQUINA DE ESTADOS (Lógica de Negocio Automática) ---

def check_and_update_project_phase(project_id: int):
    """
    [AUTOMATIZACIÓN] Revisa el stock y actualiza la fase de la obra.
    Se debe llamar después de cualquier movimiento de stock (IN/OUT) relacionado a un proyecto.
    """
    if not project_id: return

    # 1. Obtener estado actual
    proj = execute_query("SELECT phase, status FROM projects WHERE id = %s", (project_id,), fetchone=True)
    if not proj or proj['status'] != 'active': return
    
    current_phase = proj['phase']
    
    # 2. Calcular Stock Total en Custodia
    stock_res = execute_query("SELECT SUM(quantity) as total FROM stock_quants WHERE project_id = %s", (project_id,), fetchone=True)
    total_stock = stock_res['total'] if stock_res and stock_res['total'] else 0
    
    new_phase = current_phase

    # 3. REGLAS DE TRANSICIÓN
    
    # Regla A: De 'Sin Iniciar' a 'En Instalación' (Si recibe material)
    if current_phase == 'Sin Iniciar' and total_stock > 0:
        new_phase = 'En Instalación'
        
    # Regla B: De 'Liquidado' a 'En Devolución' (Si le sobró material)
    elif current_phase == 'Liquidado' and total_stock > 0:
        new_phase = 'En Devolución'
        
    # Regla C: De 'Liquidado' a 'Por Facturar' (Si quedó limpio en 0)
    elif current_phase == 'Liquidado' and total_stock <= 0.001:
        new_phase = 'Por Facturar'

    # Regla D: De 'En Devolución' a 'Por Facturar' (Cuando termina de devolver todo)
    elif current_phase == 'En Devolución' and total_stock <= 0.001:
        new_phase = 'Por Facturar'

    # 4. Aplicar cambio si hubo transición
    if new_phase != current_phase:
        print(f"[AUTO-PHASE] Obra {project_id}: {current_phase} -> {new_phase}")
        execute_commit_query("UPDATE projects SET phase = %s WHERE id = %s", (new_phase, project_id))