#app/database/repositories/project_repo.py
import psycopg2
from ..core import execute_query, execute_commit_query, get_db_connection, return_db_connection

# --- 1. DIRECCIONES (Nivel 1) ---

def get_directions(company_id: int):
    return execute_query(
        "SELECT id, name, code FROM directions WHERE company_id = %s AND status = 'active' ORDER BY name",
        (company_id,), fetchall=True
    )

def create_direction(company_id: int, name: str, code: str = None):
    """Crea una Dirección. La validación/normalización se hace en ProjectService."""
    try:
        return execute_commit_query(
            "INSERT INTO directions (company_id, name, code) VALUES (%s, %s, %s) RETURNING id",
            (company_id, name, code), fetchone=True
        )[0]
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Dirección con el nombre '{name}'.")

def update_direction(direction_id: int, name: str, code: str):
    """Actualiza una Dirección. La validación/normalización se hace en ProjectService."""
    try:
        execute_commit_query("UPDATE directions SET name=%s, code=%s WHERE id=%s", (name, code, direction_id))
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Dirección con el nombre '{name}'.")

def delete_direction(direction_id: int):
    # 1. Validación: ¿Tiene hijos (Gerencias)?
    check = execute_query(
        "SELECT COUNT(*) as c FROM managements WHERE direction_id=%s", 
        (direction_id,), fetchone=True
    )
    if check['c'] > 0:
        # Esto enviará un error 400 al frontend con este mensaje exacto
        raise ValueError("No se puede eliminar: Esta Dirección tiene Gerencias asociadas.")
    
    # 2. Si pasa la validación, borramos
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
    """Crea una Gerencia. La validación/normalización se hace en ProjectService."""
    try:
        return execute_commit_query(
            "INSERT INTO managements (company_id, direction_id, name, code) VALUES (%s, %s, %s, %s) RETURNING id",
            (company_id, direction_id, name, code), fetchone=True
        )[0]
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Gerencia con el nombre '{name}'.")

def update_management(mgmt_id: int, name: str, direction_id: int, code: str):
    """Actualiza una Gerencia. La validación/normalización se hace en ProjectService."""
    try:
        execute_commit_query("UPDATE managements SET name=%s, direction_id=%s, code=%s WHERE id=%s", (name, direction_id, code, mgmt_id))
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe una Gerencia con el nombre '{name}'.")

def delete_management(mgmt_id: int):
    # 1. Validación: ¿Tiene hijos (Proyectos/Macros)?
    check = execute_query(
        "SELECT COUNT(*) as c FROM macro_projects WHERE management_id=%s", 
        (mgmt_id,), fetchone=True
    )
    if check['c'] > 0:
        raise ValueError("No se puede eliminar: Esta Gerencia tiene Proyectos asociados.")
        
    execute_commit_query("DELETE FROM managements WHERE id=%s", (mgmt_id,))

# --- 3. MACRO PROYECTOS (Nivel 3) ---

def create_macro_project(company_id: int, name: str, management_id: int, code: str = None, cost_center: str = None):
    """Crea un Macro Proyecto. La validación/normalización se hace en ProjectService."""
    try:
        return execute_commit_query(
            """INSERT INTO macro_projects (company_id, management_id, name, code, cost_center)
               VALUES (%s, %s, %s, %s, %s)
               RETURNING id""",
            (company_id, management_id, name, code, cost_center), fetchone=True
        )[0]
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe un Proyecto con el nombre '{name}'.")

def update_macro_project(macro_id: int, name: str, management_id: int, code: str, cost_center: str = None):
    """Actualiza un Macro Proyecto. La validación/normalización se hace en ProjectService."""
    try:
        execute_commit_query(
            "UPDATE macro_projects SET name=%s, management_id=%s, code=%s, cost_center=%s WHERE id=%s",
            (name, management_id, code, cost_center, macro_id)
        )
    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"Ya existe un Proyecto con el nombre '{name}'.")

def get_macro_projects(company_id: int, management_id: int = None):
    # Agregamos mp.cost_center al SELECT
    query = """
        SELECT mp.id, mp.name, mp.code, mp.cost_center, mp.management_id, m.name as management_name 
        FROM macro_projects mp
        JOIN managements m ON mp.management_id = m.id
        WHERE mp.company_id = %s 
    """
    # Nota: Eliminé "AND mp.status = 'active'" para ser consistentes con el esquema nuevo.
    
    params = [company_id]
    if management_id:
        query += " AND mp.management_id = %s"
        params.append(management_id)
    
    query += " ORDER BY mp.name"
    return execute_query(query, tuple(params), fetchall=True)

def delete_macro_project(macro_id: int):
    # 1. Validación: ¿Tiene hijos (Obras/Projects)?
    check = execute_query(
        "SELECT COUNT(*) as c FROM projects WHERE macro_project_id=%s", 
        (macro_id,), fetchone=True
    )
    if check['c'] > 0:
        raise ValueError("No se puede eliminar: Este Proyecto tiene Obras activas.")
        
    execute_commit_query("DELETE FROM macro_projects WHERE id=%s", (macro_id,))

# --- 4. OBRAS / PROYECTOS (Nivel 4 - Entidad Operativa) ---

def get_projects(company_id: int, status: str = None, search: str = None, 
                 direction_id: int = None, management_id: int = None,
                 # [NUEVOS FILTROS DE COLUMNA]
                 filter_code: str = None, 
                 filter_macro: str = None,
                 filter_dept: str = None,
                 filter_prov: str = None,
                 filter_dist: str = None,
                 filter_direction: str = None, 
                 filter_management: str = None,
                 
                 limit: int = 100, offset: int = 0,
                 sort_by: str = None, ascending: bool = True):
    """
    Lista Obras con KPIs y Nombre Compuesto (PEP + Macro).
    """
    query = """
        WITH ProjectStock AS (
            SELECT 
                sq.project_id, 
                COALESCE(SUM(sq.quantity * prod.standard_price), 0) as stock_value
            FROM stock_quants sq
            JOIN locations l ON sq.location_id = l.id
            JOIN products prod ON sq.product_id = prod.id
            WHERE l.type = 'internal' 
              AND sq.quantity > 0
            GROUP BY sq.project_id
        ),
        ProjectConsumed AS (
            SELECT 
                sm.project_id, COALESCE(SUM(sm.quantity_done * sm.price_unit), 0) as liquidated_value
            FROM stock_moves sm
            JOIN locations l_dest ON sm.location_dest_id = l_dest.id
            WHERE sm.state = 'done' AND l_dest.category IN ('CLIENTE', 'CONTRATA CLIENTE') AND sm.project_id IS NOT NULL
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
            COALESCE(ps.stock_value, 0) as stock_value,
            COALESCE(pc.liquidated_value, 0) as liquidated_value,

            -- [NUEVO] Columna compuesta para Dropdowns: "PEP (Macro)"
            CONCAT(p.code, ' (', mp.name, ')') as full_name_display

        FROM projects p
        LEFT JOIN macro_projects mp ON p.macro_project_id = mp.id
        LEFT JOIN managements m ON mp.management_id = m.id
        LEFT JOIN directions d ON m.direction_id = d.id
        LEFT JOIN ProjectStock ps ON p.id = ps.project_id 
        LEFT JOIN ProjectConsumed pc ON p.id = pc.project_id
        WHERE p.company_id = %s
    """
    params = [company_id]
    
    # --- FILTROS EXISTENTES ---
    if direction_id: query += " AND m.direction_id = %s"; params.append(direction_id)
    if management_id: query += " AND p.management_id = %s"; params.append(management_id)
    if status: query += " AND p.status = %s"; params.append(status)
    if search:
        query += " AND (p.name ILIKE %s OR p.code ILIKE %s)"
        term = f"%{search}%"; params.extend([term, term])

    # --- FILTROS DE COLUMNA ---
    if filter_code: query += " AND p.code ILIKE %s"; params.append(f"%{filter_code}%")
    if filter_macro: query += " AND mp.name ILIKE %s"; params.append(f"%{filter_macro}%")
    if filter_dept: query += " AND p.department = %s"; params.append(filter_dept)
    if filter_prov: query += " AND p.province = %s"; params.append(filter_prov)
    if filter_dist: query += " AND p.district = %s"; params.append(filter_dist)
    if filter_direction: query += " AND d.name ILIKE %s"; params.append(f"%{filter_direction}%")
    if filter_management: query += " AND m.name ILIKE %s"; params.append(f"%{filter_management}%")

    # --- ORDENAMIENTO ---
    sort_map = {
        'id': 'p.id', 'code': 'p.code', 'name': 'p.name', 'phase': 'p.phase',
        'start_date': 'p.start_date', 'department': 'p.department', 'budget': 'p.budget',
        'macro_name': 'mp.name', 'management_name': 'm.name', 'direction_name': 'd.name',
        'stock_value': 'stock_value', 'liquidated_value': 'liquidated_value'
    }
    order_col = sort_map.get(sort_by, 'p.name')
    direction = "ASC" if ascending else "DESC"
    
    query += f" ORDER BY {order_col} {direction}, p.id ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    return execute_query(query, tuple(params), fetchall=True)

def get_projects_count(company_id: int, status: str = None, search: str = None, 
                       direction_id: int = None, management_id: int = None,
                       filter_code: str = None, filter_macro: str = None,
                       filter_dept: str = None, filter_prov: str = None, filter_dist: str = None,
                       filter_direction: str = None, filter_management: str = None): # <--- NUEVOS
    # Necesitamos los JOINS para poder filtrar por nombre de Macro Proyecto o Jerarquía
    query = """
        SELECT COUNT(*) as total 
        FROM projects p
        LEFT JOIN macro_projects mp ON p.macro_project_id = mp.id
        LEFT JOIN managements m ON mp.management_id = m.id
        LEFT JOIN directions d ON m.direction_id = d.id 
        WHERE p.company_id = %s
    """
    params = [company_id]
    
    # --- APLICAR MISMOS FILTROS QUE EN EL LISTADO ---
    
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
        term = f"%{search}%"
        params.extend([term, term])

    # --- FILTROS DE COLUMNA ESPECÍFICOS ---
    if filter_code:
        query += " AND p.code ILIKE %s"
        params.append(f"%{filter_code}%")
    
    if filter_macro:
        query += " AND mp.name ILIKE %s"
        params.append(f"%{filter_macro}%")

    if filter_dept: query += " AND p.department = %s"; params.append(filter_dept)
    if filter_prov: query += " AND p.province = %s"; params.append(filter_prov)
    if filter_dist: query += " AND p.district = %s"; params.append(filter_dist)
    if filter_direction:
        query += " AND d.name ILIKE %s"; params.append(f"%{filter_direction}%")
    if filter_management:
        query += " AND m.name ILIKE %s"; params.append(f"%{filter_management}%")
        
    res = execute_query(query, tuple(params), fetchone=True)
    return res['total'] if res else 0
        
def create_project(company_id: int, name: str, macro_project_id: int, code: str = None, address: str = None,
                   department: str = None, province: str = None, district: str = None,
                   budget: float = 0, start_date=None, end_date=None):
    """Crea una Obra. La validación/normalización se hace en ProjectService."""
    # Verificar que el Macro Proyecto pertenezca a la compañía (integridad referencial)
    check = execute_query(
        "SELECT id FROM macro_projects WHERE id = %s AND company_id = %s",
        (macro_project_id, company_id), fetchone=True
    )
    if not check:
        raise ValueError("El Proyecto Padre seleccionado no pertenece a esta compañía o no existe.")

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
            (company_id, macro_project_id, name, code, address,
             department, province, district,
             budget, start_date, end_date),
            fetchone=True
        )[0]

    except psycopg2.errors.UniqueViolation:
        raise ValueError(f"El Código PEP '{code}' ya existe en este Macro Proyecto.")
    except psycopg2.errors.NotNullViolation:
        raise ValueError("El Código PEP es obligatorio.")

def update_project(project_id: int, data: dict):
    """
    Actualiza campos editables de una Obra.
    La validación/normalización de datos se debe hacer en ProjectService.
    """
    allowed = {
        'name', 'code', 'address', 'status', 'phase', 'macro_project_id',
        'budget', 'start_date', 'end_date',
        'department', 'province', 'district'
    }

    # Verificación de integridad referencial para macro_project_id
    if 'macro_project_id' in data:
        new_macro_id = data['macro_project_id']
        current_proj = execute_query("SELECT company_id FROM projects WHERE id=%s", (project_id,), fetchone=True)

        if current_proj:
            cid = current_proj['company_id']
            check_macro = execute_query(
                "SELECT id FROM macro_projects WHERE id=%s AND company_id=%s",
                (new_macro_id, cid), fetchone=True
            )
            if not check_macro:
                raise ValueError("Operación rechazada: El Proyecto Padre seleccionado no pertenece a la compañía.")

    updates = {}
    for k, v in data.items():
        if k in allowed:
            updates[k] = v

    if not updates:
        return

    set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
    params = list(updates.values()) + [project_id]

    try:
        execute_commit_query(f"UPDATE projects SET {set_clause} WHERE id = %s", tuple(params))
    except psycopg2.errors.UniqueViolation:
        raise ValueError("Operación rechazada: El Código PEP ya existe dentro del Proyecto seleccionado.")

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
    [AUTOMATIZACIÓN - CORREGIDA] Revisa el stock y actualiza la fase de la obra.
    Se debe llamar después de cualquier movimiento de stock (IN/OUT) relacionado a un proyecto.
    """
    if not project_id: return

    # 1. Obtener estado actual
    proj = execute_query("SELECT phase, status FROM projects WHERE id = %s", (project_id,), fetchone=True)
    if not proj or proj['status'] != 'active': return
    
    current_phase = proj['phase']
    
    # 2. Calcular Stock Total en Custodia (SOLO INTERNO)
    # [CORRECCIÓN CRÍTICA] Agregamos el JOIN con locations y el filtro internal.
    # Si no hacemos esto, el stock entregado al cliente cuenta como 'En Custodia'
    # y la obra nunca pasaría a 'Por Facturar'.
    stock_res = execute_query("""
        SELECT SUM(sq.quantity) as total 
        FROM stock_quants sq
        JOIN locations l ON sq.location_id = l.id
        WHERE sq.project_id = %s AND l.type = 'internal'
    """, (project_id,), fetchone=True)
    
    total_stock = stock_res['total'] if stock_res and stock_res['total'] else 0
    
    new_phase = current_phase

    # 3. REGLAS DE TRANSICIÓN
    
    # Regla A: De 'Sin Iniciar' a 'En Instalación' (Si recibe material en custodia)
    if current_phase == 'Sin Iniciar' and total_stock > 0:
        new_phase = 'En Instalación'
        
    # Regla B: De 'Liquidado' a 'En Devolución' (Si le sobró material y volvió a custodia)
    elif current_phase == 'Liquidado' and total_stock > 0:
        new_phase = 'En Devolución'
        
    # Regla C: De 'Liquidado' a 'Por Facturar' (Si quedó limpio en 0)
    elif current_phase == 'Liquidado' and total_stock <= 0.001:
        new_phase = 'Por Facturar'

    # Regla D: De 'En Devolución' a 'Por Facturar' (Cuando termina de devolver todo)
    elif current_phase == 'En Devolución' and total_stock <= 0.001:
        new_phase = 'Por Facturar'
        
    # Regla E: De 'En Instalación' a 'Por Facturar' (Caso raro: liquidó todo de golpe sin pasar por 'Liquidado')
    elif current_phase == 'En Instalación' and total_stock <= 0.001:
        # Verificamos si hubo consumo para no regresarlo a 'Sin Iniciar' por error
        has_consumption = execute_query(
            "SELECT 1 FROM stock_moves WHERE project_id=%s AND state='done' LIMIT 1", 
            (project_id,), fetchone=True
        )
        if has_consumption:
            new_phase = 'Por Facturar'

    # 4. Aplicar cambio si hubo transición
    if new_phase != current_phase:
        print(f"[AUTO-PHASE] Obra {project_id}: {current_phase} -> {new_phase} (Stock Interno: {total_stock})")
        execute_commit_query("UPDATE projects SET phase = %s WHERE id = %s", (new_phase, project_id))

# --- IMPORTACIÓN MASIVA ---

def upsert_project_from_import(company_id, name, code, macro_project_id, address, status, phase, start_date, end_date, budget, department, province, district, cost_center=None):
    """
    Atomic UPSERT para importación de obras.
    La validación/normalización se hace en ProjectService.
    """
    conn = None

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            
            # --- UPSERT ATÓMICO (PostgreSQL) ---
            # Requiere que exista un índice UNIQUE en (macro_project_id, code)
            query = """
                INSERT INTO projects (
                    company_id, macro_project_id, name, code, address, 
                    status, phase, start_date, end_date, budget, 
                    department, province, district
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (macro_project_id, code) 
                DO UPDATE SET
                    name = EXCLUDED.name,
                    address = COALESCE(EXCLUDED.address, projects.address),
                    status = COALESCE(EXCLUDED.status, projects.status),
                    phase = COALESCE(EXCLUDED.phase, projects.phase),
                    budget = COALESCE(EXCLUDED.budget, projects.budget),
                    start_date = COALESCE(EXCLUDED.start_date, projects.start_date),
                    end_date = COALESCE(EXCLUDED.end_date, projects.end_date),
                    department = COALESCE(EXCLUDED.department, projects.department),
                    province = COALESCE(EXCLUDED.province, projects.province),
                    district = COALESCE(EXCLUDED.district, projects.district)
                RETURNING (xmax = 0) AS inserted
            """
            
            # Nota: Usamos COALESCE para no sobrescribir datos existentes con NULLs si el Excel viene vacío

            params = (
                company_id, macro_project_id, name, code, address,
                status or 'active', phase or 'Sin Iniciar',
                start_date, end_date, budget or 0,
                department, province, district
            )

            cursor.execute(query, params)
            result = cursor.fetchone()
            was_inserted = result[0] if result else False

            conn.commit()
            return "created" if was_inserted else "updated"

    except Exception as e:
        if conn: conn.rollback()
        if "unique constraint" in str(e):
            raise ValueError(f"Conflicto de integridad: El código '{code}' ya existe en otro contexto.")
        raise e
    finally:
        if conn: return_db_connection(conn)

def get_macro_project_id_by_name(company_id, name):
    """Helper para buscar ID de Macro Proyecto por nombre."""
    if not name: return None
    # [CORRECCIÓN] Limpieza preventiva
    clean_name = name.strip() 
    res = execute_query("SELECT id FROM macro_projects WHERE company_id = %s AND name ILIKE %s", (company_id, clean_name), fetchone=True)
    return res['id'] if res else None

# --- 5. REPORTES DE JERARQUÍA ---

def get_hierarchy_flat(company_id: int):
    """
    Obtiene la jerarquía completa aplanada: Dirección -> Gerencia -> Proyecto.
    Usa LEFT JOIN para traer direcciones incluso si no tienen gerencias, etc.
    """
    query = """
        SELECT 
            d.name as dir_name, d.code as dir_code,
            m.name as mgmt_name, m.code as mgmt_code,
            mp.name as macro_name, mp.code as macro_code, mp.cost_center
        FROM directions d
        LEFT JOIN managements m ON d.id = m.direction_id
        LEFT JOIN macro_projects mp ON m.id = mp.management_id
        WHERE d.company_id = %s
        ORDER BY d.name, m.name, mp.name
    """
    return execute_query(query, (company_id,), fetchall=True)

# --- 6. IMPORTACIÓN DE JERARQUÍA (CASCADA) ---

def import_hierarchy_batch(company_id: int, rows: list):
    """
    Importación masiva de jerarquía.
    La validación/normalización se hace en ProjectService.
    Los datos vienen pre-procesados con line_ref para mensajes de error.
    """
    conn = get_db_connection()
    stats = {"dirs_created": 0, "mgmts_created": 0, "macros_created": 0, "macros_updated": 0}

    try:
        with conn.cursor() as cursor:
            for row in rows:
                # Los datos vienen pre-procesados del servicio
                line_ref = row.get('line_ref', 'Fila desconocida')
                raw_dir = row.get('dir_name', '')
                dir_code = row.get('dir_code')
                raw_mgmt = row.get('mgmt_name', '')
                mgmt_code = row.get('mgmt_code')
                raw_macro = row.get('macro_name', '')
                macro_code = row.get('macro_code')
                cost_center = row.get('cost_center')

                # Si no hay dirección, saltar (ya validado en servicio)
                if not raw_dir:
                    continue

                # -----------------------------------------------

                # --- NIVEL 1: DIRECCIÓN ---
                # 1.1 Buscar coincidencia
                cursor.execute(
                    "SELECT id, name FROM directions WHERE company_id = %s AND name ILIKE %s", 
                    (company_id, raw_dir)
                )
                res_dir = cursor.fetchone()
                
                if res_dir:
                    db_id, db_name = res_dir
                    if raw_dir != db_name:
                        raise ValueError(f"{line_ref}: La Dirección '{raw_dir}' difiere de la existente '{db_name}'. Use mayúsculas exactas.")
                    dir_id = db_id
                else:
                    if raw_dir != raw_dir.upper():
                        raise ValueError(f"{line_ref}: La nueva Dirección '{raw_dir}' debe estar en MAYÚSCULAS.")
                    
                    try:
                        cursor.execute(
                            "INSERT INTO directions (company_id, name, code) VALUES (%s, %s, %s) RETURNING id",
                            (company_id, raw_dir, dir_code)
                        )
                        dir_id = cursor.fetchone()[0]
                        stats['dirs_created'] += 1
                    except psycopg2.errors.UniqueViolation:
                        raise ValueError(f"{line_ref}: El Código de Dirección '{dir_code}' ya existe.")

                # --- NIVEL 2: GERENCIA ---
                if not raw_mgmt: 
                    # Si no hay gerencia, terminamos esta fila aquí (ya validamos arriba que no haya macro)
                    continue 

                cursor.execute(
                    "SELECT id, name FROM managements WHERE company_id = %s AND direction_id = %s AND name ILIKE %s", 
                    (company_id, dir_id, raw_mgmt)
                )
                res_mgmt = cursor.fetchone()
                
                if res_mgmt:
                    db_id, db_name = res_mgmt
                    if raw_mgmt != db_name:
                        raise ValueError(f"{line_ref}: La Gerencia '{raw_mgmt}' difiere de la existente '{db_name}'.")
                    mgmt_id = db_id
                else:
                    if raw_mgmt != raw_mgmt.upper():
                        raise ValueError(f"{line_ref}: La nueva Gerencia '{raw_mgmt}' debe estar en MAYÚSCULAS.")

                    try:
                        cursor.execute(
                            "INSERT INTO managements (company_id, direction_id, name, code) VALUES (%s, %s, %s, %s) RETURNING id",
                            (company_id, dir_id, raw_mgmt, mgmt_code)
                        )
                        mgmt_id = cursor.fetchone()[0]
                        stats['mgmts_created'] += 1
                    except psycopg2.errors.UniqueViolation:
                        raise ValueError(f"{line_ref}: El Código de Gerencia '{mgmt_code}' ya existe.")

                # --- NIVEL 3: MACRO PROYECTO ---
                if not raw_macro: continue 

                cursor.execute(
                    "SELECT id, name FROM macro_projects WHERE company_id = %s AND management_id = %s AND name ILIKE %s", 
                    (company_id, mgmt_id, raw_macro)
                )
                res_macro = cursor.fetchone()
                
                if res_macro:
                    db_id, db_name = res_macro
                    if raw_macro != db_name:
                        raise ValueError(f"{line_ref}: El Proyecto '{raw_macro}' difiere del existente '{db_name}'.")
                    
                    try:
                        macro_id = db_id
                        cursor.execute(
                            """UPDATE macro_projects 
                               SET code = COALESCE(%s, code), 
                                   cost_center = COALESCE(%s, cost_center) 
                               WHERE id = %s""",
                            (macro_code, cost_center, macro_id)
                        )
                        stats['macros_updated'] += 1
                    except psycopg2.errors.UniqueViolation:
                        raise ValueError(f"{line_ref}: El Código de Proyecto '{macro_code}' ya está en uso.")
                else:
                    if raw_macro != raw_macro.upper():
                        raise ValueError(f"{line_ref}: El nuevo Proyecto '{raw_macro}' debe estar en MAYÚSCULAS.")

                    try:
                        cursor.execute(
                            """INSERT INTO macro_projects (company_id, management_id, name, code, cost_center) 
                               VALUES (%s, %s, %s, %s, %s)""",
                            (company_id, mgmt_id, raw_macro, macro_code, cost_center)
                        )
                        stats['macros_created'] += 1
                    except psycopg2.errors.UniqueViolation:
                        raise ValueError(f"{line_ref}: El Código de Proyecto '{macro_code}' ya está en uso.")

            conn.commit()
            return stats

    except ValueError:
        if conn: conn.rollback()
        raise 
    except Exception as e:
        if conn: conn.rollback()
        raise ValueError(f"Error de Base de Datos: {str(e)}")
    finally:
        if conn: return_db_connection(conn)