#app/database/schema.py

import psycopg2
import psycopg2.extras
import traceback
import hashlib
from datetime import datetime
from .core import execute_query, execute_commit_query
from .utils import create_warehouse_with_data, _create_warehouse_with_cursor

def create_schema(conn):
    cursor = conn.cursor()
    print("--- CREANDO ESQUEMA OPTIMIZADO PARA PRODUCCIÓN (V3) ---")
    
    try:
        # 1. EXTENSIÓN CRÍTICA PARA BÚSQUEDAS DE TEXTO (LIKE/ILIKE)
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    except Exception as e: 
        print(f"[WARN] No se pudo activar pg_trgm. Las búsquedas de texto serán lentas. Error: {e}")

    # --- 2. TABLAS MAESTRAS ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY, 
            name TEXT UNIQUE NOT NULL,
            country_code TEXT DEFAULT 'PE'
        );
    """)
    
    # --- 3. ESTRUCTURA JERÁRQUICA (Dirección -> Gerencia -> Macro -> Obra) ---
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS directions (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id),
            name TEXT NOT NULL,
            code TEXT,
            status TEXT DEFAULT 'active',
            UNIQUE(company_id, name)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS managements (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id),
            direction_id INTEGER REFERENCES directions(id),
            name TEXT NOT NULL,
            code TEXT, 
            analytic_account TEXT,
            description TEXT,
            status TEXT DEFAULT 'active',
            UNIQUE(company_id, name)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS macro_projects (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id),
            management_id INTEGER REFERENCES managements(id),
            name TEXT NOT NULL,
            code TEXT,
            description TEXT,
            client_name TEXT,
            status TEXT DEFAULT 'active',
            UNIQUE(company_id, name)
        );
    """)

    # Tabla PROJECTS con todos los campos necesarios
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id),
            macro_project_id INTEGER REFERENCES macro_projects(id),
            name TEXT NOT NULL, 
            code TEXT, 
            address TEXT, 
            
            -- Ubigeo
            department TEXT,
            province TEXT,
            district TEXT,
            
            -- Gestión
            start_date DATE,
            end_date DATE,
            status TEXT DEFAULT 'active',
            phase TEXT DEFAULT 'Sin Iniciar',
            budget REAL DEFAULT 0,
            
            UNIQUE(company_id, name)
        );
    """)

    # --- 4. TABLAS DE INVENTARIO ---

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_categories (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id),
            name TEXT NOT NULL, UNIQUE (company_id, name)
        );
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS uom (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_categories (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id),
            name TEXT NOT NULL, UNIQUE (company_id, name)
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS partner_categories (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id),
            name TEXT NOT NULL, UNIQUE (company_id, name)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS operation_types (
            id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, code TEXT NOT NULL, description TEXT,
            source_location_category TEXT NOT NULL, destination_location_category TEXT NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products ( 
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), 
            name TEXT NOT NULL, sku TEXT NOT NULL, 
            type TEXT NOT NULL DEFAULT 'storable', barcode TEXT, notes TEXT, 
            category_id INTEGER REFERENCES product_categories(id), 
            uom_id INTEGER REFERENCES uom(id),
            tracking TEXT NOT NULL DEFAULT 'none',
            ownership TEXT NOT NULL DEFAULT 'owned' CHECK(ownership IN ('owned', 'consigned')),
            standard_price REAL DEFAULT 0, -- Costo Promedio Ponderado
            UNIQUE (company_id, sku)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warehouses (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), 
            name TEXT NOT NULL, code TEXT NOT NULL, 
            social_reason TEXT, ruc TEXT, email TEXT, phone TEXT, address TEXT,
            category_id INTEGER REFERENCES warehouse_categories(id), 
            status TEXT NOT NULL DEFAULT 'activo' CHECK(status IN ('activo', 'inactivo')),
            UNIQUE (company_id, code)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), name TEXT NOT NULL,
            social_reason TEXT, ruc TEXT, email TEXT, phone TEXT, address TEXT,
            category_id INTEGER REFERENCES partner_categories(id), UNIQUE (company_id, name)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), 
            name TEXT NOT NULL, path TEXT NOT NULL, 
            type TEXT NOT NULL DEFAULT 'internal', 
            category TEXT, 
            warehouse_id INTEGER REFERENCES warehouses(id),
            UNIQUE (company_id, path)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS picking_types (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), 
            name TEXT NOT NULL, code TEXT NOT NULL, 
            warehouse_id INTEGER NOT NULL REFERENCES warehouses(id), 
            default_location_src_id INTEGER REFERENCES locations(id), 
            default_location_dest_id INTEGER REFERENCES locations(id),
            UNIQUE (company_id, name)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_lots (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, product_id INTEGER NOT NULL REFERENCES products(id), UNIQUE (product_id, name)
        );
    """)

    # --- 5. CORAZÓN DEL SISTEMA (QUANTS) ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_quants ( 
            id SERIAL PRIMARY KEY, 
            product_id INTEGER NOT NULL REFERENCES products(id), 
            location_id INTEGER NOT NULL REFERENCES locations(id), 
            lot_id INTEGER REFERENCES stock_lots(id), 
            project_id INTEGER REFERENCES projects(id), 
            quantity REAL NOT NULL,
            notes TEXT
        );
    """)
    
    # Índice Único Lógico para evitar duplicados de filas (NULL safe)
    # Esto asegura que (ProdA, Loc1, NULL, NULL) sea único
    try:
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_quants_unique 
            ON stock_quants (product_id, location_id, COALESCE(lot_id, -1), COALESCE(project_id, -1));
        """)
    except Exception: pass

    # --- 6. OPERACIONES Y LIQUIDACIONES ---

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS work_orders (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), 
            ot_number TEXT UNIQUE NOT NULL,
            customer_name TEXT NOT NULL, address TEXT, service_type TEXT, job_type TEXT,
            phase TEXT NOT NULL DEFAULT 'Sin Liquidar',
            date_registered TIMESTAMPTZ DEFAULT NOW(),
            project_id INTEGER REFERENCES projects(id) 
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pickings (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id), responsible_user TEXT,
            name TEXT UNIQUE NOT NULL, remission_number TEXT UNIQUE,
            picking_type_id INTEGER NOT NULL REFERENCES picking_types(id), warehouse_id INTEGER REFERENCES warehouses(id),
            location_src_id INTEGER REFERENCES locations(id), location_dest_id INTEGER REFERENCES locations(id),
            scheduled_date TIMESTAMPTZ, state TEXT NOT NULL DEFAULT 'draft',
            notes TEXT, partner_ref TEXT, work_order_id INTEGER REFERENCES work_orders(id),
            custom_operation_type TEXT, partner_id INTEGER REFERENCES partners(id),
            date_done TIMESTAMPTZ, date_transfer DATE, attention_date DATE,
            purchase_order TEXT, service_act_number TEXT,
            adjustment_reason TEXT, loss_confirmation TEXT,
            project_id INTEGER REFERENCES projects(id) 
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_moves (
            id SERIAL PRIMARY KEY, picking_id INTEGER REFERENCES pickings(id), 
            product_id INTEGER NOT NULL REFERENCES products(id), 
            product_uom_qty REAL NOT NULL, quantity_done REAL DEFAULT 0, 
            location_src_id INTEGER REFERENCES locations(id), location_dest_id INTEGER REFERENCES locations(id), 
            partner_id INTEGER REFERENCES partners(id), 
            state TEXT NOT NULL DEFAULT 'draft',
            price_unit REAL DEFAULT 0, cost_at_adjustment REAL DEFAULT 0,
            project_id INTEGER REFERENCES projects(id)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_move_lines (
            id SERIAL PRIMARY KEY, move_id INTEGER NOT NULL REFERENCES stock_moves(id),
            lot_id INTEGER NOT NULL REFERENCES stock_lots(id), qty_done REAL NOT NULL
        );
    """)
    
    # --- 7. USUARIOS Y PERMISOS ---
    cursor.execute("CREATE TABLE IF NOT EXISTS roles (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, description TEXT);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, hashed_password TEXT NOT NULL,
            full_name TEXT, role_id INTEGER NOT NULL REFERENCES roles(id), is_active INTEGER DEFAULT 1
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_companies (
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            PRIMARY KEY (user_id, company_id)
        );
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS permissions (id SERIAL PRIMARY KEY, key TEXT UNIQUE NOT NULL, description TEXT);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS role_permissions (
            role_id INTEGER NOT NULL REFERENCES roles(id), permission_id INTEGER NOT NULL REFERENCES permissions(id), PRIMARY KEY (role_id, permission_id)
        );
    """)

    # =========================================================================
    # --- 8. ÍNDICES DE RENDIMIENTO (HIGH PERFORMANCE PACK) ---
    # =========================================================================
    print(" -> Aplicando índices de alto rendimiento...")
    
    indices = [
        # A. BÚSQUEDAS DE TEXTO (GIN Trigram) - Para buscadores rápidos "LIKE %txt%"
        "CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin (name gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS idx_products_sku_trgm ON products USING gin (sku gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS idx_projects_name_trgm ON projects USING gin (name gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS idx_partners_name_trgm ON partners USING gin (name gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS idx_wo_ot_number_trgm ON work_orders USING gin (ot_number gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS idx_pickings_name_trgm ON pickings USING gin (name gin_trgm_ops);",

        # B. CONSULTAS DE STOCK (Compuestos) - Para velocidad extrema en cálculos
        # Este índice acelera get_project_stock_in_location y get_real_available_stock
        "CREATE INDEX IF NOT EXISTS idx_sq_composite ON stock_quants (product_id, location_id, project_id);",
        
        # C. CLAVES FORÁNEAS Y ESTADOS - Para Joins y Filtros
        "CREATE INDEX IF NOT EXISTS idx_moves_picking_id ON stock_moves (picking_id);",
        "CREATE INDEX IF NOT EXISTS idx_moves_product_id ON stock_moves (product_id);",
        "CREATE INDEX IF NOT EXISTS idx_pickings_state_date ON pickings (state, date_done);", # Para reportes por fecha
        "CREATE INDEX IF NOT EXISTS idx_pickings_project ON pickings (project_id);",
        "CREATE INDEX IF NOT EXISTS idx_wo_project ON work_orders (project_id);",
        
        # D. SERIES Y TRAZABILIDAD
        "CREATE INDEX IF NOT EXISTS idx_sml_move_id ON stock_move_lines (move_id);",
        "CREATE INDEX IF NOT EXISTS idx_lots_product ON stock_lots (product_id);"
    ]

    for idx_sql in indices:
        try:
            cursor.execute(idx_sql)
        except Exception as e:
            print(f"[WARN] Falló índice: {e}")

    conn.commit()
    print("Esquema V3 (Optimizado) verificado exitosamente.")


# <--- 2. AGREGA ESTA FUNCIÓN HELPER ANTES DE create_initial_data --->
def hash_password(password):
    """Genera un hash SHA-256 para la contraseña (Helper local para seed data)."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def create_initial_data(conn):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) # Usar DictCursor
    print(" -> Creando datos iniciales (versión PostgreSQL)...")

    # --- 1. Empresa ---
    cursor.execute("""
        INSERT INTO companies (name, country_code) 
        VALUES (%s, %s) 
        ON CONFLICT (name) DO UPDATE SET country_code = EXCLUDED.country_code 
        RETURNING id
    """, ("Mi Empresa Principal", "PE"))

    default_company_id_row = cursor.fetchone()
    if default_company_id_row:
        default_company_id = default_company_id_row['id']
    else:
        cursor.execute("SELECT id FROM companies WHERE name = %s", ("Mi Empresa Principal",))
        default_company_id_row = cursor.fetchone()
        if not default_company_id_row: raise Exception("No se pudo crear o encontrar la compañía principal.")
        default_company_id = default_company_id_row['id']

    # --- 1. JERARQUÍA ORGANIZATIVA INICIAL ---
    print(" -> Creando jerarquía organizativa por defecto...")
    
    # A. Dirección General
    cursor.execute(
        "INSERT INTO directions (company_id, name, code) VALUES (%s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING RETURNING id",
        (default_company_id, "Dirección General", "DIR-GEN")
    )
    row = cursor.fetchone()
    # Si ya existe, lo buscamos
    if not row: 
        cursor.execute("SELECT id FROM directions WHERE company_id=%s AND name=%s", (default_company_id, "Dirección General"))
        row = cursor.fetchone()
    dir_id = row['id']

    # B. Gerencia de Operaciones
    cursor.execute(
        "INSERT INTO managements (company_id, direction_id, name, code) VALUES (%s, %s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING RETURNING id",
        (default_company_id, dir_id, "Gerencia de Operaciones", "G-OPS")
    )
    row = cursor.fetchone()
    if not row: 
        cursor.execute("SELECT id FROM managements WHERE company_id=%s AND name=%s", (default_company_id, "Gerencia de Operaciones"))
        row = cursor.fetchone()
    mgmt_id = row['id']

    # C. Macro Proyecto Base (Contrato Marco)
    cursor.execute(
        "INSERT INTO macro_projects (company_id, management_id, name, code) VALUES (%s, %s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING RETURNING id",
        (default_company_id, mgmt_id, "Contrato Marco 2025", "M-2025")
    )
    row = cursor.fetchone()
    if not row: 
        cursor.execute("SELECT id FROM macro_projects WHERE company_id=%s AND name=%s", (default_company_id, "Contrato Marco 2025"))
        row = cursor.fetchone()
    macro_id = row['id']

    # D. Obras de Ejemplo (Hijas del Macro)
    projects_data = [
        ("Nodo Norte - Fibra", "OBRA-001", "Sin Iniciar"),
        ("Instalación Residencial A", "OBRA-002", "En Instalación"),
        ("Mantenimiento Torre B", "OBRA-003", "Liquidado")
    ]
    
    for p_name, p_code, p_phase in projects_data:
        cursor.execute("""
            INSERT INTO projects (company_id, macro_project_id, name, code, phase, status) 
            VALUES (%s, %s, %s, %s, %s, 'active')
            ON CONFLICT (company_id, name) DO UPDATE SET phase = EXCLUDED.phase
        """, (default_company_id, macro_id, p_name, p_code, p_phase))


    # --- 2. Ubicaciones Virtuales ---
    locations_data = [
        (default_company_id, "Proveedores", "PA/Vendors", "vendor", "PROVEEDOR"),
        (default_company_id, "Clientes", "PA/Customers", "customer", "CLIENTE"),
        (default_company_id, "Pérdida de Inventario", "Virtual/Scrap", "inventory", "AJUSTE"),
        (default_company_id, "Contrata Cliente", "PA/ContractorCustomer", "customer", "CONTRATA CLIENTE")
    ]
    cursor.executemany("""
        INSERT INTO locations (company_id, name, path, type, category) 
        VALUES (%s, %s, %s, %s, %s) 
        ON CONFLICT (company_id, path) DO NOTHING
    """, locations_data)

    # --- 3. Tipos de Operación (Estos son globales, no llevan company_id) ---
    op_types = [
        ("Compra Nacional", "IN", "Entrada de mercancía de proveedor.", "vendor", "internal"),
        ("Consignación Recibida", "IN", "Entrada de mercancía propiedad de cliente.", "customer", "internal"),
        ("Transferencia entre Almacenes", "INT", "Mueve stock entre tus almacenes internos.", "internal", "internal"),
        ("Consignación Entregada", "INT", "Envía tu stock a un contratista.", "internal", "internal"),
        ("Devolución de Contrata", "INT", "El contratista te devuelve stock.", "internal", "internal"),
        ("Devolución a Proveedor", "OUT", "Devuelves mercancía a proveedor.", "internal", "vendor"),
        ("Devolución a Cliente", "OUT", "Devuelves mercancía (consignada) a cliente.", "internal", "customer"),
        ("Traspaso Contrata Cliente", "OUT", "Stock de contratista se entrega a cliente final.", "internal", "customer"),
        ("Liquidación por OT", "OUT", "Salida material consumido en OT.", "internal", "customer")
    ]
    cursor.executemany("INSERT INTO operation_types (name, code, description, source_location_category, destination_location_category) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (name) DO NOTHING", op_types)

    # --- ¡INICIO DE LA CORRECCIÓN! ---
    
    # --- 4. Categorías de Almacén (Ahora con company_id) ---
    wh_categories = [
        (default_company_id, "ALMACEN PRINCIPAL"),
        (default_company_id, "CONTRATISTA")
    ]
    cursor.executemany(
        "INSERT INTO warehouse_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", 
        wh_categories
    )

    # --- 5. Categorías de Partner (Ahora con company_id) ---
    partner_categories = [
        (default_company_id, "Proveedor Externo"),
        (default_company_id, "Proveedor Cliente")
    ]
    cursor.executemany(
        "INSERT INTO partner_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", 
        partner_categories
    )
    conn.commit() # Hacemos commit aquí para que las siguientes SELECT funcionen

    # --- 6. Proveedores por defecto (con verificación) ---
    cursor.execute("SELECT id FROM partner_categories WHERE name = %s AND company_id = %s", ("Proveedor Cliente", default_company_id))
    cat_cliente_row = cursor.fetchone()
    if not cat_cliente_row: raise Exception("No se encontró la categoría 'Proveedor Cliente'")
    cat_cliente_id = cat_cliente_row['id']

    cursor.execute("SELECT id FROM partner_categories WHERE name = %s AND company_id = %s", ("Proveedor Externo", default_company_id))
    cat_externo_row = cursor.fetchone()
    if not cat_externo_row: raise Exception("No se encontró la categoría 'Proveedor Externo'")
    cat_externo_id = cat_externo_row['id']
    
    cursor.execute("INSERT INTO partners (company_id, name, category_id) VALUES (%s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING", (default_company_id, "Cliente Varios", cat_cliente_id))
    cursor.execute("INSERT INTO partners (company_id, name, category_id) VALUES (%s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING", (default_company_id, "Proveedor Varios", cat_externo_id))

    # --- 7. Datos Maestros (Productos, etc.) ---
    
    # --- Categoría de Producto (Ahora con company_id) ---
    cursor.execute(
        "INSERT INTO product_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING RETURNING id", 
        (default_company_id, 'General')
    )
    general_cat_id_row = cursor.fetchone()
    if general_cat_id_row:
        general_cat_id = general_cat_id_row['id']
    else:
        cursor.execute("SELECT id FROM product_categories WHERE name = %s AND company_id = %s", ('General', default_company_id))
        general_cat_id_row = cursor.fetchone()
        if not general_cat_id_row: raise Exception("No se pudo crear o encontrar la categoría 'General'")
        general_cat_id = general_cat_id_row['id']

    # --- FIN DE LA CORRECCIÓN ---

    cursor.execute("INSERT INTO uom (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", ('Unidades',))
    uom_unidades_id_row = cursor.fetchone()
    if uom_unidades_id_row:
        uom_unidades_id = uom_unidades_id_row['id']
    else:
        cursor.execute("SELECT id FROM uom WHERE name = %s", ('Unidades',))
        uom_unidades_id_row = cursor.fetchone()
        if not uom_unidades_id_row: raise Exception("No se pudo crear o encontrar la UdM 'Unidades'")
        uom_unidades_id = uom_unidades_id_row['id']

    products_to_create = [
        (default_company_id, "Producto de Prueba", "PRUEBA001", general_cat_id, "none", uom_unidades_id, 'owned', 0),
        (default_company_id, "Cable UTP Cat 6", "OWN-CABLE-001", general_cat_id, "none", uom_unidades_id, 'owned', 0),
        (default_company_id, "Conector RJ45", "OWN-CONN-001", general_cat_id, "none", uom_unidades_id, 'owned', 0),
        (default_company_id, "Router Cliente Avanzado", "CON-ROUTER-SERIAL", general_cat_id, "serial", uom_unidades_id, 'consigned', 150.75),
        (default_company_id, "Antena WiFi Básica", "CON-ANTENNA-NOSERIAL", general_cat_id, "none", uom_unidades_id, 'consigned', 25.50)
    ]
    cursor.executemany("""
        INSERT INTO products (company_id, name, sku, category_id, tracking, uom_id, ownership, standard_price) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
        ON CONFLICT (company_id, sku) DO NOTHING
    """, products_to_create)
    # --- 8. Creamos los ALMACENES por defecto ---
    cursor.execute("SELECT id, name FROM warehouse_categories WHERE company_id = %s", (default_company_id,))
    all_wh_categories_actual = cursor.fetchall()
    used_codes = set() 

    principal_cat = next((cat for cat in all_wh_categories_actual if cat['name'] == "ALMACEN PRINCIPAL"), None)
    if principal_cat:
        create_warehouse_with_data(cursor, "Almacén Lima", "LIMA", default_company_id, principal_cat['id'], for_existing=False) 
        used_codes.add("LIMA") 

    for category in all_wh_categories_actual:
        if category['name'] == "ALMACEN PRINCIPAL": continue 

        cat_id = category['id']
        cat_name = category['name']
        warehouse_name = f"Almacén {cat_name.title()}"
        base_code = cat_name[:3].upper()
        warehouse_code = base_code
        counter = 2
        while warehouse_code in used_codes: 
            warehouse_code = f"{base_code[:2]}{counter}"
            counter += 1
        used_codes.add(warehouse_code)
        _create_warehouse_with_cursor(cursor, warehouse_name, warehouse_code, cat_id, default_company_id, "", "", "", "", "", "activo")

    # --- 9. Creamos el TIPO DE OPERACIÓN de Ajuste ---
    cursor.execute("SELECT id FROM locations WHERE category='AJUSTE' AND company_id = %s LIMIT 1", (default_company_id,))
    adj_loc = cursor.fetchone()
    if adj_loc:
        adj_loc_id = adj_loc['id']
        
        cursor.execute("SELECT id FROM warehouse_categories WHERE name='ALMACEN PRINCIPAL' AND company_id = %s LIMIT 1", (default_company_id,))
        wh_cat_id_row = cursor.fetchone()
        
        if wh_cat_id_row:
            cursor.execute("SELECT id FROM warehouses WHERE category_id = %s AND company_id = %s LIMIT 1", (wh_cat_id_row['id'], default_company_id))
            default_wh_id_row = cursor.fetchone()
            default_wh_id = default_wh_id_row['id'] if default_wh_id_row else 1
            
            cursor.execute("""
                INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
                VALUES (%s, %s, 'ADJ', %s, %s, %s) 
                ON CONFLICT (company_id, name) DO NOTHING
            """, (default_company_id, "Ajustes de Inventario", default_wh_id, adj_loc_id, adj_loc_id))

        else:
            print("[WARN] No se encontró categoría 'ALMACEN PRINCIPAL'. No se creó el tipo de operación ADJ.")
    else:
        print("[WARN] No se encontró ubicación de ajuste (category='AJUSTE'). No se creó el tipo de operación ADJ.")

    # --- 9. Crear datos de RBAC (sin cambios) ---
    print(" -> Creando datos iniciales de RBAC (Usuarios, Roles, Permisos)...")
    try:
        cursor.execute("INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING RETURNING id", ("Administrador", "Acceso total al sistema"))
        admin_role_id_row = cursor.fetchone()
        if admin_role_id_row:
            admin_role_id = admin_role_id_row['id']
        else:
            cursor.execute("SELECT id FROM roles WHERE name = %s", ("Administrador",))
            admin_role_id_row = cursor.fetchone()
            if not admin_role_id_row: raise Exception("No se pudo crear o encontrar el rol 'Administrador'")
            admin_role_id = admin_role_id_row['id']
        
        # ... (resto de tu código RBAC sin cambios) ...
        cursor.execute("INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", ("Liquidador", "Puede gestionar liquidaciones"))
        cursor.execute("INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", ("Asistente de Almacén", "Puede gestionar operaciones de almacén"))

        admin_pass_hashed = hash_password("admin")
        cursor.execute("INSERT INTO users (username, hashed_password, full_name, role_id) VALUES (%s, %s, %s, %s) ON CONFLICT (username) DO NOTHING", ("admin", admin_pass_hashed, "Administrador", admin_role_id))

        # Primero obtenemos el ID real del admin (por si ya existía y el INSERT ignoró)
        cursor.execute("SELECT id FROM users WHERE username = %s", ("admin",))
        admin_user_row = cursor.fetchone()
        
        if admin_user_row:
            admin_id = admin_user_row['id']
            print(f" -> Asignando compañía '{default_company_id}' al usuario Admin ({admin_id})...")
            cursor.execute("""
                INSERT INTO user_companies (user_id, company_id) 
                VALUES (%s, %s) 
                ON CONFLICT (user_id, company_id) DO NOTHING
            """, (admin_id, default_company_id))

        all_permissions = {
            "nav.dashboard.view": "Ver Dashboard", "nav.operations.view": "Ver menú Operaciones",
            "nav.masters.view": "Ver menú Maestros", "nav.reports.view": "Ver menú Reportes",
            "nav.config.view": "Ver menú Configuración", "nav.admin.view": "Ver menú Administración (RBAC)",
            "operations.can_view": "Ver lista de Operaciones (IN/OUT/INT)",
            "operations.tab.in.view": "Ver pestaña Recepciones",
            "operations.tab.out.view": "Ver pestaña Salidas",
            "operations.tab.int.view": "Ver pestaña Transferencias",
            "operations.tab.ret.view": "Ver pestaña Retiros",
            "operations.can_create": "Crear nuevas Operaciones",
            "operations.can_edit": "Editar Operaciones (Borrador)", "operations.can_validate": "Validar Operaciones (Pasar a 'Hecho')",
            "operations.can_import_export": "Importar/Exportar Operaciones",
            "liquidaciones.can_view": "Ver lista de Liquidaciones", "liquidaciones.can_create": "Registrar nuevas OTs",
            "liquidaciones.can_edit": "Guardar cambios en Liquidaciones", "liquidaciones.can_liquidate": "Validar y Liquidar OTs",
            "liquidaciones.can_import_export": "Importar/Exportar OTs",
            "adjustments.can_view": "Ver Ajustes de Inventario", "adjustments.can_create": "Crear nuevos Ajustes",
            "adjustments.can_edit": "Editar Ajustes (Borrador)", "adjustments.can_validate": "Validar Ajustes",
            "products.can_crud": "CRUD Productos", "warehouses.can_crud": "CRUD Almacenes",
            "partners.can_crud": "CRUD Proveedores", "locations.can_crud": "CRUD Ubicaciones",
            "reports.stock.view": "Ver Reporte de Stock", "reports.kardex.view": "Ver Reporte Kardex",
            "reports.aging.view": "Ver Reporte Antigüedad", "reports.coverage.view": "Ver Reporte Cobertura",
            "admin.can_manage_users": "Gestionar Usuarios (Crear/Editar)", "admin.can_manage_roles": "Gestionar Roles y Permisos"
        }

        permission_ids = {}
        for key, desc in all_permissions.items():
            cursor.execute("INSERT INTO permissions (key, description) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING RETURNING id", (key, desc))
            perm_id_row = cursor.fetchone()
            if perm_id_row:
                perm_id = perm_id_row['id']
            else:
                cursor.execute("SELECT id FROM permissions WHERE key = %s", (key,))
                perm_id_row = cursor.fetchone()
                if not perm_id_row: raise Exception(f"No se pudo crear o encontrar el permiso '{key}'")
                perm_id = perm_id_row['id']
            permission_ids[key] = perm_id

        admin_permissions_to_insert = [(admin_role_id, perm_id) for perm_id in permission_ids.values()]
        if admin_permissions_to_insert:
            cursor.executemany("INSERT INTO role_permissions (role_id, permission_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", admin_permissions_to_insert)
            print(f" -> {len(admin_permissions_to_insert)} permisos asignados al rol 'Administrador'.")
        
    except Exception as e:
        print(f"[ERROR] Falló la creación de datos iniciales de RBAC: {e}")
        traceback.print_exc()

    conn.commit()
    print("Datos iniciales de PostgreSQL creados/verificados.")


