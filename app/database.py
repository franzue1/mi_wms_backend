# app/database.py
import psycopg2
import psycopg2.extras # Para acceder a los datos como diccionario
import psycopg2.extensions
import psycopg2.pool # <-- 1. IMPORTAR EL POOL
import os
from datetime import datetime, date, timedelta
import traceback
import functools
from collections import defaultdict
import hashlib
from dotenv import load_dotenv

# --- 2. CONFIGURACIÓN DEL POOL GLOBAL ---
# Este pool se creará UNA VEZ al iniciar la app.
db_pool = None
DATABASE_URL = None
#DATABASE_URL = os.environ.get("DATABASE_URL")

def init_db_pool():
    """
    Inicializa el pool de conexiones de la base de datos.
    Esta función DEBE llamarse UNA SOLA VEZ al iniciar el servidor FastAPI.
    """
    global db_pool, DATABASE_URL
    if db_pool:
        return # El pool ya está inicializado

    load_dotenv()
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if DATABASE_URL is None:
        raise ValueError("No se pudo conectar: DATABASE_URL no está configurada.")

    try:
        # --- 3. CREAR EL POOL ---
        # minconn=1, maxconn=10 (Permitirá hasta 10 conexiones simultáneas)
        # Ideal para tus llamadas en paralelo de 'asyncio.gather'
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, dsn=DATABASE_URL
        )
        
        # Probar la conexión
        conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor
        if "localhost" in DATABASE_URL:
            print(" -> Pool de BD (Local) Creado (1-10 conexiones).")
        else:
            print(" -> Pool de BD (Producción) Creado (1-10 conexiones).")
        db_pool.putconn(conn) # Devolver la conexión de prueba

    except psycopg2.OperationalError as e:
        print(f"!!! ERROR CRÍTICO AL CREAR EL POOL DE BD !!!\n{e}")
        traceback.print_exc()
        raise

def execute_query(query, params=(), fetchone=False, fetchall=False):
    """
    Función centralizada para ejecutar consultas de LECTURA (SELECT).
    ¡AHORA USA EL POOL DE CONEXIONES!
    """
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
             raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    conn = None
    try:
        # --- 5. OBTENER CONEXIÓN DEL POOL ---
        conn = db_pool.getconn() 
        conn.cursor_factory = psycopg2.extras.DictCursor # Asegurar DictCursor
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)

            if fetchone:
                return cursor.fetchone()
            if fetchall:
                return cursor.fetchall()
            # (Si no es fetchone/fetchall, no devuelve nada, como en un UPDATE)

    except Exception as e:
        print(f"Error inesperado en consulta de lectura (PostgreSQL): {e}")
        traceback.print_exc()
        raise e # Re-lanzar la excepción para que la API la capture
    finally:
        if conn:
            # --- 6. DEVOLVER LA CONEXIÓN AL POOL ---
            db_pool.putconn(conn) 

# En app/database.py (añadir esto después de la función execute_query)

def execute_commit_query(query, params=(), fetchone=False):
    """
    Función centralizada para ejecutar consultas de ESCRITURA (INSERT, UPDATE, DELETE).
    ¡Usa el pool de conexiones y HACE COMMIT!
    """
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
             raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    conn = None
    try:
        # 1. Obtener conexión del pool
        conn = db_pool.getconn() 
        conn.cursor_factory = psycopg2.extras.DictCursor
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            
            result = None
            if fetchone:
                # Capturar el resultado de 'RETURNING'
                result = cursor.fetchone() 
                
            # 2. ¡Hacer commit de la transacción!
            conn.commit() 
            
            if fetchone:
                return result # Devolver el resultado (ej. el ID)
            return True # Indicar éxito para UPsDATE/DELETE
            
    except Exception as e:
        print(f"Error en consulta de escritura (PostgreSQL): {e}")
        traceback.print_exc()
        if conn:
            conn.rollback() # Revertir si algo falló
        raise e # Re-lanzar la excepción para que la API la capture
    finally:
        if conn:
            # 3. Devolver la conexión al pool
            db_pool.putconn(conn)

def create_schema(conn):
    cursor = conn.cursor()
    print("Verificando/Creando esquema de tablas en PostgreSQL...")
    
    # --- Tablas Principales (sin FKs) ---
    cursor.execute("CREATE TABLE IF NOT EXISTS companies (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS product_categories (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS uom (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS warehouse_categories (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS partner_categories (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS operation_types (
            id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, code TEXT NOT NULL, description TEXT,
            source_location_category TEXT NOT NULL, destination_location_category TEXT NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products ( 
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, name TEXT NOT NULL, sku TEXT UNIQUE NOT NULL, 
            type TEXT NOT NULL DEFAULT 'storable', barcode TEXT, notes TEXT, 
            category_id INTEGER, uom_id INTEGER,
            tracking TEXT NOT NULL DEFAULT 'none',
            ownership TEXT NOT NULL DEFAULT 'owned' CHECK(ownership IN ('owned', 'consigned')),
            standard_price REAL DEFAULT 0
        );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warehouses (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, name TEXT NOT NULL, code TEXT NOT NULL UNIQUE,
            social_reason TEXT, ruc TEXT, email TEXT, phone TEXT, address TEXT,
            category_id INTEGER, status TEXT NOT NULL DEFAULT 'activo' CHECK(status IN ('activo', 'inactivo'))
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, name TEXT NOT NULL,
            social_reason TEXT, ruc TEXT, email TEXT, phone TEXT, address TEXT,
            category_id INTEGER, UNIQUE (company_id, name)
        );
    """)
    cursor.execute("""CREATE TABLE IF NOT EXISTS locations (id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, name TEXT NOT NULL, path TEXT UNIQUE NOT NULL, type TEXT NOT NULL DEFAULT 'internal', category TEXT, warehouse_id INTEGER);""")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS picking_types (
            id SERIAL PRIMARY KEY, 
            company_id INTEGER NOT NULL, 
            name TEXT NOT NULL UNIQUE,  -- <-- ¡AÑADIMOS UNIQUE AQUÍ!
            code TEXT NOT NULL, 
            warehouse_id INTEGER NOT NULL, 
            default_location_src_id INTEGER, 
            default_location_dest_id INTEGER
        );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_lots (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, product_id INTEGER NOT NULL, UNIQUE (product_id, name)
        );
    """)
    cursor.execute("""CREATE TABLE IF NOT EXISTS stock_quants ( id SERIAL PRIMARY KEY, product_id INTEGER NOT NULL, location_id INTEGER NOT NULL, lot_id INTEGER, quantity REAL NOT NULL, UNIQUE (product_id, location_id, lot_id));""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS work_orders (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, ot_number TEXT UNIQUE NOT NULL,
            customer_name TEXT NOT NULL, address TEXT, service_type TEXT, job_type TEXT,
            phase TEXT NOT NULL DEFAULT 'Sin Liquidar',
            date_registered TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pickings (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, responsible_user TEXT,
            name TEXT UNIQUE NOT NULL, remission_number TEXT UNIQUE,
            picking_type_id INTEGER NOT NULL, warehouse_id INTEGER,
            location_src_id INTEGER, location_dest_id INTEGER,
            scheduled_date TIMESTAMPTZ, state TEXT NOT NULL DEFAULT 'draft',
            notes TEXT, partner_ref TEXT, work_order_id INTEGER,
            custom_operation_type TEXT, partner_id INTEGER,
            date_done TIMESTAMPTZ, date_transfer DATE, attention_date DATE,
            purchase_order TEXT, service_act_number TEXT,
            adjustment_reason TEXT, loss_confirmation TEXT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_moves (
            id SERIAL PRIMARY KEY, picking_id INTEGER, product_id INTEGER NOT NULL, 
            product_uom_qty REAL NOT NULL, quantity_done REAL DEFAULT 0, 
            location_src_id INTEGER, location_dest_id INTEGER, partner_id INTEGER, 
            state TEXT NOT NULL DEFAULT 'draft',
            price_unit REAL DEFAULT 0, cost_at_adjustment REAL DEFAULT 0
        );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_move_lines (
            id SERIAL PRIMARY KEY, move_id INTEGER NOT NULL,
            lot_id INTEGER NOT NULL, qty_done REAL NOT NULL
        );
    """)
    
    # --- Tablas RBAC (Usuarios, Roles, Permisos) ---
    print(" -> Creando tablas de Usuarios y RBAC...")
    cursor.execute("CREATE TABLE IF NOT EXISTS roles (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, description TEXT);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, hashed_password TEXT NOT NULL,
            full_name TEXT, role_id INTEGER NOT NULL, is_active INTEGER DEFAULT 1
        );
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS permissions (id SERIAL PRIMARY KEY, key TEXT UNIQUE NOT NULL, description TEXT);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS role_permissions (
            role_id INTEGER NOT NULL, permission_id INTEGER NOT NULL, PRIMARY KEY (role_id, permission_id)
        );
    """)
    print(" -> Tablas de Usuarios y RBAC creadas/verificadas.")

    # --- ¡NUEVO! Añadir Claves Foráneas (FOREIGN KEY) ---
    # (Esto se hace al final y es seguro ejecutarlo múltiples veces)
    print(" -> Verificando/Añadiendo Claves Foráneas (FKs)...")
    try:
        cursor.execute("ALTER TABLE products ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES product_categories(id);")
    except psycopg2.Error: pass # Ignorar si ya existe
    try:
        cursor.execute("ALTER TABLE products ADD CONSTRAINT fk_uom FOREIGN KEY (uom_id) REFERENCES uom(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE warehouses ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES warehouse_categories(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE partners ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES partner_categories(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE locations ADD CONSTRAINT fk_warehouse FOREIGN KEY (warehouse_id) REFERENCES warehouses(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE picking_types ADD CONSTRAINT fk_warehouse FOREIGN KEY (warehouse_id) REFERENCES warehouses(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE picking_types ADD CONSTRAINT fk_loc_src FOREIGN KEY (default_location_src_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE picking_types ADD CONSTRAINT fk_loc_dest FOREIGN KEY (default_location_dest_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_lots ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_quants ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_quants ADD CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_quants ADD CONSTRAINT fk_lot FOREIGN KEY (lot_id) REFERENCES stock_lots(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE pickings ADD CONSTRAINT fk_picking_type FOREIGN KEY (picking_type_id) REFERENCES picking_types(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE pickings ADD CONSTRAINT fk_warehouse FOREIGN KEY (warehouse_id) REFERENCES warehouses(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE pickings ADD CONSTRAINT fk_loc_src FOREIGN KEY (location_src_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE pickings ADD CONSTRAINT fk_loc_dest FOREIGN KEY (location_dest_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE pickings ADD CONSTRAINT fk_work_order FOREIGN KEY (work_order_id) REFERENCES work_orders(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE pickings ADD CONSTRAINT fk_partner FOREIGN KEY (partner_id) REFERENCES partners(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_moves ADD CONSTRAINT fk_picking FOREIGN KEY (picking_id) REFERENCES pickings(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_moves ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_moves ADD CONSTRAINT fk_loc_src FOREIGN KEY (location_src_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_moves ADD CONSTRAINT fk_loc_dest FOREIGN KEY (location_dest_id) REFERENCES locations(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_move_lines ADD CONSTRAINT fk_move FOREIGN KEY (move_id) REFERENCES stock_moves(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE stock_move_lines ADD CONSTRAINT fk_lot FOREIGN KEY (lot_id) REFERENCES stock_lots(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE users ADD CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE role_permissions ADD CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE role_permissions ADD CONSTRAINT fk_permission FOREIGN KEY (permission_id) REFERENCES permissions(id);")
    except psycopg2.Error: pass
    
    print(" -> Verificación de FKs completada.")
    print(" -> Creando índices de base de datos para optimización...")
    try:
        # Índices para 'pickings' (albaranes) - ¡Muy importante!
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pickings_state ON pickings (state);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pickings_work_order_id ON pickings (work_order_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pickings_picking_type_id ON pickings (picking_type_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pickings_date_done ON pickings (date_done);")

        # Índices para 'stock_moves' (movimientos) - ¡El más importante!
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_moves_picking_id ON stock_moves (picking_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_moves_product_id ON stock_moves (product_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_moves_location_src_id ON stock_moves (location_src_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_moves_location_dest_id ON stock_moves (location_dest_id);")
        
        # Índices para 'stock_quants' (el stock)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_quants_product_id ON stock_quants (product_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_quants_location_id ON stock_quants (location_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_quants_lot_id ON stock_quants (lot_id);")
        
        # Índices para 'locations'
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_locations_warehouse_id ON locations (warehouse_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_locations_type ON locations (type);")
        
        # Índices para 'stock_move_lines'
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_move_lines_move_id ON stock_move_lines (move_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_move_lines_lot_id ON stock_move_lines (lot_id);")
        
    except Exception as e:
        print(f"Error al crear índices: {e}")
        # (No detenemos la ejecución, solo lo reportamos)
        pass
    print(" -> Índices creados/verificados.")

    conn.commit()

def create_initial_data(conn):
    cursor = conn.cursor()
    print(" -> Creando datos iniciales (versión PostgreSQL)...")

    # --- 1. Empresa ---
    cursor.execute("INSERT INTO companies (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", ("Mi Empresa Principal",))
    default_company_id_row = cursor.fetchone()
    if default_company_id_row:
        default_company_id = default_company_id_row[0]
    else:
        cursor.execute("SELECT id FROM companies WHERE name = %s", ("Mi Empresa Principal",))
        default_company_id_row = cursor.fetchone()
        if not default_company_id_row: raise Exception("No se pudo crear o encontrar la compañía principal.")
        default_company_id = default_company_id_row[0]

    # --- 2. Ubicaciones Virtuales ---
    locations_data = [
        (default_company_id, "Proveedores", "PA/Vendors", "vendor", "PROVEEDOR"),
        (default_company_id, "Clientes", "PA/Customers", "customer", "CLIENTE"),
        (default_company_id, "Pérdida de Inventario", "Virtual/Scrap", "inventory", "AJUSTE"),
        (default_company_id, "Contrata Cliente", "PA/ContractorCustomer", "customer", "CONTRATA CLIENTE")
    ]
    cursor.executemany("INSERT INTO locations (company_id, name, path, type, category) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (path) DO NOTHING", locations_data)

    # --- 3. Tipos de Operación ---
    op_types = [
        ("Compra Nacional", "IN", "Entrada de mercancía de proveedor.", "vendor", "internal"),
        ("Consignación Recibida", "IN", "Entrada de mercancía propiedad de cliente.", "customer", "internal"),
        ("Transferencia entre Almacenes", "INT", "Mueve stock entre tus almacenes internos.", "internal", "internal"),
        ("Consignación Entregada", "INT", "Envía tu stock a un contratista.", "internal", "internal"),
        ("Devolución de Contrata", "INT", "El contratista te devuelve stock.", "internal", "internal"),
        ("Transferencia entre Contratas", "INT", "Mueve stock entre contratistas.", "internal", "internal"),
        ("Devolución a Proveedor", "OUT", "Devuelves mercancía a proveedor.", "internal", "vendor"),
        ("Devolución a Cliente", "OUT", "Devuelves mercancía (consignada) a cliente.", "internal", "customer"),
        ("Traspaso Contrata Cliente", "OUT", "Stock de contratista se entrega a cliente final.", "internal", "customer"),
        ("Liquidación por OT", "OUT", "Salida material consumido en OT.", "internal", "customer")
    ]
    cursor.executemany("INSERT INTO operation_types (name, code, description, source_location_category, destination_location_category) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (name) DO NOTHING", op_types)

    # --- 4. Categorías de Almacén ---
    wh_categories = [("ALMACEN PRINCIPAL",), ("CONTRATISTA",)]
    cursor.executemany("INSERT INTO warehouse_categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", wh_categories)

    # --- 5. Categorías de Partner ---
    partner_categories = [("Proveedor Externo",), ("Proveedor Cliente",)]
    cursor.executemany("INSERT INTO partner_categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", partner_categories)
    conn.commit() # Hacemos commit aquí para que las siguientes SELECT funcionen

    # --- 6. Proveedores por defecto (con verificación) ---
    cursor.execute("SELECT id FROM partner_categories WHERE name = %s", ("Proveedor Cliente",))
    cat_cliente_row = cursor.fetchone()
    if not cat_cliente_row: raise Exception("No se encontró la categoría 'Proveedor Cliente'")
    cat_cliente_id = cat_cliente_row[0]

    cursor.execute("SELECT id FROM partner_categories WHERE name = %s", ("Proveedor Externo",))
    cat_externo_row = cursor.fetchone()
    if not cat_externo_row: raise Exception("No se encontró la categoría 'Proveedor Externo'")
    cat_externo_id = cat_externo_row[0]
    
    cursor.execute("INSERT INTO partners (company_id, name, category_id) VALUES (%s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING", (default_company_id, "Cliente Varios", cat_cliente_id))
    cursor.execute("INSERT INTO partners (company_id, name, category_id) VALUES (%s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING", (default_company_id, "Proveedor Varios", cat_externo_id))

    # --- 7. Datos Maestros (Productos, etc.) ---
    cursor.execute("INSERT INTO product_categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", ('General',))
    general_cat_id_row = cursor.fetchone()
    if general_cat_id_row:
        general_cat_id = general_cat_id_row[0]
    else:
        cursor.execute("SELECT id FROM product_categories WHERE name = %s", ('General',))
        general_cat_id_row = cursor.fetchone()
        if not general_cat_id_row: raise Exception("No se pudo crear o encontrar la categoría 'General'")
        general_cat_id = general_cat_id_row[0]

    cursor.execute("INSERT INTO uom (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id", ('Unidades',))
    uom_unidades_id_row = cursor.fetchone()
    if uom_unidades_id_row:
        uom_unidades_id = uom_unidades_id_row[0]
    else:
        cursor.execute("SELECT id FROM uom WHERE name = %s", ('Unidades',))
        uom_unidades_id_row = cursor.fetchone()
        if not uom_unidades_id_row: raise Exception("No se pudo crear o encontrar la UdM 'Unidades'")
        uom_unidades_id = uom_unidades_id_row[0]

    products_to_create = [
        (default_company_id, "Producto de Prueba", "PRUEBA001", general_cat_id, "none", uom_unidades_id, 'owned', 0),
        (default_company_id, "Cable UTP Cat 6", "OWN-CABLE-001", general_cat_id, "none", uom_unidades_id, 'owned', 0),
        (default_company_id, "Conector RJ45", "OWN-CONN-001", general_cat_id, "none", uom_unidades_id, 'owned', 0),
        (default_company_id, "Router Cliente Avanzado", "CON-ROUTER-SERIAL", general_cat_id, "serial", uom_unidades_id, 'consigned', 150.75),
        (default_company_id, "Antena WiFi Básica", "CON-ANTENNA-NOSERIAL", general_cat_id, "none", uom_unidades_id, 'consigned', 25.50)
    ]
    cursor.executemany("""
        INSERT INTO products (company_id, name, sku, category_id, tracking, uom_id, ownership, standard_price) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (sku) DO NOTHING
    """, products_to_create)
    
    # --- 8. Creamos los ALMACENES por defecto (AJUSTADO) ---
    cursor.execute("SELECT id, name FROM warehouse_categories")
    all_wh_categories_actual = cursor.fetchall()
    used_codes = set() 

    principal_cat = next((cat for cat in all_wh_categories_actual if cat[1] == "ALMACEN PRINCIPAL"), None)
    if principal_cat:
        # ¡CAMBIO! Pasamos 'cursor' en lugar de 'conn'
        create_warehouse_with_data(cursor, "Almacén Lima", "LIMA", default_company_id, principal_cat[0], for_existing=False) 
        used_codes.add("LIMA") 

    for category in all_wh_categories_actual:
        if category[1] == "ALMACEN PRINCIPAL": continue 

        cat_id = category[0]
        cat_name = category[1]
        warehouse_name = f"Almacén {cat_name.title()}"
        base_code = cat_name[:3].upper()
        warehouse_code = base_code
        counter = 2
        while warehouse_code in used_codes: 
            warehouse_code = f"{base_code[:2]}{counter}"
            counter += 1
        used_codes.add(warehouse_code)
        # ¡CAMBIO! Llamamos a _create_warehouse_with_cursor
        _create_warehouse_with_cursor(cursor, warehouse_name, warehouse_code, cat_id, default_company_id, "", "", "", "", "", "activo")

    # --- 9. Creamos el TIPO DE OPERACIÓN de Ajuste (AJUSTADO) ---
    cursor.execute("SELECT id FROM locations WHERE category='AJUSTE' LIMIT 1")
    adj_loc = cursor.fetchone()
    if adj_loc:
        adj_loc_id = adj_loc[0]
        
        cursor.execute("SELECT id FROM warehouses WHERE category_id = (SELECT id FROM warehouse_categories WHERE name='ALMACEN PRINCIPAL') LIMIT 1")
        default_wh_id_row = cursor.fetchone()
        default_wh_id = default_wh_id_row[0] if default_wh_id_row else 1 # Fallback al primer almacén
        
        # ¡CAMBIO! %s y ON CONFLICT
        cursor.execute("""
            INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
            VALUES (%s, %s, 'ADJ', %s, %s, %s) ON CONFLICT (name) DO NOTHING
        """, (default_company_id, "Ajustes de Inventario", default_wh_id, adj_loc_id, adj_loc_id))
    else:
        print("[WARN] No se encontró ubicación de ajuste (category='AJUSTE'). No se creó el tipo de operación ADJ.")

    # --- 9. Crear datos de RBAC (con verificación) ---
    print(" -> Creando datos iniciales de RBAC (Usuarios, Roles, Permisos)...")
    try:
        cursor.execute("INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING RETURNING id", ("Administrador", "Acceso total al sistema"))
        admin_role_id_row = cursor.fetchone()
        if admin_role_id_row:
            admin_role_id = admin_role_id_row[0]
        else:
            cursor.execute("SELECT id FROM roles WHERE name = %s", ("Administrador",))
            admin_role_id_row = cursor.fetchone()
            if not admin_role_id_row: raise Exception("No se pudo crear o encontrar el rol 'Administrador'")
            admin_role_id = admin_role_id_row[0]
        
        cursor.execute("INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", ("Liquidador", "Puede gestionar liquidaciones"))
        cursor.execute("INSERT INTO roles (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", ("Asistente de Almacén", "Puede gestionar operaciones de almacén"))

        admin_pass_hashed = hash_password("admin")
        cursor.execute("INSERT INTO users (username, hashed_password, full_name, role_id) VALUES (%s, %s, %s, %s) ON CONFLICT (username) DO NOTHING", ("admin", admin_pass_hashed, "Administrador", admin_role_id))

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
                perm_id = perm_id_row[0]
            else:
                cursor.execute("SELECT id FROM permissions WHERE key = %s", (key,))
                perm_id_row = cursor.fetchone()
                if not perm_id_row: raise Exception(f"No se pudo crear o encontrar el permiso '{key}'")
                perm_id = perm_id_row[0]
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

def hash_password(password):
    """Genera un hash SHA-256 para la contraseña."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def check_password(hashed_password, plain_password):
    """Verifica si la contraseña coincide con el hash."""
    return hashed_password == hash_password(plain_password)

def get_product_categories(): return execute_query("SELECT id, name FROM product_categories ORDER BY name", fetchall=True)

def create_product_category(name):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO product_categories (name) VALUES (%s) RETURNING id", 
                    (name,)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e: # Captura genérica de psycopg2
        if "product_categories_name_key" in str(e): # Revisa el nombre real del 'constraint'
            raise ValueError(f"La categoría de producto '{name}' ya existe.")
        else:
            raise e

def update_product_category(cat_id, name):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE product_categories SET name = %s WHERE id = %s",
                    (name, cat_id)
                )
                conn.commit()
    except Exception as e:  # Captura genérica para psycopg2
        # Detectar violación de restricción única (nombre duplicado)
        if "product_categories_name_key" in str(e):  # Ajusta al nombre real del constraint
            raise ValueError(f"La categoría de producto '{name}' ya existe.")
        else:
            raise e
    
def delete_product_category(cat_id):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                # PostgreSQL lanzará un error de integridad si la categoría está en uso
                cursor.execute("DELETE FROM product_categories WHERE id = %s", (cat_id,))
                conn.commit()
        return True, "Categoría de producto eliminada."
    except Exception as e:
        # Detectar error de llave foránea (común en PostgreSQL para esto)
        if "violates foreign key constraint" in str(e):
             return False, "No se puede eliminar: Esta categoría está asignada a uno o más productos."
        print(f"[DB-ERROR] delete_product_category: {e}")
        return False, f"Error al eliminar: {e}"

def delete_product(product_id):
    try:
        with connect_db() as conn:
            cursor = conn.cursor()
            
            # --- EL GUARDIÁN ---
            # 1. Revisa si hay movimientos para este producto.
            cursor.execute("SELECT COUNT(*) FROM stock_moves WHERE product_id =  %s", (product_id,))
            move_count = cursor.fetchone()[0]
            
            # 2. Si hay movimientos, se niega a borrar y devuelve un error.
            if move_count > 0:
                return (False, "Este producto no se puede eliminar porque tiene movimientos de inventario registrados.")
            
            # 3. Si no hay movimientos, procede con la eliminación.
            cursor.execute("DELETE FROM stock_quants WHERE product_id =  %s", (product_id,))
            cursor.execute("DELETE FROM stock_lots WHERE product_id =  %s", (product_id,))
            # La siguiente línea ya no es necesaria aquí, pero la dejamos por si se usa en otro contexto.
            cursor.execute("DELETE FROM stock_moves WHERE product_id =  %s", (product_id,))
            cursor.execute("DELETE FROM products WHERE id =  %s", (product_id,))
            conn.commit()
            
            return (True, "Producto eliminado correctamente.")
            
    except Exception as e:
        return (False, f"Error inesperado en la base de datos: {e}")

def get_stock_on_hand(warehouse_id=None):
    base_query = """
    SELECT
        p.sku, p.name as product_name, pc.name as category_name,
        w.name as warehouse_name,
        sl.name as lot_name,
        SUM(sq.quantity) as quantity,
        u.name as uom_name,
        -- Añadir IDs/columnas para agrupar
        w.id, p.id, sl.id, pc.id, u.id
    FROM stock_quants sq
    JOIN products p ON sq.product_id = p.id
    JOIN locations l ON sq.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN stock_lots sl ON sq.lot_id = sl.id
    LEFT JOIN uom u ON p.uom_id = u.id
    WHERE sq.quantity > 0
    """
    params = []
    if warehouse_id:
        base_query += " AND w.id = %s"
        params.append(warehouse_id)

    # --- ¡ESTA ES LA CORRECCIÓN! ---
    base_query += " GROUP BY w.id, w.name, p.id, p.sku, p.name, sl.id, sl.name, pc.id, pc.name, u.id, u.name"
    # --- FIN DE LA CORRECCIÓN ---
    
    base_query += " ORDER BY w.name, p.name, sl.name"
    return execute_query(base_query, tuple(params), fetchall=True)

def get_reserved_stock(product_id, location_id):
    """
    Calcula la cantidad de stock que está físicamente en una ubicación
    pero ya está comprometida en operaciones 'listo' (salientes).
    """
    query = """
        SELECT SUM(sm.product_uom_qty) as reserved_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE sm.product_id = %s
          AND sm.location_src_id = %s
          AND p.state = 'listo'  -- Solo movimientos listos para salir
          AND sm.state != 'cancelled' -- Ignorar cancelados por seguridad
    """
    result = execute_query(query, (product_id, location_id), fetchone=True)
    return result['reserved_qty'] if result and result['reserved_qty'] else 0.0

def get_incoming_stock(product_id, location_id):
    """
    Calcula la cantidad de stock que se espera recibir en una ubicación
    desde operaciones 'listo' (entrantes).
    """
    query = """
        SELECT SUM(sm.product_uom_qty) as incoming_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE sm.product_id = %s
          AND sm.location_dest_id = %s
          AND p.state = 'listo'  -- Solo movimientos listos para entrar
          AND sm.state != 'cancelled'
    """
    result = execute_query(query, (product_id, location_id), fetchone=True)
    return result['incoming_qty'] if result and result['incoming_qty'] else 0.0

def get_uoms(): return execute_query("SELECT id, name FROM uom ORDER BY name", fetchall=True)
def create_uom(name):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO uom (name) VALUES (%s) RETURNING id",
                    (name,)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e:  # Captura genérica (psycopg2 lanza distintas excepciones)
        # Detecta si el error proviene de una restricción única en la columna 'name'
        if "uom_name_key" in str(e):  # Asegúrate de que este sea el nombre real del constraint
            raise ValueError(f"La unidad de medida '{name}' ya existe.")
        else:
            raise e

def update_uom(uom_id, name):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE uom SET name = %s WHERE id = %s",
                    (name, uom_id)
                )
                conn.commit()
    except Exception as e:  # Manejo genérico de psycopg2
        # Verifica si el error proviene de una restricción única (duplicado de 'name')
        if "uom_name_key" in str(e):  # Ajusta según el nombre real del constraint
            raise ValueError(f"La unidad de medida '{name}' ya existe.")
        else:
            raise e

def delete_uom(uom_id):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM uom WHERE id = %s", (uom_id,))
                conn.commit()
        return True, "Unidad de medida eliminada."
    except Exception as e:
        if "violates foreign key constraint" in str(e):
             return False, "No se puede eliminar: Esta UdM está asignada a uno o más productos."
        print(f"[DB-ERROR] delete_uom: {e}")
        return False, f"Error al eliminar: {e}"
def get_products(company_id, ownership_filter=None):
    """
    Obtiene los productos, AHORA INCLUYENDO los nuevos campos.
    """
    query = """
    SELECT p.id, p.name, p.sku, p.type, pc.name as category_name, u.name as uom_name, 
           p.tracking, p.ownership, p.standard_price  -- <-- AÑADIDO
    FROM products p
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    WHERE p.company_id =  %s
    """
    params = (company_id,)
    if ownership_filter:
        query += " AND p.ownership =  %s"
        params += (ownership_filter,)
    query += " ORDER BY p.name"
    return execute_query(query, params, fetchall=True)

def get_product_details(product_id):
    """ Obtiene un producto por su ID con todos los campos del schema. """
    query = """
        SELECT 
            p.id, p.company_id, p.name, p.sku, 
            p.category_id, pc.name as category_name, 
            p.uom_id, u.name as uom_name,
            p.tracking, p.ownership, p.standard_price, p.type
        FROM products p
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        LEFT JOIN uom u ON p.uom_id = u.id
        WHERE p.id = %s
    """
    return execute_query(query, (product_id,), fetchone=True)

def create_product(name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO products (name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                    (name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e:
        if "products_sku_key" in str(e):
            raise ValueError(f"El SKU '{sku}' ya existe.")
        else:
            raise e
def update_product(product_id, name, sku, category_id, tracking, uom_id, ownership, standard_price):
    with connect_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE products SET name =  %s, sku =  %s, category_id =  %s, tracking =  %s, uom_id =  %s, ownership =  %s, standard_price =  %s WHERE id =  %s",
            (name, sku, category_id, tracking, uom_id, ownership, standard_price, product_id)
        )
        conn.commit()

def get_work_orders(company_id):
    # ... (la consulta base es la misma) ...
    query = """
    SELECT
        wo.id, wo.ot_number, wo.customer_name, wo.address,
        wo.service_type, wo.job_type,
        wo.phase,
        wo.date_registered,
        COALESCE(p_draft.id, p_done.id) as picking_id,
        COALESCE(w_draft.name, w_done.name, 'N/A') as warehouse_name,
        COALESCE(l_draft.path, l_done.path, '-') as location_src_path,
        COALESCE(p_draft.service_act_number, p_done.service_act_number, '') as service_act_number,
        
        -- --- ¡CAMBIO DE SINTAXIS! strftime -> TO_CHAR ---
        COALESCE(
            TO_CHAR(p_draft.attention_date, 'DD/MM/YYYY'),
            TO_CHAR(p_done.attention_date, 'DD/MM/YYYY'),
            ''
        ) as attention_date_str
        -- --- FIN CAMBIO ---

    FROM work_orders wo
    LEFT JOIN pickings p_draft ON wo.id = p_draft.work_order_id AND p_draft.state = 'draft' AND p_draft.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_draft ON p_draft.warehouse_id = w_draft.id
    LEFT JOIN locations l_draft ON p_draft.location_src_id = l_draft.id
    LEFT JOIN pickings p_done ON wo.id = p_done.work_order_id AND p_done.state = 'done' AND p_done.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_done ON p_done.warehouse_id = w_done.id
    LEFT JOIN locations l_done ON p_done.location_src_id = l_done.id
    WHERE wo.company_id = %s
    ORDER BY wo.id DESC
    """
    # Nota: PostgreSQL usa %s como placeholder, no  %s
    results = execute_query(query, (company_id,), fetchall=True)
    return results

def get_work_order_details(wo_id): return execute_query("SELECT * FROM work_orders WHERE id =  %s", (wo_id,), fetchone=True)

def create_work_order(company_id, ot_number, customer, address, service, job_type):
    print(f"[DB-DEBUG] Creando Work Order: OT={ot_number}, Cliente={customer}, Comp={company_id}")
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO work_orders
                       (company_id, ot_number, customer_name, address, service_type, job_type)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       RETURNING id""", # <-- ¡CAMBIO! Pedimos que devuelva el ID
                    (company_id, ot_number, customer, address, service, job_type)
                )
                new_wo_id = cursor.fetchone()[0] # <-- ¡CAMBIO! Obtenemos el ID devuelto
                conn.commit()
                return new_wo_id
    except Exception as e: # Captura genérica para psycopg2.Error
        if "work_orders_ot_number_key" in str(e): # El nombre de la restricción UNIQUE
            print(f"[DB-WARN] Intento de crear OT duplicada: {ot_number}")
            raise ValueError(f"La Orden de Trabajo '{ot_number}' ya existe.")
        else:
            print(f"Error DB: {e}")
            traceback.print_exc()
            raise e

def get_picking_type_by_code(warehouse_id, code): return execute_query("SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id =  %s AND code =  %s", (warehouse_id, code), fetchone=True)

def get_available_serials_at_location(product_id, location_id):
    """
    Devuelve las series que están físicamente en la ubicación Y NO están reservadas
    por ninguna otra operación en estado 'listo'.
    """
    if not location_id: return []

    query = """
        SELECT sl.id, sl.name
        FROM stock_quants sq
        JOIN stock_lots sl ON sq.lot_id = sl.id
        WHERE sq.product_id = %s
          AND sq.location_id = %s
          AND sq.quantity > 0
          -- CRÍTICO: Excluir series que ya están en líneas de movimientos 'listo'
          AND sl.id NOT IN (
              SELECT sml.lot_id
              FROM stock_move_lines sml
              JOIN stock_moves sm ON sml.move_id = sm.id
              JOIN pickings p ON sm.picking_id = p.id
              WHERE sm.location_src_id = %s  -- En la misma ubicación origen
                AND p.state = 'listo'        -- Operación confirmada pero no realizada
                AND sm.state != 'cancelled'
          )
        ORDER BY sl.name
    """
    # Pasamos location_id dos veces: una para quants, otra para la subconsulta de moves
    return execute_query(query, (product_id, location_id, location_id), fetchall=True)

# En app/database.py (REEMPLAZA la función create_picking existente)

def create_picking(name, picking_type_id, location_src_id, location_dest_id, company_id, responsible_user, work_order_id=None):
    """
    Crea un nuevo picking (albarán) usando el pool de conexiones.
    (Versión Migrada)
    """
    s_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    query = """INSERT INTO pickings (company_id, name, picking_type_id, location_src_id, location_dest_id, scheduled_date, state, work_order_id, responsible_user) 
               VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s) RETURNING id"""
    params = (company_id, name, picking_type_id, location_src_id, location_dest_id, s_date, work_order_id, responsible_user)
    
    # Llamar a la nueva función de ESCRITURA
    new_id_row = execute_commit_query(query, params, fetchone=True)
    
    if new_id_row and new_id_row[0]:
        return new_id_row[0] # Devolver el ID
    else:
        raise Exception("No se pudo crear el picking, no se devolvió ID.")

def get_lot_by_name(cursor, product_id, lot_name):
    if cursor is None:
        return execute_query("SELECT id FROM stock_lots WHERE product_id =  %s AND name =  %s", (product_id, lot_name), fetchone=True)
    else:
        cursor.execute("SELECT id FROM stock_lots WHERE product_id =  %s AND name =  %s", (product_id, lot_name))
        return cursor.fetchone()
    
def create_lot(cursor, product_id, lot_name):
    """
    Crea un nuevo lote (stock_lot) usando un cursor existente y devuelve el nuevo ID.
    (Versión PostgreSQL con ON CONFLICT)
    """
    try:
        # Intentamos insertar. Si ya existe (ON CONFLICT), no hacemos nada.
        cursor.execute(
            "INSERT INTO stock_lots (name, product_id) VALUES (%s, %s) ON CONFLICT (product_id, name) DO NOTHING RETURNING id",
            (lot_name, product_id)
        )
        new_id_row = cursor.fetchone()
        
        if new_id_row:
            return new_id_row[0] # Devolvemos el ID de la fila recién insertada
        else:
            # Si no se insertó nada (porque ya existía), lo buscamos
            print(f" -> Lote {lot_name} ya existe, obteniendo ID...")
            cursor.execute("SELECT id FROM stock_lots WHERE product_id = %s AND name = %s", (product_id, lot_name))
            existing_row = cursor.fetchone()
            if existing_row:
                return existing_row[0]
            else:
                raise Exception(f"Fallo crítico al crear o encontrar el lote {lot_name}.")
    except Exception as e:
        print(f"Error en create_lot: {e}")
        raise e
def update_stock_quant(cursor, product_id, location_id, quantity_change, lot_id=None):
    op_type = "SUMANDO" if quantity_change > 0 else "RESTANDO"
    print(f"    [+] update_stock_quant: {op_type} {abs(quantity_change)} uds. del Producto ID {product_id} en Ubicación ID {location_id}")

    # Si la ubicación es None, es un error de lógica, detenemos todo para evitar fallos.
    if location_id is None:
        raise ValueError(f"Error Crítico: Se intentó actualizar stock con una location_id nula para el producto ID: {product_id}.")
    lot_check_query = "lot_id =  %s" if lot_id else "lot_id IS NULL"
    params = (product_id, location_id)
    if lot_id:
        params += (lot_id,)
    cursor.execute(f"SELECT id, quantity FROM stock_quants WHERE product_id =  %s AND location_id =  %s AND {lot_check_query}", params)
    quant = cursor.fetchone()
    
    # --- AÑADE ESTE PRINT ---
    stock_previo = quant['quantity'] if quant else 0
    print(f"        - Stock previo: {stock_previo}")
    # --- FIN DEL PRINT ---
    
    if quant:
        # Si el registro de stock ya existe, lo actualizamos
        new_quantity = quant['quantity'] + quantity_change
        print(f"        - Calculando... Nuevo stock: {new_quantity}")

        if new_quantity < -0.001 and quantity_change < 0:
            # Si estamos restando y el resultado es negativo, es un error fatal.
            raise ValueError(f"Stock insuficiente para el producto ID {product_id} en la ubicación ID {location_id}.")

        if new_quantity > 0.001:
            print("        - Acción: Actualizando cantidad existente.") # <-- AÑADIR
            cursor.execute("UPDATE stock_quants SET quantity =  %s WHERE id =  %s", (new_quantity, quant['id']))
        else:
            print("        - Acción: Eliminando registro (stock es cero o negativo).") # <-- AÑADIR
            cursor.execute("DELETE FROM stock_quants WHERE id =  %s", (quant['id'],))
    elif quantity_change > 0.001:
        print("        - Acción: Creando nuevo registro de stock.") # <-- AÑADIR

        # Si el registro no existe y estamos AÑADIENDO stock, lo creamos
        cursor.execute(
            "INSERT INTO stock_quants (product_id, location_id, lot_id, quantity) VALUES ( %s,  %s,  %s,  %s)",
            (product_id, location_id, lot_id, quantity_change)
        )
    elif quantity_change < -0.001:
        print("        - ACCIÓN: NINGUNA. Se intentó restar stock de un registro inexistente.") # <-- AÑADIR

        # --- LÓGICA CORREGIDA (El caso que faltaba) ---
        # Si el registro no existe y estamos INTENTANDO RESTAR stock, es un error.
        # La pre-validación de stock debería haber detenido esto. Si llegamos aquí, es un bug.
        # No hacemos nada en la base de datos, pero la función que llama (process_picking_validation)
        # ya habrá detectado el stock insuficiente. Este 'else' es una seguridad extra.
        pass

def _check_stock_with_cursor(cursor, picking_id, picking_type_code):
    """
    Función interna MEJORADA que verifica stock.
    Si el code es 'ADJ', valida contra FÍSICO.
    Si es 'OUT' o 'INT' o 'RET', valida contra DISPONIBLE (Físico - Reservado).
    """
    print(f"[DEBUG-STOCK] FASE PRE-VALIDACIÓN (Tipo: {picking_type_code}): Verificando disponibilidad...")
    
    # 1. Obtener los movimientos que queremos validar AHORA
    cursor.execute("""
        SELECT 
            sm.id AS move_id,
            sm.product_uom_qty, 
            sm.product_id, 
            sm.location_src_id,
            p.name as product_name,
            l.path as location_name
        FROM stock_moves sm
        JOIN products p ON sm.product_id = p.id
        JOIN locations l ON sm.location_src_id = l.id
        WHERE sm.picking_id = %s AND l.type = 'internal'
    """, (picking_id,))
    moves_to_validate = cursor.fetchall()

    if not moves_to_validate:
        return True, "No se requieren movimientos de stock interno."

    for move in moves_to_validate:
        product_id = move['product_id']
        location_id = move['location_src_id']
        qty_needed_now = move['product_uom_qty']

        # 2. Calcular Stock Físico Total (siempre se necesita)
        cursor.execute("""
            SELECT SUM(quantity) as total 
            FROM stock_quants 
            WHERE product_id = %s AND location_id = %s
        """, (product_id, location_id))
        quant_result = cursor.fetchone()
        physical_stock = quant_result['total'] if quant_result and quant_result['total'] else 0.0

        # 3. Lógica condicional de validación
        real_available = 0.0
        validation_mode = ""

        if picking_type_code == 'ADJ':
            # --- LÓGICA DE AJUSTE ---
            # Para Ajustes, el disponible ES el físico.
            real_available = physical_stock
            validation_mode = "Físico"
        
        else:
            # --- LÓGICA DE OPERACIONES (OUT, INT, RET) ---
            # Para Operaciones, el disponible es Físico MENOS Reservado.
            validation_mode = "Disponible Real"
            cursor.execute("""
                SELECT SUM(sm.product_uom_qty) as reserved_others
                FROM stock_moves sm
                JOIN pickings p ON sm.picking_id = p.id
                WHERE sm.product_id = %s
                  AND sm.location_src_id = %s
                  AND p.state = 'listo'
                  AND p.id != %s  -- <-- CLAVE: Excluir el picking actual
            """, (product_id, location_id, picking_id))
            reserved_result = cursor.fetchone()
            reserved_others = reserved_result['reserved_others'] if reserved_result and reserved_result['reserved_others'] else 0.0
            
            real_available = physical_stock - reserved_others
            print(f"   [STOCK-CHECK] Prod {product_id} @ Loc {location_id}: Físico={physical_stock}, ReservadoOtros={reserved_others} -> DisponibleReal={real_available}")

        # 4. Comparación Final
        # (Permitir ajuste negativo si el stock físico es suficiente)
        if picking_type_code == 'ADJ' and qty_needed_now < 0:
             if physical_stock < abs(qty_needed_now):
                 msg = (f"Stock físico insuficiente para '{move['product_name']}' en '{move['location_name']}'. "
                        f"Se necesita ajustar: {qty_needed_now}. Físico actual: {physical_stock}.")
                 print(f"[DEBUG-STOCK] FALLO: {msg}")
                 return False, msg
        # Validación estándar para salidas o ajustes positivos
        elif real_available < qty_needed_now:
            msg = (f"Stock insuficiente para '{move['product_name']}' en '{move['location_name']}'. "
                   f"Requerido: {qty_needed_now}. Disponible ({validation_mode}): {real_available}.")
            print(f"[DEBUG-STOCK] FALLO: {msg}")
            return False, msg

    print(f"[DEBUG-STOCK] PRE-VALIDACIÓN (Tipo: {picking_type_code}) EXITOSA.")
    return True, "Stock disponible verificado."

def check_stock_for_picking(picking_id):
    """(MIGRADO) Función pública que usa el POOL para verificar el stock."""
    global db_pool
    conn = None
    try:
        conn = db_pool.getconn() # Tomar conexión del pool
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            cursor.execute("""
                SELECT pt.code 
                FROM pickings p
                JOIN picking_types pt ON p.picking_type_id = pt.id
                WHERE p.id = %s
            """, (picking_id,))
            picking_info = cursor.fetchone()
            
            if not picking_info:
                return False, "Error: No se encontró el albarán."
            
            picking_type_code = picking_info['code']
            
            # Llamar a la función helper que usa el cursor
            # (Asumimos que _check_stock_with_cursor existe y es correcta)
            return _check_stock_with_cursor(cursor, picking_id, picking_type_code)
            
    except Exception as e:
        print(f"Error inesperado en la verificación de stock: {e}")
        traceback.print_exc()
        return False, str(e)
    finally:
        if conn:
            db_pool.putconn(conn) # Devolver conexión al pool

def _process_picking_validation_with_cursor(cursor, picking_id, moves_with_tracking):
    """
    (VERSIÓN CORREGIDA) Prepara todos los movimientos y luego los ejecuta.
    Maneja correctamente las cantidades negativas de 'ADJ'.
    """
    
    # --- ¡CORRECCIÓN 1! ---
    # Obtenemos el albarán Y su 'type_code' al inicio
    cursor.execute("SELECT p.*, pt.code as type_code FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id WHERE p.id = %s", (picking_id,))
    picking = cursor.fetchone()
    
    if not picking:
        return False, "Albarán no encontrado."

    cursor.execute("SELECT sm.*, p.tracking FROM stock_moves sm JOIN products p ON sm.product_id = p.id WHERE sm.picking_id = %s", (picking_id,))
    all_moves = cursor.fetchall()

    # Obtener IDs de ubicaciones virtuales (lógica sin cambios)
    cursor.execute("SELECT id FROM locations WHERE category = 'PROVEEDOR'")
    vendor_loc_id = cursor.fetchone()['id']
    cursor.execute("SELECT id FROM locations WHERE category = 'CLIENTE'")
    customer_loc_id = cursor.fetchone()['id']
    cursor.execute("SELECT id FROM locations WHERE category = 'CONTRATA CLIENTE'")
    contractor_customer_loc_id = cursor.fetchone()['id']
    
    processed_moves = []
    for move in all_moves:
        move_dict = dict(move)
        source_loc_id = move_dict['location_src_id']
        dest_loc_id = move_dict['location_dest_id']
        
        # Lógica de reemplazo de ubicaciones (sin cambios)
        if picking['type_code'] == 'IN':
            source_loc_id = vendor_loc_id
        elif picking['type_code'] == 'OUT':
            op_rule = get_operation_type_details(picking['custom_operation_type'])
            if op_rule:
                if op_rule['destination_location_category'] == 'CLIENTE': dest_loc_id = customer_loc_id
                elif op_rule['destination_location_category'] == 'PROVEEDOR': dest_loc_id = vendor_loc_id
                elif op_rule['destination_location_category'] == 'CONTRATA CLIENTE': dest_loc_id = contractor_customer_loc_id
        
        move_dict['final_source_id'] = source_loc_id
        move_dict['final_dest_id'] = dest_loc_id
        processed_moves.append(move_dict)

    # --- ¡CORRECCIÓN 2! ---
    # Aquí es donde estaba el error. Ahora pasamos el 'picking_type_code'
    # que obtuvimos al inicio.
    success, message = _check_stock_with_cursor(cursor, picking_id, picking['type_code'])
    if not success:
        print(f"[DEBUG-STOCK] PRE-VALIDACIÓN FALLIDA: {message}")
        return False, message

    # --- FASE 2: EJECUCIÓN (CON LÓGICA CORREGIDA) ---
    # (El resto de esta función (Fase 2) no necesita cambios)
    print(f"[DEBUG-STOCK] FASE 2: Ejecutando movimientos de stock...")
    processed_serials_in_transaction = set()

    for move in processed_moves:
        product_id = move['product_id']; qty_done = move['quantity_done']
        final_source_id = move['final_source_id']; final_dest_id = move['final_dest_id']
        product_tracking = move['tracking']
        print(f"     - Moviendo Producto ID {product_id}: {qty_done} uds. de loc_id={final_source_id} a loc_id={final_dest_id}")

        if move['tracking'] == 'none':
            if picking['type_code'] == 'ADJ' and qty_done < 0:
                update_stock_quant(cursor, product_id, final_source_id, qty_done, None)
                update_stock_quant(cursor, product_id, final_dest_id, -qty_done, None)
            else:
                update_stock_quant(cursor, product_id, final_source_id, -qty_done, None)
                update_stock_quant(cursor, product_id, final_dest_id, qty_done, None)
        else: 
            move_tracking_data = moves_with_tracking.get(move['id'], {})
            for lot_name, qty in move_tracking_data.items():
                serial_key = (product_id, lot_name)
                if serial_key in processed_serials_in_transaction:
                    return False, f"Error: La serie '{lot_name}' está duplicada en esta operación."
                processed_serials_in_transaction.add(serial_key)
                
                lot_row = get_lot_by_name(cursor, product_id, lot_name)
                lot_id = lot_row[0] if lot_row else create_lot(cursor, product_id, lot_name)
                
                if picking['type_code'] == 'IN' and product_tracking == 'serial':
                    cursor.execute(
                        """SELECT SUM(sq.quantity) as total_stock 
                           FROM stock_quants sq
                           JOIN locations l ON sq.location_id = l.id 
                           WHERE sq.product_id = %s AND sq.lot_id = %s AND l.type = 'internal'""",
                        (product_id, lot_id) 
                    )
                    existing_quant_anywhere = cursor.fetchone()
                    if existing_quant_anywhere and existing_quant_anywhere['total_stock'] and existing_quant_anywhere['total_stock'] > 0.001:
                        return False, f"Error: La serie única '{lot_name}' ya existe con stock en el inventario."

                if picking['type_code'] == 'ADJ' and qty_done < 0:
                    update_stock_quant(cursor, product_id, final_source_id, -qty, lot_id)
                    update_stock_quant(cursor, product_id, final_dest_id, qty, lot_id) 
                else:
                    update_stock_quant(cursor, product_id, final_source_id, -qty, lot_id)
                    update_stock_quant(cursor, product_id, final_dest_id, qty, lot_id) 
                
                cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move['id'], lot_id, qty))

    # Actualizar estados (sin cambios)
    cursor.execute("UPDATE stock_moves SET state = 'done' WHERE picking_id = %s", (picking_id,))
    date_done_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("UPDATE pickings SET state = 'done', date_done = %s WHERE id = %s", (date_done_str, picking_id))
    return True, "Validado."

def process_picking_validation(picking_id, moves_with_tracking):
    """(MIGRADO) Función pública que maneja la transacción usando el POOL."""
    print(f"\n[DEBUG-STOCK] ================= INICIO VALIDACIÓN: PICKING ID {picking_id} =================")
    global db_pool
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            # Llama a la única función que contiene la lógica de negocio.
            # (Asumimos que _process_picking_validation_with_cursor existe)
            success, message = _process_picking_validation_with_cursor(cursor, picking_id, moves_with_tracking)
            
            if success:
                conn.commit()
                print(f"[DEBUG-STOCK] ================= FIN VALIDACIÓN: ÉXITO (COMMIT) =================")
                return True, message
            else:
                conn.rollback()
                print(f"[DEBUG-STOCK] ================= FIN VALIDACIÓN: FALLO (ROLLBACK) - Causa: {message} =================")
                return False, message

    except Exception as e:
        if conn: conn.rollback() # Rollback en error crítico
        print(f"Error inesperado en la validación: {e}")
        print(f"[DEBUG-STOCK] ================= FIN VALIDACIÓN: ERROR CRÍTICO (ROLLBACK) =================")
        return False, f"Error inesperado en la base de datos: {e}"
    finally:
        if conn: db_pool.putconn(conn)

def process_full_liquidation(wo_id, consumptions, retiros, service_act_number, date_attended_db, current_ui_location_id, user_name):
    """
    Finaliza una liquidación ATÓMICA (Consumos y/o Retiros).
    Versión corregida: Obtiene el warehouse_id antes de procesar las líneas.
    """
    print(f"[DB-LIQ-FULL] Iniciando FINALIZACIÓN ATÓMICA para WO ID: {wo_id}, LocSrcID: {current_ui_location_id}")
    
    # Validación previa esencial
    if not current_ui_location_id:
        return False, "Error interno: Falta ID de Ubicación de Origen para determinar el Almacén."
    if user_name is None: user_name = "Sistema"

    try:
        with connect_db() as conn:
            # Usamos DictCursor para facilitar el acceso a columnas por nombre
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                
                # --- 0. Obtener y validar datos comunes (Warehouse ID) ---
                cursor.execute("SELECT warehouse_id FROM locations WHERE id = %s", (current_ui_location_id,))
                wh_row = cursor.fetchone()
                if not wh_row:
                    return False, f"No se pudo encontrar el almacén asociado a la ubicación ID {current_ui_location_id}"
                
                main_warehouse_id = wh_row['warehouse_id'] # <-- Variable común para ambos procesos
                print(f" -> Almacén principal determinado: ID {main_warehouse_id}")

                # --- 1. Obtener detalles de la OT ---
                cursor.execute("SELECT company_id, phase FROM work_orders WHERE id = %s", (wo_id,))
                wo_details = cursor.fetchone()
                if not wo_details: return False, "La orden de trabajo no existe."
                if wo_details['phase'] == 'Liquidado': return False, "Esta OT ya ha sido liquidada."
                company_id = wo_details['company_id']

                picking_id_out = None
                picking_id_ret = None
                picking_name_out = ""
                picking_name_ret = ""
                moves_with_tracking_out = {}
                moves_with_tracking_ret = {}

                # --- PASO 1: CREAR/ACTUALIZAR BORRADORES ---
                
                # --- 1.A. Procesar Consumo (OUT) ---
                if consumptions:
                    print(" -> Paso 1A: Guardando borrador de Consumo (OUT)...")
                    consumo_data = {
                        'warehouse_id': main_warehouse_id, # Usamos la variable común
                        'location_src_id': current_ui_location_id,
                        'date_attended_db': date_attended_db, 
                        'service_act_number': service_act_number,
                        'lines_data': consumptions
                    }
                    # Llamamos a la función interna (que ya debe estar corregida para devolver 2 valores)
                    picking_id_out, moves_with_tracking_out = _create_or_update_draft_picking_internal(
                        cursor, wo_id, 'OUT', consumo_data, company_id, user_name
                    )
                    if picking_id_out is None:
                         raise Exception("Fallo al crear/actualizar borrador OUT (sin ID devuelto).")

                    cursor.execute("SELECT name FROM pickings WHERE id = %s", (picking_id_out,))
                    picking_name_out = cursor.fetchone()['name']
                    print(f" -> Borrador (OUT) '{picking_name_out}' (ID: {picking_id_out}) listo para validar.")
                
                # --- 1.B. Procesar Retiro (RET) ---
                if retiros:
                    print(" -> Paso 1B: Guardando borrador de Retiro (RET)...")
                    retiro_data = {
                        'warehouse_id': main_warehouse_id, # Usamos la variable común
                        # 'location_src_id' NO se usa para RET (usa el default del tipo de operación)
                        'date_attended_db': date_attended_db, 
                        'service_act_number': service_act_number,
                        'lines_data': retiros
                    }
                    picking_id_ret, moves_with_tracking_ret = _create_or_update_draft_picking_internal(
                        cursor, wo_id, 'RET', retiro_data, company_id, user_name
                    )
                    if picking_id_ret is None:
                        raise Exception("Fallo al crear/actualizar borrador RET (sin ID devuelto).")

                    cursor.execute("SELECT name FROM pickings WHERE id = %s", (picking_id_ret,))
                    picking_name_ret = cursor.fetchone()['name']
                    print(f" -> Borrador (RET) '{picking_name_ret}' (ID: {picking_id_ret}) listo para validar.")

                # --- PASO 2: VALIDAR LOS BORRADORES ---

                # --- 2.A. Validar Consumo (OUT) ---
                if picking_id_out:
                    print(f" -> Paso 2A: Validando borrador (OUT) {picking_id_out}...")
                    success_out, message_out = _process_picking_validation_with_cursor(cursor, picking_id_out, moves_with_tracking_out)
                    if not success_out:
                        raise Exception(f"Validación Consumo falló: {message_out}")
                    print(" -> Validación (OUT) exitosa.")

                # --- 2.B. Validar Retiro (RET) ---
                if picking_id_ret:
                    print(f" -> Paso 2B: Validando borrador (RET) {picking_id_ret}...")
                    success_ret, message_ret = _process_picking_validation_with_cursor(cursor, picking_id_ret, moves_with_tracking_ret)
                    if not success_ret:
                        raise Exception(f"Validación Retiro falló: {message_ret}")
                    print(" -> Validación (RET) exitosa.")

                # --- PASO 3: FINALIZAR LA OT ---
                if picking_id_out or picking_id_ret:
                    cursor.execute("UPDATE work_orders SET phase = 'Liquidado' WHERE id = %s", (wo_id,))
                    print("[DB-LIQ-FULL] Fase de OT actualizada a 'Liquidado'.")
                else:
                     print("[DB-LIQ-FULL] WARN: No hubo consumos ni retiros para liquidar.")

                conn.commit()
                print("[DB-LIQ-FULL] Transacción completada (COMMIT).")
                
                msg_parts = []
                if picking_name_out: msg_parts.append(f"Liquidación {picking_name_out} validada")
                if picking_name_ret: msg_parts.append(f"Retiro {picking_name_ret} validado")
                
                final_msg = ". ".join(msg_parts) + "." if msg_parts else "OT finalizada sin movimientos."
                return True, final_msg

    except Exception as e:
        print(f"[ERROR-LIQ-FULL] Error en process_full_liquidation: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al liquidar: {e}"

def get_or_create_by_name(cursor, table, name):
    if not name or not name.strip(): return None
    cursor.execute(f"SELECT id FROM {table} WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # ¡CAMBIO!
        cursor.execute(f"INSERT INTO {table} (name) VALUES (%s) RETURNING id", (name,))
        return cursor.fetchone()[0]

def upsert_product_from_import(company_id, sku, name, category_id, uom_id, tracking, ownership, price):
    """
    Intenta insertar o actualizar un producto.
    [CORREGIDO] Lanza una excepción (raise) si la base de datos falla.
    [CORREGIDO 2] Se eliminó 'product_type' del INSERT; se usa el DEFAULT de la BD.
    """
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                # 1. Intentar encontrar el producto
                cursor.execute(
                    "SELECT id FROM products WHERE sku = %s AND company_id = %s",
                    (sku, company_id)
                )
                existing_product = cursor.fetchone()
                
                if existing_product:
                    # 2. Si existe, ACTUALIZAR
                    product_id = existing_product['id']
                    cursor.execute(
                        """UPDATE products SET 
                           name = %s, category_id = %s, uom_id = %s, 
                           tracking = %s, ownership = %s, standard_price = %s 
                           WHERE id = %s""",
                        (name, category_id, uom_id, tracking, ownership, price, product_id)
                    )
                    conn.commit()
                    return "updated"
                else:
                    # 3. Si no existe, CREAR
                    # --- ¡CORRECCIÓN AQUÍ! ---
                    # Se quitó 'product_type' de la consulta
                    cursor.execute(
                        """INSERT INTO products 
                           (company_id, sku, name, category_id, uom_id, tracking, ownership, standard_price) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (company_id, sku, name, category_id, uom_id, tracking, ownership, price)
                    )
                    # --- FIN CORRECCIÓN ---
                    conn.commit()
                    return "created"

    except psycopg2.Error as db_err:
        print(f"Error procesando fila para SKU {sku}: {db_err}")
        raise db_err 
    except Exception as e:
        print(f"Error inesperado en upsert_product_from_import: {e}")
        raise e

def get_picking_details(picking_id):
    query = """
        SELECT p.*, pt.code as type_code 
        FROM pickings p 
        JOIN picking_types pt ON p.picking_type_id = pt.id 
        WHERE p.id =  %s
    """
    p_info = execute_query(query, (picking_id,), fetchone=True)

    moves_query = """
            SELECT 
                sm.id, pr.name, pr.sku, sm.product_uom_qty, 
                sm.quantity_done, pr.tracking, pr.id as product_id,
                u.name as uom_name,
                sm.price_unit,
                pr.standard_price,
                sm.cost_at_adjustment
            FROM stock_moves sm 
            JOIN products pr ON sm.product_id = pr.id
            LEFT JOIN uom u ON pr.uom_id = u.id
            WHERE sm.picking_id =  %s
        """
    moves = execute_query(moves_query, (picking_id,), fetchall=True)
    
    return p_info, moves

def add_stock_move_to_picking(picking_id, product_id, qty, loc_src_id, loc_dest_id, price_unit=0, partner_id=None):
    """
    (MIGRADO) Añade una línea de stock_move usando el pool de conexiones.
    """
    query = """INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done, location_src_id, location_dest_id, price_unit, partner_id) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"""
    params = (picking_id, product_id, qty, qty, loc_src_id, loc_dest_id, price_unit, partner_id)
    
    # Usamos el helper de escritura que hace commit y devuelve el ID
    new_id_row = execute_commit_query(query, params, fetchone=True)
    
    if new_id_row and new_id_row[0]:
        return new_id_row[0]
    else:
        raise Exception("No se pudo crear la línea de stock, no se devolvió ID.")

def get_warehouses(company_id):
    """
    Función de compatibilidad. Obtiene todos los almacenes activos,
    ordenados por nombre (comportamiento similar al original).
    Llama a la nueva función filtrada/ordenada internamente.
    """
    # Llama a la nueva función, pidiendo solo los 'activos' y ordenando por nombre
    # Si necesitas OTRO comportamiento por defecto, ajústalo aquí.
    return get_warehouses_filtered_sorted(
        company_id,
        filters={'status': 'activo'}, # Filtro por defecto (si antes solo mostraba activos)
        sort_by='name',             # Orden por defecto
        ascending=True
    )

def get_warehouse_details_by_id(warehouse_id: int):
    """
    Obtiene los detalles completos de un solo almacén por su ID,
    incluyendo el nombre de la categoría (para el WarehouseResponse schema).
    """
    query = """
    SELECT
        w.id, w.company_id, w.name, w.code,
        w.social_reason, w.ruc, w.email, w.phone, w.address,
        w.status, w.category_id,
        wc.name as category_name
    FROM warehouses w
    LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
    WHERE w.id = %s
    """
    return execute_query(query, (warehouse_id,), fetchone=True)

def _create_warehouse_with_cursor(cursor, name, code, category_id, company_id, social_reason, ruc, email, phone, address, status):
    """Función interna que AHORA incluye el estado. (Versión PostgreSQL)"""
    cursor.execute(
        """INSERT INTO warehouses (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (code) DO NOTHING
           RETURNING id""",
        (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status)
    )
    warehouse_id_row = cursor.fetchone()
    
    if not warehouse_id_row:
        print(f" -> Almacén con código '{code}' ya existía. Omitiendo creación de datos duplicados.")
        return # Si el almacén ya existía, no creamos sus ubicaciones/tipos de op de nuevo

    warehouse_id = warehouse_id_row[0]

    # Pasamos el cursor que ya tenemos, NO la conexión
    create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=True, warehouse_id=warehouse_id)

def create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=False, warehouse_id=None):
    """Crea ubicaciones y tipos de operación para un almacén. (Versión PostgreSQL)"""
    
    if not for_existing:
        cursor.execute(
            """INSERT INTO warehouses (company_id, name, code, category_id, status) 
               VALUES (%s, %s, %s, %s, 'activo') 
               ON CONFLICT (code) DO NOTHING 
               RETURNING id""", 
            (company_id, name, code, category_id)
        )
        warehouse_id_row = cursor.fetchone()
        if not warehouse_id_row:
            print(f" -> Almacén '{code}' ya existía (llamado desde create_warehouse_with_data). Omitiendo.")
            return
        warehouse_id = warehouse_id_row[0]

    # --- 1. Crear Ubicación de Stock Principal ---
    stock_loc_name = f"{code}/Stock"
    # (Corregido el INSERT, tu versión SQLite tenía un error aquí)
    cursor.execute(
        """INSERT INTO locations (company_id, name, path, type, category, warehouse_id) 
           VALUES (%s, 'Stock', %s, 'internal', %s, %s) 
           ON CONFLICT (path) DO NOTHING 
           RETURNING id""",
        (company_id, stock_loc_name, "ALMACEN PRINCIPAL", warehouse_id)
    )
    stock_loc_id_row = cursor.fetchone()
    if not stock_loc_id_row: # Si ya existía
        cursor.execute("SELECT id FROM locations WHERE path = %s", (stock_loc_name,))
        stock_loc_id_row = cursor.fetchone()
    stock_loc_id = stock_loc_id_row[0]

    # --- 2. Crear Ubicación de Averiados ---
    damaged_loc_name = f"{code}/Averiados"
    cursor.execute(
        """INSERT INTO locations (company_id, name, path, type, category, warehouse_id) 
           VALUES (%s, 'Averiados', %s, 'internal', %s, %s) 
           ON CONFLICT (path) DO NOTHING 
           RETURNING id""",
        (company_id, damaged_loc_name, "AVERIADO", warehouse_id)
    )
    damaged_loc_id_row = cursor.fetchone()
    if not damaged_loc_id_row: # Si ya existía
        cursor.execute("SELECT id FROM locations WHERE path = %s", (damaged_loc_name,))
        damaged_loc_id_row = cursor.fetchone()
    damaged_loc_id = damaged_loc_id_row[0]
    
    # --- 3. Obtener IDs de Ubicaciones Virtuales (Sin "magic numbers") ---
    cursor.execute("SELECT id FROM locations WHERE category = 'PROVEEDOR' LIMIT 1")
    vendor_loc_row = cursor.fetchone()
    if not vendor_loc_row: raise Exception("Ubicación virtual 'PROVEEDOR' no encontrada. create_initial_data debe correr primero.")
    vendor_loc_id = vendor_loc_row[0]

    cursor.execute("SELECT id FROM locations WHERE category = 'CLIENTE' LIMIT 1")
    customer_loc_row = cursor.fetchone()
    if not customer_loc_row: raise Exception("Ubicación virtual 'CLIENTE' no encontrada.")
    customer_loc_id = customer_loc_row[0]
    
    # --- 4. Crear Tipos de Operación (usando %s y ON CONFLICT) ---
    picking_types_to_create = [
        (company_id, f"Recepciones {code}", 'IN', warehouse_id, vendor_loc_id, stock_loc_id),
        (company_id, f"Liquidaciones {code}", 'OUT', warehouse_id, stock_loc_id, customer_loc_id),
        (company_id, f"Despachos {code}", 'INT', warehouse_id, None, None),
        (company_id, f"Retiros {code}", 'RET', warehouse_id, customer_loc_id, damaged_loc_id)
    ]
    cursor.executemany("""
        INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON CONFLICT (name) DO NOTHING
    """, picking_types_to_create)
    
    print(f" -> Datos (ubicaciones, tipos op) creados para Almacén '{code}' (ID: {warehouse_id}).")
    # El commit se hace en create_initial_data

def create_warehouse(name, code, category_id, company_id, social_reason, ruc, email, phone, address, status):
    """Función pública que AHORA incluye el estado."""
    with connect_db() as conn:
        cursor = conn.cursor()
        _create_warehouse_with_cursor(cursor, name, code, category_id, company_id, social_reason, ruc, email, phone, address, status)

def update_warehouse(wh_id, name, code, category_id, social_reason, ruc, email, phone, address, status):
    """
    Actualiza un almacén y, si su código cambia, actualiza en cascada
    los paths de sus ubicaciones internas asociadas. (Versión PostgreSQL)
    """
    print(f"[DB-UPDATE-WH] Intentando actualizar Warehouse ID: {wh_id} con nuevo código: {code}")
    new_code_upper = code.strip().upper() if code else None
    if not new_code_upper:
        raise ValueError("El código de almacén no puede estar vacío.")

    conn = None
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            cursor.execute("SELECT code FROM warehouses WHERE id = %s", (wh_id,))
            old_data = cursor.fetchone()
            if not old_data:
                raise ValueError(f"No se encontró el almacén con ID {wh_id} para actualizar.")
            old_code = old_data[0]
            print(f" -> Código antiguo: '{old_code}', Código nuevo propuesto: '{new_code_upper}'")

            cursor.execute(
                """UPDATE warehouses SET
                   name = %s, code = %s, category_id = %s, social_reason = %s, ruc = %s,
                   email = %s, phone = %s, address = %s, status = %s
                   WHERE id = %s""",
                (name, new_code_upper, category_id, social_reason, ruc, email, phone, address, status, wh_id)
            )
            print(" -> Tabla 'warehouses' actualizada.")

            if old_code != new_code_upper:
                print(f" -> El código cambió. Actualizando paths en 'locations'...")
                old_prefix = f"{old_code}/"
                new_prefix = f"{new_code_upper}/"
                
                # --- ¡CAMBIO DE SINTAXIS! SUBSTR -> SUBSTRING ---
                cursor.execute(
                    """UPDATE locations
                       SET path = %s || SUBSTRING(path FROM %s)
                       WHERE warehouse_id = %s AND path LIKE %s""",
                    (new_prefix, len(old_prefix) + 1, wh_id, f"{old_prefix}%")
                )
                rows_affected = cursor.rowcount
                print(f" -> {rows_affected} paths de ubicaciones actualizados.")
            else:
                print(" -> El código no cambió, no se requiere actualización de paths.")

            conn.commit()
            print(" -> Cambios confirmados (commit).")
            return True

    except Exception as err:
        if conn: conn.rollback()
        print(f"[DB-ERROR] Error al actualizar almacén: {err}")
        if 'warehouses_code_key' in str(err):
             raise ValueError(f"El código '{new_code_upper}' ya está en uso por otro almacén.")
        else:
             traceback.print_exc(); raise err
    finally:
        if conn: conn.close()

# --- REEMPLAZA esta función completa en database.py (de nuevo) ---

def get_picking_types(company_id):
    """
    Obtiene los códigos de operación únicos para una compañía.
    (Versión PostgreSQL)
  
    """
    query = """
        SELECT 
            MIN(id) as id,
            MIN(name) as name,
            code
        FROM picking_types 
        WHERE company_id = %s
        GROUP BY code 
        ORDER BY CASE code 
            WHEN 'IN' THEN 1 
            WHEN 'INT' THEN 2 
            WHEN 'OUT' THEN 3 
            ELSE 4 
        END
    """
    # --- ¡LA CLAVE ESTÁ AQUÍ! ---
    # Asegúrate de que (company_id,) tenga la coma final
    # para que Python sepa que es un tuple, incluso si solo tiene un ítem.
    params_tuple = (company_id,)
    return execute_query(query, params_tuple, fetchall=True)

def get_next_picking_name(picking_type_id):
    pt = execute_query("SELECT wt.code as wh_code, pt.code as pt_code FROM picking_types pt JOIN warehouses wt ON pt.warehouse_id = wt.id WHERE pt.id =  %s", (picking_type_id,), fetchone=True)
    prefix = f"{pt['wh_code']}/{pt['pt_code']}/"
    count = execute_query("SELECT COUNT(*) FROM pickings WHERE name LIKE  %s", (f"{prefix}%",), fetchone=True)[0]
    return f"{prefix}{str(count + 1).zfill(5)}"
def get_all_locations(): return execute_query("SELECT id, path FROM locations ORDER BY path", fetchall=True)

def update_picking_header(pid, src_id, dest_id, ref, date_transfer, purchase_order, custom_op_type=None, partner_id=None):
    """
    [CORREGIDO] Actualiza la cabecera de un albarán usando el pool de conexiones.
    (Versión Migrada)
    """
    print(f"[DEBUG-DB] 10. DATOS RECIBIDOS EN LA FUNCIÓN DE BASE DE DATOS 'update_picking_header':")
    print(f"     - pid: {pid}, src_id: {src_id}, dest_id: {dest_id}, partner_id: {partner_id}")
    print(f"     - ref: '{ref}', date_transfer: '{date_transfer}', purchase_order: '{purchase_order}', custom_op_type: '{custom_op_type}'")
    
    query = """UPDATE pickings SET 
                   location_src_id = %s, 
                   location_dest_id = %s, 
                   partner_ref = %s, 
                   date_transfer = %s, 
                   purchase_order = %s, 
                   custom_operation_type = %s, 
                   partner_id = %s 
               WHERE id = %s"""
    params = (src_id, dest_id, ref, date_transfer, purchase_order, custom_op_type, partner_id, pid)

    try:
        # --- ¡CAMBIO CLAVE! ---
        # Usamos la nueva función que hace commit y usa el pool
        execute_commit_query(query, params)
        # --- FIN DEL CAMBIO ---
        
        print(f" -> Cabecera {pid} actualizada con TipoOp: {custom_op_type}")
        return True # Devolver True en éxito
        
    except Exception as e:
        # El error ya fue impreso por execute_commit_query
        print(f"!!! ERROR en update_picking_header (capturado en la función wrapper): {e}")
        return False # Devolver False en caso de fallo

def get_picking_type_details(type_id): return execute_query("SELECT * FROM picking_types WHERE id =  %s", (type_id,), fetchone=True)

def update_move_quantity_done(move_id, quantity_done):
    """
    (MIGRADO) Actualiza la cantidad de un move usando el pool de conexiones.
    """
    query = "UPDATE stock_moves SET product_uom_qty = %s, quantity_done = %s WHERE id = %s"
    params = (quantity_done, quantity_done, move_id)
    
    # Usamos el helper de escritura (sin fetchone)
    execute_commit_query(query, params)
    return True # Asumimos éxito si no hay excepción
def get_warehouse_categories():
    return execute_query("SELECT id, name FROM warehouse_categories ORDER BY name", fetchall=True)

def get_warehouse_category_details(cat_id):
    return execute_query("SELECT * FROM warehouse_categories WHERE id =  %s", (cat_id,), fetchone=True)

def create_warehouse_category(name):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO warehouse_categories (name) VALUES (%s) RETURNING id",
                    (name,)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e:  # Captura genérica de psycopg2
        if "warehouse_categories_name_key" in str(e):  # Ajusta al nombre real del constraint
            raise ValueError(f"La categoría de almacén '{name}' ya existe.")
        else:
            raise e

def update_warehouse_category(cat_id, name):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE warehouse_categories SET name = %s WHERE id = %s",
                    (name, cat_id)
                )
                conn.commit()
    except Exception as e:  # Captura genérica para psycopg2
        if "warehouse_categories_name_key" in str(e):  # Nombre del constraint único
            raise ValueError(f"La categoría de almacén '{name}' ya existe.")
        else:
            raise e

def delete_warehouse_category(cat_id):
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM warehouse_categories WHERE id = %s", (cat_id,))
                conn.commit()
        return True, "Categoría de almacén eliminada."
    except Exception as e:
        if "violates foreign key constraint" in str(e):
             return False, "No se puede eliminar: Esta categoría está en uso por uno o más almacenes."
        print(f"[DB-ERROR] delete_warehouse_category: {e}")
        return False, f"Error al eliminar: {e}"

def inactivate_warehouse(warehouse_id):
    """
    Archiva (desactiva) un almacén.
    """
    print(f"[DB-INACTIVATE-WH] Intentando archivar Warehouse ID: {warehouse_id}")
    try:
        with connect_db() as conn:
            # ¡USAMOS DictCursor AQUÍ!
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

                # --- VERIFICACIÓN DE SEGURIDAD (HARD BLOCK) ---
                # ¿Tiene stock actual en CUALQUIER ubicación de este almacén?
                cursor.execute(
                    """SELECT SUM(sq.quantity) as total_stock
                       FROM stock_quants sq
                       JOIN locations l ON sq.location_id = l.id
                       WHERE l.warehouse_id = %s""",
                    (warehouse_id,)
                )
                stock_result = cursor.fetchone()
                # Ahora esto funcionará porque stock_result es un diccionario
                if stock_result and stock_result['total_stock'] and abs(stock_result['total_stock']) > 0.001:
                    stock_total = stock_result['total_stock']
                    print(f" -> Bloqueado: Tiene stock ({stock_total}).")
                    # No hace falta rollback explícito si solo leímos, pero no hace daño.
                    return False, f"No se puede archivar: El almacén aún tiene stock ({stock_total} unidades)."

                # --- FASE 2: ARCHIVAR (SOFT DELETE) ---
                print(" -> Almacén limpio (sin stock). Procediendo a archivar...")
                cursor.execute(
                    "UPDATE warehouses SET status = 'inactivo' WHERE id = %s AND status = 'activo'",
                    (warehouse_id,)
                )
                rows_affected = cursor.rowcount
                
                conn.commit() # Confirmar la transacción

                if rows_affected > 0:
                    print(" -> Almacén archivado con éxito.")
                    return True, "Almacén archivado correctamente."
                else:
                    print(" -> El almacén ya estaba inactivo o no se encontró.")
                    return False, "El almacén no se pudo archivar (quizás ya estaba inactivo)."

    except Exception as e:
        print(f"Error CRÍTICO en inactivate_warehouse: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al archivar: {e}"

    except Exception as e:
        print(f"Error CRÍTICO en inactivate_warehouse: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al archivar: {e}"

def get_warehouse_id_by_name(name):
    """Busca el ID de un almacén por su nombre exacto."""
    if not name: return None
    result = execute_query("SELECT id FROM warehouses WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def get_warehouse_id_for_location(location_id):
    """Obtiene el warehouse_id de una ubicación interna específica."""
    if not location_id:
        return None
    result = execute_query(
        "SELECT warehouse_id FROM locations WHERE id =  %s AND type = 'internal'",
        (location_id,),
        fetchone=True
    )
    return result['warehouse_id'] if result else None

def create_work_order_from_import(company_id, ot_number, customer, address, service, job_type):
    """
    Crea una OT desde la importación, validando solo la existencia de la OT y usando la nueva estructura.
    NO maneja almacén.
    """
    # 1. Validar que la OT no exista previamente
    existing_ot = execute_query(
        "SELECT id FROM work_orders WHERE ot_number =  %s AND company_id =  %s",
        (ot_number, company_id),
        fetchone=True
    )
    if existing_ot:
        print(f"[DB-IMPORT] OT Omitida (duplicada): {ot_number}")
        return "skipped"

    # 2. Llamar a la función simplificada para crear la OT
    try:
        # Ya no pasamos warehouse_id ni date_attended
        new_id = create_work_order(
            company_id, ot_number, customer, address, service, job_type
        )
        if new_id:
             print(f"[DB-IMPORT] OT Creada: {ot_number}")
             return "created"
        else:
            # Podría fallar por duplicado si la validación anterior falla por concurrencia
            print(f"[DB-IMPORT] Error creando OT (posible duplicado no detectado antes): {ot_number}")
            return "error"
    except Exception as e:
         print(f"[DB-IMPORT] Error inesperado creando OT {ot_number}: {e}")
         return "error"
# --- FIN DEL REEMPLAZO ---

def validate_warehouse_names(names_to_check):
    """
    Recibe una lista de nombres de almacenes y devuelve una lista de aquellos que NO existen en la base de datos.
    """
    if not names_to_check:
        return []
    
    # Creamos placeholders ( %s, %s, %s) para la consulta SQL
    placeholders = ', '.join(' %s' for name in names_to_check)
    query = f"SELECT name FROM warehouses WHERE name IN ({placeholders})"
    
    # Obtenemos los nombres que SÍ existen
    results = execute_query(query, tuple(names_to_check), fetchall=True)
    existing_names = {row['name'] for row in results}
    
    # Comparamos la lista original con los existentes y devolvemos los que faltan
    non_existent_names = [name for name in names_to_check if name not in existing_names]
    
    return non_existent_names

# --- CRUD PARA CATEGORÍAS DE PROVEEDOR ---
def get_partner_categories():
    return execute_query("SELECT id, name FROM partner_categories ORDER BY name", fetchall=True)

def get_partner_details_by_id(partner_id: int):
    """
    Obtiene los detalles completos de un solo socio (partner) por su ID,
    incluyendo el nombre de la categoría (para el PartnerResponse schema).
    """
    query = """
    SELECT
        p.id, p.company_id, p.name, p.category_id,
        p.social_reason, p.ruc, p.email, p.phone, p.address,
        pc.name as category_name
    FROM partners p
    LEFT JOIN partner_categories pc ON p.category_id = pc.id
    WHERE p.id = %s
    """
    return execute_query(query, (partner_id,), fetchone=True)

def create_partner(name, category_id, company_id, social_reason, ruc, email, phone, address):
    """Crea un nuevo partner con todos sus detalles (versión PostgreSQL)."""
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO partners
                    (name, category_id, company_id, social_reason, ruc, email, phone, address)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (name, category_id, company_id, social_reason, ruc, email, phone, address)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id

    except Exception as e:  # psycopg2 lanza excepciones genéricas o específicas
        # Detecta el constraint UNIQUE en PostgreSQL
        if "partners_company_id_name_key" in str(e):
            print(f"[DB-WARN] Intento de crear Partner duplicado: {name} para Company ID: {company_id}")
            raise ValueError(f"Ya existe un proveedor/cliente con el nombre '{name}'.")
        else:
            raise e

def get_partner_category_id_by_name(name):
    """Busca el ID de una categoría de partner por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM partner_categories WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def update_partner(partner_id, name, category_id, social_reason, ruc, email, phone, address):
    """Actualiza los detalles de un partner existente (versión PostgreSQL)."""
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE partners SET
                        name = %s,
                        category_id = %s,
                        social_reason = %s,
                        ruc = %s,
                        email = %s,
                        phone = %s,
                        address = %s
                    WHERE id = %s
                    """,
                    (name, category_id, social_reason, ruc, email, phone, address, partner_id)
                )
                conn.commit()

    except Exception as e:  # psycopg2 genera errores distintos a sqlite3
        if "partners_company_id_name_key" in str(e):  # Nombre del constraint UNIQUE
            raise ValueError(f"Ya existe otro proveedor/cliente con el nombre '{name}'.")
        else:
            raise e

def delete_partner(partner_id):
    """Elimina un partner si no está siendo usado en operaciones."""
    try:
        with connect_db() as conn:
            cursor = conn.cursor()
            # --- VERIFICACIÓN DE SEGURIDAD ---
            # Revisar si se usa en pickings (como partner_id)
            cursor.execute("SELECT COUNT(*) FROM pickings WHERE partner_id =  %s", (partner_id,))
            picking_count = cursor.fetchone()[0]
            if picking_count > 0:
                return False, f"No se puede eliminar: está asociado a {picking_count} operación(es)."

            # Revisar si se usa en stock_moves (si tienes esa columna, aunque no parece)
            # cursor.execute("SELECT COUNT(*) FROM stock_moves WHERE partner_id =  %s", (partner_id,))
            # move_count = cursor.fetchone()[0]
            # if move_count > 0:
            #     return False, f"No se puede eliminar: está asociado a {move_count} movimiento(s) de stock."

            # --- Si no se usa, proceder a eliminar ---
            cursor.execute("DELETE FROM partners WHERE id =  %s", (partner_id,))
            conn.commit()
            return True, "Proveedor/Cliente eliminado correctamente."

    except Exception as e:
        print(f"Error en delete_partner: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al eliminar: {e}"

# --- CRUD PARA PROVEEDORES ---

def get_partners(company_id, category_name=None):
    query = """
        SELECT
            p.id, p.name, p.social_reason, p.ruc, p.email, p.phone, p.address,
            pc.name as category_name,
            p.category_id
        FROM partners p
        LEFT JOIN partner_categories pc ON p.category_id = pc.id
        WHERE p.company_id =  %s
    """
    params = (company_id,)
    if category_name:
        query += " AND pc.name =  %s"
        params += (category_name,)
    query += " ORDER BY p.name"
    return execute_query(query, params, fetchall=True)

def get_partner_details(partner_id):
    return execute_query("SELECT * FROM partners WHERE id =  %s", (partner_id,), fetchone=True)

def cancel_picking(picking_id):
    """(MIGRADO) Cancela un albarán usando el POOL."""
    global db_pool
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            cursor.execute("SELECT state FROM pickings WHERE id = %s", (picking_id,))
            picking = cursor.fetchone()
            
            if not picking:
                return False, "El albarán no existe."
            
            if picking['state'] not in ('draft', 'listo'):
                return False, f"Solo se pueden cancelar albaranes en estado 'Borrador' o 'Listo' (Estado actual: {picking['state']})."

            cursor.execute("UPDATE pickings SET state = 'cancelled' WHERE id = %s", (picking_id,))
            cursor.execute("UPDATE stock_moves SET state = 'cancelled' WHERE picking_id = %s", (picking_id,))
            
        conn.commit()
        return True, "Albarán cancelado correctamente."

    except Exception as e:
        if conn: conn.rollback()
        print(f"[DB-ERROR] cancel_picking: {e}")
        traceback.print_exc()
        return False, f"Error al cancelar: {e}"
    finally:
        if conn: db_pool.putconn(conn)

def get_location_path(location_id):
    """Devuelve el 'path' de una ubicación a partir de su ID."""
    loc = execute_query("SELECT path FROM locations WHERE id =  %s", (location_id,), fetchone=True)
    return loc['path'] if loc else ""

def get_serials_for_picking(picking_id):
    """
    Recupera todas las series/lotes usados en un albarán específico
    y los devuelve en un diccionario agrupado por move_id.
    """
    query = """
        SELECT sm.id as move_id, sl.name as lot_name, sml.qty_done
        FROM stock_moves sm
        JOIN stock_move_lines sml ON sm.id = sml.move_id
        JOIN stock_lots sl ON sml.lot_id = sl.id
        WHERE sm.picking_id =  %s
    """
    results = execute_query(query, (picking_id,), fetchall=True)

    serials_by_move = {}
    for row in results:
        move_id = row['move_id']
        if move_id not in serials_by_move:
            serials_by_move[move_id] = {}
        serials_by_move[move_id][row['lot_name']] = row['qty_done']

    return serials_by_move

def get_next_remission_number():
    """
    Calcula el siguiente número de guía de remisión correlativo.
    Formato: GR-00001
    """
    prefix = "GR-"
    last_remission = execute_query(
        "SELECT remission_number FROM pickings WHERE remission_number IS NOT NULL ORDER BY remission_number DESC LIMIT 1",
        fetchone=True
    )
    
    if last_remission and last_remission['remission_number']:
        last_number = int(last_remission['remission_number'].replace(prefix, ""))
        next_number = last_number + 1
    else:
        next_number = 1
        
    return f"{prefix}{str(next_number).zfill(5)}"

def delete_stock_move(move_id):
    """
    (MIGRADO) Elimina un move y sus move_lines usando el pool de conexiones.
    """
    global db_pool # Accedemos al pool global
    conn = None
    try:
        conn = db_pool.getconn() # Tomar una conexión del pool
        with conn.cursor() as cursor:
            # Primero, eliminar los detalles de series/lotes
            cursor.execute("DELETE FROM stock_move_lines WHERE move_id = %s", (move_id,))
            # Luego, eliminar la línea de movimiento principal
            cursor.execute("DELETE FROM stock_moves WHERE id = %s", (move_id,))
        conn.commit() # Hacer commit de la transacción
        return True
    except Exception as e:
        if conn:
            conn.rollback() # Revertir en caso de error
        print(f"Error en delete_stock_move: {e}")
        traceback.print_exc()
        raise e # Re-lanzar el error para que la API lo vea
    finally:
        if conn:
            db_pool.putconn(conn) # Devolver la conexión al pool

def mark_picking_as_ready(picking_id):
    """(MIGRADO) Cambia el estado a 'listo' usando el POOL."""
    global db_pool
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE pickings SET state = 'listo' WHERE id = %s AND state = 'draft'", (picking_id,))
            rows_affected = cursor.rowcount # Obtener filas afectadas
        conn.commit()
        return rows_affected # Devuelve 1 si tuvo éxito, 0 si no
    except Exception as e:
        if conn: conn.rollback()
        print(f"Error en mark_picking_as_ready: {e}")
        raise e
    finally:
        if conn: db_pool.putconn(conn)

def get_companies():
    return execute_query("SELECT id, name FROM companies ORDER BY name", fetchall=True)

def get_category_id_by_name(name):
    """Busca el ID de una categoría de producto por su nombre exacto."""
    if not name or not name.strip(): return None
    result = execute_query("SELECT id FROM product_categories WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def get_uom_id_by_name(name):
    """Busca el ID de una unidad de medida por su nombre exacto."""
    if not name or not name.strip(): return None
    result = execute_query("SELECT id FROM uom WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def get_internal_locations(company_id):
    """
    Devuelve una lista de ubicaciones internas (tipo 'internal')
    para una compañía, incluyendo su ID y PATH completo.
    """
    query = """
        SELECT
            l.id,
            l.path
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.type = 'internal' AND w.company_id = %s
        ORDER BY l.path
    """
    # Usamos %s para psycopg2 (PostgreSQL)
    results = execute_query(query, (company_id,), fetchall=True)
    
    # execute_query ahora devuelve un DictRow (que funciona como un dict)
    return results if results else []
# --- REEMPLAZA esta función en database.py ---

def get_locations_by_warehouse(warehouse_id):
    """
    Devuelve una lista de ubicaciones INTERNAS (tipo 'internal')
    asociadas a un UNICO almacén, incluyendo ID y PATH.
    Optimizado para filtrar directamente en SQL. (Versión PostgreSQL)
    """
    if not warehouse_id:
        return [] # Devolver lista vacía si no se proporciona warehouse_id

    query = """
        SELECT
            l.id,
            l.path,
            l.warehouse_id
        FROM locations l
        WHERE l.warehouse_id = %s AND l.type = 'internal'
        ORDER BY l.path
    """
    # Asegurarse de que warehouse_id sea entero si viene de un dropdown
    try:
        wh_id_int = int(warehouse_id)
    except (ValueError, TypeError):
        print(f"[DB-WARN] warehouse_id inválido en get_locations_by_warehouse: {warehouse_id}")
        return [] # ID inválido, devolver lista vacía

    results = execute_query(query, (wh_id_int,), fetchall=True)
    return results if results else []
def get_stock_for_product_location(product_id, location_id):
    """Obtiene la cantidad actual de un producto en una ubicación (sin lotes/series)."""
    result = execute_query(
        "SELECT SUM(quantity) as total FROM stock_quants WHERE product_id =  %s AND location_id =  %s",
        (product_id, location_id),
        fetchone=True
    )
    return result['total'] if result and result['total'] else 0

def create_draft_adjustment(company_id, user_name):
    print(f"[DB-ADJ] Creando nuevo ajuste en borrador para Cia: {company_id}")
    try:
        with connect_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("SELECT id FROM picking_types WHERE code = 'ADJ' AND company_id = %s", (company_id,))
                adj_picking_type = cursor.fetchone()
                if not adj_picking_type:
                    raise ValueError("No se encontró un tipo de operación 'ADJ' para esta compañía.")
                pt_id = adj_picking_type['id']

                cursor.execute("SELECT id FROM locations WHERE category = 'AJUSTE' AND company_id = %s", (company_id,))
                adj_loc = cursor.fetchone()
                if not adj_loc:
                    raise ValueError("No se encontró la ubicación virtual de 'Ajuste' (category='AJUSTE').")
                adj_loc_id = adj_loc['id']

                new_name = get_next_picking_name(pt_id)
                s_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    """INSERT INTO pickings (company_id, name, picking_type_id, location_src_id, location_dest_id, 
                                           scheduled_date, state, responsible_user, custom_operation_type) 
                       VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s) RETURNING id""",
                    (company_id, new_name, pt_id, adj_loc_id, adj_loc_id, 
                     s_date, user_name, "Ajuste de Inventario")
                )
                new_picking_id = cursor.fetchone()[0]
                conn.commit()
                print(f" -> Ajuste borrador '{new_name}' (ID: {new_picking_id}) creado.")
                return new_picking_id

    except Exception as e:
        print(f"Error en create_draft_adjustment: {e}"); traceback.print_exc()
        return None

def get_adjustments(company_id):
    """
    Obtiene todos los albaranes de tipo Ajuste de una compañía,
    AHORA INCLUYENDO los nuevos campos de trazabilidad.
    """
    query = """
        SELECT 
            p.id, p.name, p.state, strftime('%Y-%m-%d', p.scheduled_date) as date,
            l_src.path as src_path, l_dest.path as dest_path,
            p.responsible_user,
            p.adjustment_reason,
            p.notes,
            p.loss_confirmation
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN locations l_src ON p.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
        WHERE p.company_id =  %s AND pt.code = 'ADJ'
        ORDER BY p.id DESC
    """
    return execute_query(query, (company_id,), fetchall=True)

def get_operation_types_by_code(code):
    """Obtiene los tipos de operación detallados por su código ('IN', 'INT', 'OUT')."""
    # Asegúrate de que selecciona 'id' Y 'name'
    return execute_query("SELECT id, name FROM operation_types WHERE code = %s ORDER BY name", (code,), fetchall=True)

def get_operation_type_details(name):
    """Obtiene todos los detalles de una regla de tipo de operación por su nombre."""
    return execute_query("SELECT * FROM operation_types WHERE name =  %s", (name,), fetchone=True)

def get_warehouses_by_category(company_id, category_name):
    """
    Obtiene las UBICACIONES de stock de los almacenes que pertenecen a una categoría.
    Devuelve el ID de la UBICACIÓN y el nombre del ALMACÉN.
    """
    query = """
        SELECT l.id, w.name 
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE l.type = 'internal' AND w.company_id =  %s AND wc.name =  %s
        ORDER BY w.name
    """
    return execute_query(query, (company_id, category_name), fetchall=True)

def update_picking_fields(picking_id, fields_to_update):
    """Actualiza múltiples campos de un albarán a la vez."""
    set_clause = ", ".join([f"{key} =  %s" for key in fields_to_update.keys()])
    params = list(fields_to_update.values()) + [picking_id]
    with connect_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE pickings SET {set_clause} WHERE id =  %s", tuple(params))
        conn.commit()

def delete_picking(picking_id):
    """
    Elimina permanentemente un albarán y todos sus movimientos asociados.
    Solo permite la eliminación si el estado es 'draft' o 'listo'.
    """
    with connect_db() as conn:
        cursor = conn.cursor()
        
        # 1. VERIFICACIÓN DE SEGURIDAD: Comprobar el estado actual del albarán.
        picking = cursor.execute("SELECT state FROM pickings WHERE id =  %s", (picking_id,)).fetchone()
        
        if not picking:
            return False, "El albarán no existe."
        
        if picking['state'] != 'draft':
            return False, "Solo se pueden eliminar permanentemente los albaranes en estado 'Borrador'."

        # 2. Si es seguro, proceder con la eliminación en orden.
        print(f"[DEBUG-DB] Iniciando eliminación del albarán ID: {picking_id}")

        # Obtenemos los IDs de los movimientos asociados
        moves = cursor.execute("SELECT id FROM stock_moves WHERE picking_id =  %s", (picking_id,)).fetchall()
        move_ids = tuple([move['id'] for move in moves])

        if move_ids:
            # Eliminar primero las líneas de detalle (stock_move_lines) si existen
            cursor.execute(f"DELETE FROM stock_move_lines WHERE move_id IN {move_ids if len(move_ids) > 1 else f'({move_ids[0]})'}")
            print(f"[DEBUG-DB]... stock_move_lines eliminadas para los moves: {move_ids}")

            # Eliminar los movimientos de stock (stock_moves)
            cursor.execute("DELETE FROM stock_moves WHERE picking_id =  %s", (picking_id,))
            print(f"[DEBUG-DB]... stock_moves eliminados.")

        # Finalmente, eliminar el albarán principal (pickings)
        cursor.execute("DELETE FROM pickings WHERE id =  %s", (picking_id,))
        print(f"[DEBUG-DB]... albarán principal eliminado.")
        
        conn.commit()
    return True, "Albarán eliminado permanentemente."

def update_work_order_fields(wo_id, fields_to_update: dict):
    """
    Actualiza campos específicos de una Orden de Trabajo usando un diccionario.
    Más flexible que update_work_order_details.
    """
    allowed_fields = [
        "customer_name", "address", "warehouse_id", "date_attended",
        "service_type", "job_type", "phase"
    ]
    update_dict = {k: v for k, v in fields_to_update.items() if k in allowed_fields and v is not None}

    print(f"[ESPÍA DB UPDATE DICT] Diccionario a actualizar para OT {wo_id}: {update_dict}")
    if not update_dict:
        print(f"[DB-WARN] No se proporcionaron campos válidos para actualizar la OT {wo_id}.")
        return 0

    set_clause = ", ".join([f"{key} =  %s" for key in update_dict.keys()])
    params = list(update_dict.values()) + [wo_id]

    print(f"[ESPÍA DB SAVE] SQL: UPDATE work_orders SET {set_clause} WHERE id =  %s")
    print(f"[ESPÍA DB SAVE] Params: {tuple(params)}")
    
    with connect_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE work_orders SET {set_clause} WHERE id =  %s", tuple(params))
        conn.commit()
        return cursor.rowcount # Devuelve el número de filas afectadas

def get_draft_liquidation(wo_id):
    """
    Busca si ya existe una liquidación (picking) en estado borrador para una OT.
    Devuelve la fila del picking si la encuentra, si no, devuelve None.
    """
    query = """
        SELECT p.* FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.work_order_id =  %s AND p.state = 'draft' AND pt.code = 'OUT'
    """
    return execute_query(query, (wo_id,), fetchone=True)

def get_products_with_stock_at_location(location_id):
    """
    Devuelve una lista de productos que tienen una cantidad positiva de stock
    en una ubicación específica.
    """
    if not location_id:
        return []
    
    query = """
        SELECT DISTINCT
            p.id,
            p.name,
            p.sku
        FROM products p
        JOIN stock_quants sq ON p.id = sq.product_id
        WHERE sq.location_id =  %s AND sq.quantity > 0
        ORDER BY p.name
    """
    return execute_query(query, (location_id,), fetchall=True)

def get_finalized_liquidation(wo_id):
    """
    Busca la liquidación (picking) en estado 'hecho' para una OT.
    """
    query = """
        SELECT p.* FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.work_order_id =  %s AND p.state = 'done' AND pt.code = 'OUT'
    """
    return execute_query(query, (wo_id,), fetchone=True)

def get_stock_summary_by_product(warehouse_id=None):
    """
    Devuelve el stock total por producto, agrupando todas las series/lotes.
    """
    base_query = """
    SELECT
        p.sku, p.name as product_name, pc.name as category_name,
        w.name as warehouse_name,
        SUM(sq.quantity) as quantity,
        u.name as uom_name,
        -- Añadir IDs/columnas para agrupar
        w.id, p.id, pc.id, u.id
    FROM stock_quants sq
    JOIN products p ON sq.product_id = p.id
    JOIN locations l ON sq.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    WHERE sq.quantity > 0
    """
    params = []
    if warehouse_id:
        base_query += " AND w.id = %s"
        params.append(warehouse_id)

    # --- ¡ESTA ES LA CORRECCIÓN! ---
    base_query += " GROUP BY w.id, w.name, p.id, p.sku, p.name, pc.id, pc.name, u.id, u.name"
    # --- FIN DE LA CORRECCIÓN ---
    
    base_query += " ORDER BY w.name, p.name"
    return execute_query(base_query, tuple(params), fetchall=True)

def get_dashboard_kpis(company_id):
    query = """
        SELECT
            pt.code,
            COUNT(p.id) as pending_count
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.company_id =  %s AND p.state IN ('draft', 'listo')
        GROUP BY pt.code
    """
    results = execute_query(query, (company_id,), fetchall=True)
    
    kpis = {'IN': 0, 'OUT': 0, 'INT': 0}
    for row in results:
        if row['code'] in kpis:
            kpis[row['code']] = row['pending_count']
    return kpis

def get_operations_throughput(company_id):
    """
    Devuelve el número de operaciones completadas por día durante los últimos 7 días.
    VERSIÓN POSTGRESQL: Usa CURRENT_DATE e INTERVAL.
    """
    # 1. Python calcula las fechas de inicio y fin
    today = date.today()
    start_date = today - timedelta(days=6)
    days_data = {(start_date + timedelta(days=i)): 0 for i in range(7)}

    # 2. La consulta SQL ahora es más simple y solo pide un rango
    query = """
        SELECT
            date_done::date as day,
            COUNT(id) as count
        FROM pickings
        WHERE
            state = 'done' AND
            company_id = %s AND
            date_done >= (CURRENT_DATE - INTERVAL '6 days')
        GROUP BY day
    """
    results = execute_query(query, (company_id,), fetchall=True)

    for row in results:
        day = row['day'] # psycopg2 ya lo devuelve como objeto date
        if day in days_data:
            days_data[day] = row['count']
            
    return sorted(days_data.items())

def get_inventory_aging(company_id, tracked_only=True):
    """
    Calcula el antiguamiento del inventario RASTREANDO CADA LOTE individualmente.
    (Versión PostgreSQL)
    """
    buckets = {'0-30 días': 0, '31-60 días': 0, '61-90 días': 0, '+90 días': 0, 'Sin Fecha': 0}
    
    tracking_filter_sql = "AND p.tracking != 'none'" if tracked_only else ""

    # (La consulta CTE es similar, pero la lógica CASE usa resta de fechas)
    query = f"""
        WITH LotCreationDate AS (
            SELECT
                sml.lot_id,
                MIN(COALESCE(p.date_transfer, p.date_done::date)) as effective_date
            FROM pickings p
            JOIN stock_moves sm ON p.id = sm.picking_id
            JOIN stock_move_lines sml ON sm.id = sml.move_id
            JOIN picking_types pt ON p.picking_type_id = pt.id
            WHERE p.state = 'done' AND pt.code = 'IN' AND p.company_id = %s
            GROUP BY sml.lot_id
        )
        SELECT
            CASE
                WHEN lcd.effective_date IS NULL THEN 'Sin Fecha'
                WHEN (CURRENT_DATE - lcd.effective_date) <= 30 THEN '0-30 días'
                WHEN (CURRENT_DATE - lcd.effective_date) <= 60 THEN '31-60 días'
                WHEN (CURRENT_DATE - lcd.effective_date) <= 90 THEN '61-90 días'
                ELSE '+90 días'
            END as age_bucket,
            SUM(sq.quantity) as total_quantity
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        LEFT JOIN LotCreationDate lcd ON sq.lot_id = lcd.lot_id
        WHERE l.type = 'internal' AND l.company_id = %s {tracking_filter_sql} AND sq.lot_id IS NOT NULL
        GROUP BY age_bucket
    """
    params = (company_id, company_id)
    results = execute_query(query, params, fetchall=True)

    for row in results:
        if row['age_bucket'] and row['age_bucket'] in buckets:
            buckets[row['age_bucket']] = row['total_quantity']
    final_buckets = {k: v for k, v in buckets.items() if v > 0}
    return final_buckets

def get_inventory_aging_details(company_id, filters={}):
    # (La consulta CTE es similar, la resta de fechas es la clave)
    base_query = """
        WITH LotCreationInfo AS (
            SELECT
                sml.lot_id,
                MIN(COALESCE(p.date_transfer, p.date_done::date)) as effective_date,
                AVG(sm.price_unit) as unit_cost
            FROM pickings p
            JOIN stock_moves sm ON p.id = sm.picking_id
            JOIN stock_move_lines sml ON sm.id = sml.move_id
            JOIN picking_types pt ON p.picking_type_id = pt.id
            WHERE p.state = 'done' AND pt.code = 'IN' AND p.company_id = %s
            GROUP BY sml.lot_id
        )
        SELECT
            p.sku, p.name as product_name, sl.name as lot_name,
            w.id as warehouse_id, w.name as warehouse_name,
            lci.effective_date as entry_date,
            (CURRENT_DATE - lci.effective_date) as aging_days,
            sq.quantity,
            COALESCE(lci.unit_cost, p.standard_price, 0) as unit_cost,
            (sq.quantity * COALESCE(lci.unit_cost, p.standard_price, 0)) as total_value
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN stock_lots sl ON sq.lot_id = sl.id
        JOIN locations l ON sq.location_id = l.id
        JOIN warehouses w ON l.warehouse_id = w.id
        LEFT JOIN LotCreationInfo lci ON sq.lot_id = lci.lot_id
        WHERE l.type = 'internal' AND l.company_id = %s AND sq.lot_id IS NOT NULL
    """
    # (La lógica de filtros dinámicos se mantiene, pero usa %s)
    where_clauses = []
    params = [company_id, company_id] 

    if filters.get("product"):
        product_query = f"%{filters['product']}%"
        where_clauses.append("(p.sku ILIKE %s OR p.name ILIKE %s)") # Usamos ILIKE para case-insensitive
        params.extend([product_query, product_query])
        
    if filters.get("warehouse_id"):
        where_clauses.append("w.id = %s")
        params.append(filters["warehouse_id"])

    if filters.get("bucket"):
        bucket = filters["bucket"]
        # (La lógica de buckets usa 'aging_days' que ya está calculado)
        if bucket == '0-30': where_clauses.append("aging_days BETWEEN 0 AND 30")
        elif bucket == '31-60': where_clauses.append("aging_days BETWEEN 31 AND 60")
        elif bucket == '61-90': where_clauses.append("aging_days BETWEEN 61 AND 90")
        elif bucket == '+90': where_clauses.append("aging_days > 90")
    
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    base_query += " ORDER BY aging_days DESC"
    return execute_query(base_query, tuple(params), fetchall=True)

def get_product_kardex(company_id, product_id, date_from=None, date_to=None, warehouse_id=None):
    """
    Obtiene el historial de movimientos crudos para un producto, generando
    filas separadas para entradas y salidas, incluso en transferencias internas.
    (Versión Corregida v5 - Lógica de JOIN 'l' y CASE)
    """
    
    # --- 1. Parámetros y cláusulas para el WHERE ---
    where_clauses = ["p.state = 'done'", "sm.product_id =  %s", "p.company_id =  %s"]
    params = [product_id, company_id] # 2 params

    if date_from:
        where_clauses.append("date(p.date_done) >=  %s")
        params.append(date_from) # 3 params
    if date_to:
        where_clauses.append("date(p.date_done) <=  %s")
        params.append(date_to) # 4 params
    
    # --- 2. Filtro de Almacén (se aplica al JOIN 'l') ---
    if warehouse_id and warehouse_id != "all":
        where_clauses.append("l.warehouse_id =  %s")
        params.append(warehouse_id) # 5 params
    else:
        # Si queremos "Todos", solo nos importan las ubicaciones internas
        where_clauses.append("l.type = 'internal'")
        
    query = f"""
        SELECT
            p.id, p.date_done as date, p.name as operation_ref, p.custom_operation_type,
            p.purchase_order, p.adjustment_reason, pt.code as type_code,
            prod.sku as product_sku, prod.name as product_name, pc.name as category_name,

            CASE
                WHEN p.custom_operation_type = 'Liquidación por OT' THEN p.attention_date
                ELSE p.date_transfer
            END as date_transfer,
            CASE
                WHEN p.custom_operation_type = 'Liquidación por OT' THEN p.service_act_number
                ELSE p.partner_ref
            END as partner_ref,
            CASE 
                WHEN p.custom_operation_type = 'Liquidación por OT' THEN wo.ot_number
                ELSE partner.name 
            END as partner_name,
            
            w.name as affected_warehouse, -- <-- El almacén que estamos "viendo"
            
            CASE 
                WHEN sm.location_dest_id = l.id THEN sm.quantity_done
                ELSE 0 
            END as quantity_in,
            
            CASE 
                WHEN sm.location_src_id = l.id THEN sm.quantity_done
                ELSE 0 
            END as quantity_out,
            
            CASE 
                WHEN sm.location_dest_id = l.id THEN sm.quantity_done * sm.price_unit
                ELSE 0 
            END as initial_value_in,
            
            sm.price_unit,
            sm.cost_at_adjustment,
            
            w_src.name as almacen_origen,
            l_src.name as ubicacion_origen,
            w_dest.name as almacen_destino,
            l_dest.name as ubicacion_destino,
            l_src.path as location_src_path,
            l_dest.path as location_dest_path
            
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        
        -- ¡EL JOIN CLAVE RESTAURADO!
        JOIN locations l ON (sm.location_src_id = l.id OR sm.location_dest_id = l.id)
        
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN products prod ON sm.product_id = prod.id -- JOIN para product_id
        LEFT JOIN product_categories pc ON prod.category_id = pc.id
        LEFT JOIN partners partner ON p.partner_id = partner.id
        LEFT JOIN locations l_src ON sm.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON sm.location_dest_id = l_dest.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
        LEFT JOIN work_orders wo ON p.work_order_id = wo.id
        
        WHERE {" AND ".join(where_clauses)}
        AND (
            (CASE WHEN sm.location_dest_id = l.id THEN sm.quantity_done ELSE 0 END) > 0
            OR 
            (CASE WHEN sm.location_src_id = l.id THEN sm.quantity_done ELSE 0 END) > 0
        )
        -- --------------------------------------------------------------------
        
        ORDER BY p.date_done, p.id
    """

    return execute_query(query, tuple(params), fetchall=True)


def get_full_product_kardex_data(company_id, date_from, date_to, warehouse_id=None, product_filter=None):
    """
    Obtiene TODOS los movimientos de stock detallados ('done') para el EXPORT CSV,
    generando filas separadas para entradas y salidas, incluso en transferencias internas.
    (Versión Corregida v5 - Lógica de JOIN 'l' y CASE)
    """
    
    # --- 1. Parámetros y cláusulas para el WHERE ---
    where_clauses = ["p.state = 'done'", "p.company_id =  %s"]
    params = [company_id]

    if date_from:
        where_clauses.append("date(p.date_done) >=  %s")
        params.append(date_from)
    if date_to:
        where_clauses.append("date(p.date_done) <=  %s")
        params.append(date_to)
    
    # Filtro de Almacén
    if warehouse_id and warehouse_id != "all":
        where_clauses.append("l.warehouse_id =  %s")
        params.append(warehouse_id)
    else:
        where_clauses.append("l.type = 'internal'")
        
    # Filtro de Producto
    if product_filter:
        where_clauses.append("(prod.sku LIKE  %s OR prod.name LIKE  %s)")
        params.extend([f"%{product_filter}%", f"%{product_filter}%"])
        
    query = f"""
        SELECT
            sm.id as move_id,
            sm.product_id,
            prod.sku as product_sku,
            prod.name as product_name,
            pc.name as category_name,
            p.id as picking_id,
            p.date_done as date,
            p.name as operation_ref,
            p.custom_operation_type,
            p.purchase_order,
            pt.code as type_code,
            p.adjustment_reason,

            CASE
                WHEN p.custom_operation_type = 'Liquidación por OT' THEN p.attention_date
                ELSE p.date_transfer
            END as date_transfer,
            CASE
                WHEN p.custom_operation_type = 'Liquidación por OT' THEN p.service_act_number
                ELSE p.partner_ref
            END as partner_ref,
            CASE
                WHEN p.custom_operation_type = 'Liquidación por OT' THEN wo.ot_number
                ELSE par.name
            END as partner_name,

            CASE pt.code
                WHEN 'IN' THEN par.name
                ELSE COALESCE(w_src.name, l_src.path)
            END as source_name,
            CASE pt.code
                WHEN 'OUT' THEN par.name
                ELSE COALESCE(w_dest.name, l_dest.path)
            END as destination_name,

            w.name as affected_warehouse,

            -- Lógica de Entrada/Salida basada en el JOIN 'l'
            CASE WHEN sm.location_dest_id = l.id THEN sm.quantity_done ELSE 0 END as quantity_in,
            CASE WHEN sm.location_src_id = l.id THEN sm.quantity_done ELSE 0 END as quantity_out,
            CASE WHEN sm.location_dest_id = l.id THEN sm.quantity_done * sm.price_unit ELSE 0 END as initial_value_in,
            
            sm.price_unit,
            sm.cost_at_adjustment,
            
            w_src.name as almacen_origen,
            l_src.name as ubicacion_origen,
            w_dest.name as almacen_destino,
            l_dest.name as ubicacion_destino,
            l_src.path as location_src_path,
            l_dest.path as location_dest_path

        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        
        -- ¡EL JOIN CLAVE RESTAURADO!
        JOIN locations l ON (sm.location_src_id = l.id OR sm.location_dest_id = l.id)
        
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN products prod ON sm.product_id = prod.id
        LEFT JOIN product_categories pc ON prod.category_id = pc.id
        LEFT JOIN partners par ON p.partner_id = par.id
        LEFT JOIN locations l_src ON sm.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON sm.location_dest_id = l_dest.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
        LEFT JOIN work_orders wo ON p.work_order_id = wo.id

        WHERE {" AND ".join(where_clauses)}
        
        AND sm.quantity_done > 0 -- ¡CORREGIDO!

        ORDER BY prod.sku ASC, p.date_done ASC, p.id ASC
    """

    print(f"[DB DEBUG] get_full_product_kardex_data Query Params: {tuple(params)}")
    return execute_query(query, tuple(params), fetchall=True)

def get_kardex_summary(company_id, date_from, date_to, product_filter=None, warehouse_id=None):
    # (La lógica es similar, pero reemplaza date() por ::date)
    params = []
    warehouse_clause_cte = ""
    warehouse_clause_sub = ""
    wh_id = warehouse_id if warehouse_id and warehouse_id != "all" else "all"

    params.append(company_id)
    if wh_id != "all":
        warehouse_clause_cte = " AND w.id = %s"
        params.append(wh_id)

    params.append(date_from)
    if wh_id != "all":
        warehouse_clause_sub = " AND warehouse_id = %s"
        params.append(wh_id)

    params.append(date_from)
    params.append(date_to)
    if wh_id != "all":
        params.append(wh_id)
        
    params.append(company_id)
    
    product_clause = ""
    if product_filter:
        product_clause = " AND (p.sku ILIKE %s OR p.name ILIKE %s)" # Usamos ILIKE
        params.extend([f"%{product_filter}%", f"%{product_filter}%"])
     
    query = f"""
        WITH InternalStockMoves AS (
            SELECT
                sm.product_id, p.date_done, l.warehouse_id,
                CASE WHEN sm.location_dest_id = l.id THEN sm.quantity_done ELSE 0 END as quantity_in,
                CASE WHEN sm.location_src_id = l.id THEN sm.quantity_done ELSE 0 END as quantity_out,
                CASE WHEN sm.location_dest_id = l.id THEN sm.quantity_done * sm.price_unit ELSE 0 END as value_in,
                CASE WHEN sm.location_src_id = l.id THEN sm.quantity_done * sm.price_unit ELSE 0 END as value_out
            FROM stock_moves sm
            JOIN pickings p ON sm.picking_id = p.id
            JOIN locations l ON (sm.location_src_id = l.id OR sm.location_dest_id = l.id)
            JOIN warehouses w ON l.warehouse_id = w.id
            WHERE p.state = 'done' AND l.type = 'internal' AND p.company_id = %s {warehouse_clause_cte}
        ),
        InitialBalance AS (
            SELECT product_id,
                   SUM(quantity_in) - SUM(quantity_out) as balance,
                   SUM(value_in) - SUM(value_out) as value_balance
            FROM InternalStockMoves WHERE date_done::date < %s {warehouse_clause_sub} GROUP BY product_id
        ),
        PeriodMovements AS (
            SELECT product_id,
                   SUM(quantity_in) as total_in, SUM(quantity_out) as total_out,
                   SUM(value_in) as total_value_in, SUM(value_out) as total_value_out
            FROM InternalStockMoves WHERE date_done::date BETWEEN %s AND %s {warehouse_clause_sub} GROUP BY product_id
        )
        SELECT
            p.id as product_id, p.sku, p.name as product_name,
            pc.name as category_name,
            COALESCE(ib.balance, 0) as initial_balance,
            COALESCE(pm.total_in, 0) as total_in,
            COALESCE(pm.total_out, 0) as total_out,
            (COALESCE(ib.balance, 0) + COALESCE(pm.total_in, 0) - COALESCE(pm.total_out, 0)) as final_balance,
            COALESCE(ib.value_balance, 0) as initial_value,
            COALESCE(pm.total_value_in, 0) as total_value_in,
            COALESCE(pm.total_value_out, 0) as total_value_out,
            (COALESCE(ib.value_balance, 0) + COALESCE(pm.total_value_in, 0) - COALESCE(pm.total_value_out, 0)) as final_value
        FROM products p
        LEFT JOIN PeriodMovements pm ON p.id = pm.product_id
        LEFT JOIN InitialBalance ib ON p.id = ib.product_id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        WHERE p.company_id = %s AND (pm.product_id IS NOT NULL OR ib.product_id IS NOT NULL)
        {product_clause}
        ORDER BY p.name
    """
    return execute_query(query, tuple(params), fetchall=True)

def get_stock_coverage_report(company_id, history_days=90, product_filter=None):
    safe_history_days = max(1, history_days)
    params = [company_id, company_id, company_id]
    
    product_clause = ""
    if product_filter:
        product_clause = "AND (p.sku ILIKE %s OR p.name ILIKE %s)"
        params.extend([f"%{product_filter}%", f"%{product_filter}%"])

    # (La lógica es similar, pero reemplaza date() por ::date y usa INTERVAL)
    query = f"""
        WITH
        CurrentStock AS (
            SELECT sq.product_id, SUM(sq.quantity) as on_hand_qty
            FROM stock_quants sq
            JOIN locations l ON sq.location_id = l.id
            WHERE l.type = 'internal' AND sq.lot_id IS NULL AND l.company_id = %s
            GROUP BY sq.product_id
        ),
        ConsumptionData AS (
            SELECT sm.product_id, SUM(sm.quantity_done) as total_consumed
            FROM stock_moves sm
            JOIN pickings p ON sm.picking_id = p.id
            JOIN picking_types pt ON p.picking_type_id = pt.id
            WHERE p.state = 'done'
              AND pt.code = 'OUT'
              AND p.company_id = %s
              AND p.date_done::date >= (CURRENT_DATE - INTERVAL '{safe_history_days} days')
            GROUP BY sm.product_id
        )
        SELECT
            p.sku, p.name as product_name,
            COALESCE(cs.on_hand_qty, 0) AS current_stock,
            COALESCE(cd.total_consumed, 0) AS total_consumption,
            (COALESCE(cd.total_consumed, 0) * 1.0 / {safe_history_days}) AS avg_daily_consumption,
            CASE
                WHEN (COALESCE(cd.total_consumed, 0) * 1.0 / {safe_history_days}) > 0
                THEN COALESCE(cs.on_hand_qty, 0) / (COALESCE(cd.total_consumed, 0) * 1.0 / {safe_history_days})
                ELSE 999
            END AS coverage_days
        FROM products p
        LEFT JOIN CurrentStock cs ON p.id = cs.product_id
        LEFT JOIN ConsumptionData cd ON p.id = cd.product_id
        WHERE p.company_id = %s AND p.tracking = 'none' AND COALESCE(cs.on_hand_qty, 0) > 0
        {product_clause}
        ORDER BY coverage_days ASC, p.name ASC;
    """
    return execute_query(query, tuple(params), fetchall=True)


def get_inventory_value_kpis(company_id):
    """
    Calcula el valor total del inventario usando un método estándar y robusto.
    """
    query = """
        WITH
        LastPurchasePrice AS (
            SELECT
                product_id,
                price_unit as last_price
            FROM (
                SELECT
                    sm.product_id,
                    sm.price_unit,
                    ROW_NUMBER() OVER(PARTITION BY sm.product_id ORDER BY p.date_done DESC, p.id DESC) as rn
                FROM stock_moves sm
                JOIN pickings p ON sm.picking_id = p.id
                JOIN picking_types pt ON p.picking_type_id = pt.id
                WHERE pt.code = 'IN' AND sm.price_unit > 0 AND p.company_id =  %s
            )
            WHERE rn = 1
        )
        SELECT
            wc.name as category_name,
            SUM(sq.quantity * COALESCE(lpp.last_price, p.standard_price, 0)) as total_value
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN warehouse_categories wc ON w.category_id = wc.id
        LEFT JOIN LastPurchasePrice lpp ON sq.product_id = lpp.product_id
        WHERE l.type = 'internal' AND p.company_id =  %s AND wc.name IN ('ALMACEN PRINCIPAL', 'CONTRATISTA')
        GROUP BY wc.name
    """
    params = (company_id, company_id)
    results = execute_query(query, params, fetchall=True)

    kpis = {"total": 0.0, "pri": 0.0, "tec": 0.0}
    if results:
        for row in results:
            category = row['category_name']
            value = row['total_value'] or 0.0
            if category == 'ALMACEN PRINCIPAL':
                kpis['pri'] = value
            elif category == 'CONTRATISTA':
                kpis['tec'] = value
    
    kpis['total'] = kpis['pri'] + kpis['tec']
    return kpis

# EN database.py (AÑADE ESTA NUEVA FUNCIÓN)

def get_stock_for_multiple_products(location_id, product_ids: list):
    """
    Obtiene el stock para una lista de productos en una ubicación específica
    en UNA SOLA CONSULTA para un rendimiento óptimo.
    Devuelve un diccionario: {product_id: stock_quantity}.
    """
    if not location_id or not product_ids:
        return {}
    
    # Creamos los placeholders ( %s,  %s,  %s) para la consulta SQL
    placeholders = ', '.join(' %s' for _ in product_ids)
    query = f"""
        SELECT
            product_id,
            SUM(quantity) as on_hand_stock
        FROM stock_quants
        WHERE location_id =  %s AND product_id IN ({placeholders})
        GROUP BY product_id
    """
    
    params = [location_id] + product_ids
    results = execute_query(query, tuple(params), fetchall=True)
    
    # Convertimos la lista de resultados en un diccionario para fácil acceso
    stock_map = {row['product_id']: row['on_hand_stock'] for row in results}
    return stock_map

# --- AÑADE ESTA NUEVA FUNCIÓN a database.py ---
def create_or_update_draft_picking(wo_id, company_id, user_name, warehouse_id, date_attended, service_act_number, lines_data: list):
    """
    Busca un picking 'draft' para la WO. Si no existe, lo crea.
    Luego, actualiza la cabecera del picking (almacén, fecha, acta) y
    reemplaza COMPLETAMENTE sus líneas (stock_moves y stock_move_lines).
    Se usa para guardar el progreso.
    """
    print(f"[DB-DEBUG] Iniciando create/update BORRADOR para WO ID: {wo_id}")
    if warehouse_id is None:
        return False, "Se requiere seleccionar una Contrata/Almacén para guardar."

    try:
        with connect_db() as conn:
            cursor = conn.cursor()

            # --- A. Buscar Picking Borrador Existente ---
            draft_picking = cursor.execute(
                """SELECT p.id, pt.default_location_src_id, pt.default_location_dest_id
                   FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
                   WHERE p.work_order_id =  %s AND p.state = 'draft' AND pt.code = 'OUT'""",
                (wo_id,)
            ).fetchone()

            picking_id = None
            loc_src_id = None
            loc_dest_id = None

            if draft_picking:
                picking_id = draft_picking['id']
                print(f" -> Picking borrador encontrado (ID: {picking_id}). Actualizando...")

                # Actualizar cabecera del picking existente (INCLUYENDO warehouse_id)
                # Necesitamos recalcular loc_src_id si el warehouse cambió
                location_src_row = cursor.execute(
                    "SELECT id FROM locations WHERE warehouse_id =  %s AND type = 'internal' AND name = 'Stock'",
                    (warehouse_id,)
                ).fetchone()
                if not location_src_row:
                    raise ValueError(f"No se encontró ubicación interna para el nuevo almacén ID {warehouse_id}.")
                loc_src_id = location_src_row['id']
                # loc_dest_id no debería cambiar para OUT, pero lo obtenemos por si acaso
                loc_dest_id = draft_picking['default_location_dest_id']

                cursor.execute(
                    """UPDATE pickings
                       SET warehouse_id =  %s, location_src_id =  %s, attention_date =  %s, service_act_number =  %s, responsible_user =  %s
                       WHERE id =  %s""",
                    (warehouse_id, loc_src_id, date_attended, service_act_number, user_name, picking_id)
                )
                print(f" -> Cabecera del picking {picking_id} actualizada (Wh={warehouse_id}, LocSrc={loc_src_id}).")

            else:
                # --- B. Crear Picking Borrador Nuevo ---
                print(" -> No se encontró picking borrador. Creando uno nuevo...")
                # Necesitamos company_id (viene como argumento)
                # Necesitamos warehouse_id (viene como argumento)

                # Buscar tipo OUT para el almacén seleccionado
                picking_type = cursor.execute(
                    "SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id =  %s AND code = 'OUT'",
                    (warehouse_id,)
                ).fetchone()
                if not picking_type:
                    raise ValueError(f"No se encontró tipo 'OUT' para el almacén ID {warehouse_id}")

                loc_src_id = picking_type['default_location_src_id'] # Ubicación del almacén seleccionado
                loc_dest_id = picking_type['default_location_dest_id'] # Destino virtual

                picking_name = get_next_picking_name(picking_type['id'])

                cursor.execute(
                    """INSERT INTO pickings (company_id, name, picking_type_id, warehouse_id, location_src_id, location_dest_id,
                                           state, work_order_id, custom_operation_type,
                                           service_act_number, attention_date, responsible_user)
                       VALUES ( %s,  %s,  %s,  %s,  %s,  %s, 'draft',  %s,  %s,  %s,  %s,  %s)""",
                    (company_id, picking_name, picking_type['id'], warehouse_id, loc_src_id, loc_dest_id,
                     wo_id, "Liquidación por OT",
                     service_act_number, date_attended, user_name)
                )
                picking_id = cursor.lastrowid
                print(f" -> Nuevo picking borrador creado (ID: {picking_id}, Nombre: {picking_name}, Wh={warehouse_id}, LocSrc={loc_src_id}).")

                # --- OPCIONAL: Actualizar Fase de WO a 'En Liquidación' ---
                # Considera si quieres hacerlo aquí o en la vista después de guardar
                # cursor.execute("UPDATE work_orders SET phase = 'En Liquidación' WHERE id =  %s AND phase = 'Sin Liquidar'", (wo_id,))
                # print(" -> Fase de WO actualizada a 'En Liquidación' (si estaba 'Sin Liquidar').")
                # --- FIN OPCIONAL ---

            # --- C. Borrar Movimientos Borrador Anteriores (Igual que antes) ---
            old_moves = cursor.execute("SELECT id FROM stock_moves WHERE picking_id =  %s AND state = 'draft'", (picking_id,)).fetchall()
            if old_moves:
                old_move_ids = tuple([m['id'] for m in old_moves])
                cursor.execute(f"DELETE FROM stock_move_lines WHERE move_id IN ({','.join([' %s']*len(old_move_ids))})", old_move_ids)
                cursor.execute("DELETE FROM stock_moves WHERE id IN ({})".format(','.join([' %s']*len(old_move_ids))), old_move_ids)
                print(f" -> {len(old_move_ids)} movimiento(s) borrador anteriores eliminados.")

            # --- D. Crear Nuevos Movimientos Borrador (Igual que antes, PERO usa loc_src/dest recalculados) ---
            customer_partner_id = cursor.execute("SELECT id FROM partners WHERE name = 'Cliente Varios' AND company_id =  %s", (company_id,)).fetchone()
            partner_id_to_set = customer_partner_id['id'] if customer_partner_id else None

            moves_created_count = 0
            for line in lines_data:
                product_id = line['product_id']
                quantity = line['quantity']
                tracking_data = line.get('tracking_data', {})

                cursor.execute(
                    """INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done,
                                               location_src_id, location_dest_id, state, partner_id)
                       VALUES ( %s,  %s,  %s,  %s,  %s,  %s, 'draft',  %s)""",
                    (picking_id, product_id, quantity, quantity,
                     loc_src_id, loc_dest_id, partner_id_to_set) # Usa loc_src/dest correctos
                )
                move_id = cursor.lastrowid
                moves_created_count += 1

                if tracking_data:
                    # ... (código para guardar stock_move_lines - sin cambios) ...
                     for lot_name, qty_done in tracking_data.items():
                         lot_row = get_lot_by_name(cursor, product_id, lot_name)
                         lot_id = lot_row['id'] if lot_row else create_lot(cursor, product_id, lot_name)
                         cursor.execute(
                             "INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES ( %s,  %s,  %s)",
                             (move_id, lot_id, qty_done)
                         )

            print(f" -> {moves_created_count} nuevos movimientos borrador creados.")
            conn.commit()
            return True, "Progreso de liquidación guardado."

    except Exception as e:
        print(f"[ERROR] en create_or_update_draft_picking: {e}")
        traceback.print_exc()
        return False, f"Error al guardar borrador: {e}"

def get_warehouse_category_id_by_name(name):
    """Busca el ID de una categoría de almacén por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM warehouse_categories WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def upsert_warehouse_from_import(company_id, code, name, status, social_reason, ruc, email, phone, address, category_id):
    # (Similar a productos, usamos ON CONFLICT)
    query = """
        INSERT INTO warehouses (company_id, code, name, status, social_reason, ruc, email, phone, address, category_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (code) DO UPDATE SET
            name = EXCLUDED.name,
            status = EXCLUDED.status,
            social_reason = EXCLUDED.social_reason,
            ruc = EXCLUDED.ruc,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            address = EXCLUDED.address,
            category_id = EXCLUDED.category_id
        RETURNING (xmax = 0) AS inserted;
    """
    params = (company_id, code, name, status, social_reason, ruc, email, phone, address, category_id)
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                was_inserted = cursor.fetchone()[0]
                # ¡IMPORTANTE! Si fue insertado, debemos crear sus datos asociados
                if was_inserted:
                    cursor.execute("SELECT id FROM warehouses WHERE code = %s AND company_id = %s", (code, company_id))
                    new_wh_id = cursor.fetchone()[0]
                    create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=True, warehouse_id=new_wh_id)
                conn.commit()
                return "created" if was_inserted else "updated"
    except Exception as e:
        print(f"Error procesando fila para CÓDIGO {code}: {e}")
        traceback.print_exc()
        return "error"

def get_data_for_export(company_id, export_type='headers'):
    """
    Obtiene los datos necesarios para exportar albaranes con las 4 columnas granulares.
    (Versión PostgreSQL Corregida)
    """
    # --- Consulta de Cabeceras (MODIFICADA para las 4 columnas) ---
    base_query = """
        SELECT
            p.id as picking_id, p.name as picking_name, pt.code as picking_type_code, p.state,
            p.custom_operation_type,
            CASE
                WHEN l_src.type = 'internal' THEN w_src.name
                ELSE NULL
            END as almacen_origen,
            CASE
                WHEN pt.code = 'IN' THEN COALESCE(par_prov.name, 'N/A')
                ELSE COALESCE(l_src.name, 'N/A')
            END as ubicacion_origen,
            CASE
                WHEN l_dest.type = 'internal' THEN w_dest.name
                ELSE NULL
            END as almacen_destino,
             CASE
                WHEN pt.code = 'OUT' THEN COALESCE(par_cli.name, 'N/A')
                ELSE COALESCE(l_dest.name, 'N/A')
            END as ubicacion_destino,
            
            -- --- ¡CORRECCIÓN! Usar TO_CHAR en lugar de strftime ---
            p.partner_ref, p.purchase_order, TO_CHAR(p.date_transfer, 'DD/MM/YYYY') as date_transfer, p.responsible_user
            -- --- FIN CORRECCIÓN ---
        
        FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN locations l_src ON p.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
        LEFT JOIN partners par_prov ON p.partner_id = par_prov.id AND pt.code = 'IN'
        LEFT JOIN partners par_cli ON p.partner_id = par_cli.id AND pt.code = 'OUT'
        WHERE p.company_id = %s AND pt.code != 'ADJ' ORDER BY p.id
    """
    # (El resto de la función es idéntico y correcto)
    pickings_data_raw = execute_query(base_query, (company_id,), fetchall=True)
    pickings_map = {row['picking_id']: dict(row) for row in pickings_data_raw}

    if export_type == 'headers' or not pickings_map:
        return list(pickings_map.values())

    picking_ids = tuple(pickings_map.keys())
    if not picking_ids: # Añadir comprobación por si picking_ids está vacío
        return [] 
        
    placeholders = ', '.join('%s' for _ in picking_ids)
    moves_query = f"""
        SELECT
            sm.id as move_id, sm.picking_id, prod.sku as product_sku, prod.name as product_name,
            sm.quantity_done, prod.tracking, sm.price_unit,
            sml.id as move_line_id, sl.name as serial, sml.qty_done as serial_qty
        FROM stock_moves sm JOIN products prod ON sm.product_id = prod.id
        LEFT JOIN stock_move_lines sml ON sm.id = sml.move_id
        LEFT JOIN stock_lots sl ON sml.lot_id = sl.id
        WHERE sm.picking_id IN ({placeholders})
        ORDER BY sm.picking_id, prod.sku, sl.name
    """
    moves_data_raw = execute_query(moves_query, picking_ids, fetchall=True)

    # (El resto de la lógica de combinación de Python no cambia)
    full_export_data = []
    moves_grouped = defaultdict(list)
    if moves_data_raw:
        for move_line_row in moves_data_raw:
            moves_grouped[move_line_row['move_id']].append(dict(move_line_row))
    # ... (etc.) ...
    for move_id, lines in moves_grouped.items():
        base_line = lines[0]; picking_id = base_line['picking_id']
        if picking_id not in pickings_map: continue
        product_tracking = base_line['tracking']
        has_tracking_lines = any(line['move_line_id'] is not None for line in lines)
        if product_tracking != 'none' and has_tracking_lines:
            processed_serials_for_move = set()
            for line in lines:
                if line['move_line_id'] is not None:
                    serial_or_lot_name = line['serial']
                    if serial_or_lot_name not in processed_serials_for_move:
                        combined_row = pickings_map[picking_id].copy()
                        combined_row.update({
                            'product_sku': base_line['product_sku'], 'product_name': base_line['product_name'],
                            'price_unit': base_line['price_unit'], 'serial': serial_or_lot_name,
                            'quantity': 1.0 if product_tracking == 'serial' else (line['serial_qty'] or 0.0)
                    })
                        full_export_data.append(combined_row)
                        processed_serials_for_move.add(serial_or_lot_name)
        else:
            combined_row = pickings_map[picking_id].copy()
            combined_row.update({
                'product_sku': base_line['product_sku'], 'product_name': base_line['product_name'],
                'price_unit': base_line['price_unit'], 'serial': '',
                'quantity': base_line['quantity_done']
            })
            full_export_data.append(combined_row)
    pickings_with_moves = {row['picking_id'] for row in full_export_data}
    for pid, p_data in pickings_map.items():
        if pid not in pickings_with_moves:
             p_data_dict = dict(p_data)
             p_data_dict.update({ 'product_sku': '', 'product_name': '', 'quantity': '', 'price_unit': '', 'serial': '' })
             full_export_data.append(p_data_dict)
    full_export_data.sort(key=lambda x: (x.get('picking_name', ''), x.get('product_sku', ''), x.get('serial', '')))

    return full_export_data

def return_picking_to_draft(picking_id):
    """(MIGRADO) Regresa a 'draft' usando el POOL."""
    global db_pool
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            cursor.execute("SELECT state FROM pickings WHERE id = %s", (picking_id,))
            current_state_row = cursor.fetchone()

            if not current_state_row:
                return False, "El albarán no existe."
            if current_state_row['state'] != 'listo':
                return False, f"Solo se puede regresar desde el estado 'Listo' (estado actual: {current_state_row['state']})."

            cursor.execute("UPDATE pickings SET state = 'draft' WHERE id = %s AND state = 'listo'", (picking_id,))
            rows_affected = cursor.rowcount
        
        conn.commit() # Commit de la transacción

        if rows_affected > 0:
            return True, "Albarán regresado a estado 'Borrador'."
        else:
            return False, "No se pudo actualizar el estado (posible concurrencia o error)."

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error en return_picking_to_draft: {e}")
        traceback.print_exc()
        return False, f"Error inesperado en base de datos: {e}"
    finally:
        if conn: db_pool.putconn(conn)

def save_move_lines_for_move(move_id, tracking_data: dict):
    """
    (MIGRADO) Reemplaza las move_lines para un move usando el pool de conexiones.
    Esta función es transaccional.
    """
    print(f"[DB] Guardando/Actualizando move_lines para move_id: {move_id}. Datos: {tracking_data}")
    global db_pool # Accedemos al pool global
    conn = None
    try:
        conn = db_pool.getconn() # Tomar una conexión del pool
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor: # Usar DictCursor aquí

            # 0. Obtener product_id del move
            cursor.execute("SELECT product_id FROM stock_moves WHERE id = %s", (move_id,))
            move_info = cursor.fetchone()
            
            if not move_info:
                print(f"[DB-ERROR] No se encontró el stock_move con ID: {move_id}")
                return False, f"Movimiento ID {move_id} no encontrado."
            
            product_id = move_info['product_id'] # move_info es ahora un DictRow

            # 1. Borrar líneas existentes para este movimiento
            cursor.execute("DELETE FROM stock_move_lines WHERE move_id = %s", (move_id,))
            print(f" -> Líneas antiguas para move_id {move_id} eliminadas.")

            # 2. Insertar las nuevas líneas
            inserted_count = 0
            for serial_name, qty in tracking_data.items():
                if qty <= 0: continue

                # Buscar o crear el stock_lot (pasando el cursor)
                lot = get_lot_by_name(cursor, product_id, serial_name)
                
                lot_id = lot['id'] if lot else create_lot(cursor, product_id, serial_name) 

                # Insertar la stock_move_line
                cursor.execute(
                    "INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)",
                    (move_id, lot_id, qty)
                )
                inserted_count += 1

            conn.commit() # Hacer commit de toda la transacción
            print(f" -> {inserted_count} nuevas líneas insertadas para move_id {move_id}.")
            return True, f"{inserted_count} series/lotes guardados."

    except Exception as e:
        if conn:
            conn.rollback() # Revertir en caso de error
        print(f"Error en save_move_lines_for_move: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al guardar series: {e}"
    finally:
        if conn:
            db_pool.putconn(conn) # Devolver la conexión al pool

def get_operation_type_details_by_name(name):
    """
    Busca los detalles de una regla de operación por su nombre.
    CORREGIDO: Usa TRIM para ignorar espacios en blanco al inicio/final.
    """
    return execute_query("SELECT * FROM operation_types WHERE TRIM(name) = TRIM(%s)", (name,), fetchone=True)

def get_partner_id_by_name(name, company_id):
    """Busca el ID y el NOMBRE DE CATEGORÍA de un partner por su nombre exacto."""
    if not name: return None
    # --- MODIFICADO: Añadir pc.name ---
    query = """
        SELECT p.id, pc.name as category_name
        FROM partners p
        LEFT JOIN partner_categories pc ON p.category_id = pc.id
        WHERE p.name =  %s AND p.company_id =  %s
    """
    result = execute_query(query, (name, company_id), fetchone=True)
    # Devuelve el resultado completo (Row object) o None
    return result

def get_location_id_by_warehouse_name(name, company_id):
    """
    Busca el ID de la ubicación de stock ('Nombre/Stock') y el NOMBRE DE CATEGORÍA
    del almacén por el nombre exacto del almacén.
    """
    if not name: return None
    # --- MODIFICADO: Añadir wc.name ---
    query = """
        SELECT l.id, wc.name as category_name
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE w.name =  %s AND w.company_id =  %s AND l.type = 'internal' AND l.name = 'Stock'
    """
    result = execute_query(query, (name, company_id), fetchone=True)
    # Devuelve el resultado completo (Row object) o None
    return result

def find_picking_type_id(company_id, type_code, warehouse_id=None):
    """
    Intenta encontrar un ID de picking_type que coincida con el código (IN/OUT/INT)
    y opcionalmente con un almacén específico. Devuelve el primero que encuentra.
    """
    params = [company_id, type_code]
    query = "SELECT id FROM picking_types WHERE company_id =  %s AND code =  %s"
    if warehouse_id:
        query += " AND warehouse_id =  %s"
        params.append(warehouse_id)
    query += " LIMIT 1" # Tomamos el primero que coincida

    result = execute_query(query, tuple(params), fetchone=True)
    return result['id'] if result else None

def get_product_details_by_sku(sku, company_id):
    """Busca los detalles completos de un producto por su SKU exacto para una compañía."""
    if not sku: return None
    # Incluimos id, name, tracking, y uom_name (útil para la fila)
    query = """
        SELECT p.id, p.name, p.tracking, u.name as uom_name, p.standard_price
        FROM products p
        LEFT JOIN uom u ON p.uom_id = u.id
        WHERE p.sku =  %s AND p.company_id =  %s
    """
    return execute_query(query, (sku, company_id), fetchone=True)

def get_locations_detailed(company_id):
    """
    Obtiene lista detallada de ubicaciones, incluyendo nombre y ESTADO del almacén.
    """
    query = """
        SELECT
            l.id, l.name, l.path, l.type, l.category,
            l.warehouse_id as location_wh_id,
            w.id as warehouse_actual_id,
            w.name as warehouse_name,
            w.company_id as warehouse_company_id,
            w.status as warehouse_status -- <-- AÑADIR ESTA LÍNEA
        FROM locations l
        LEFT JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.company_id =  %s
        ORDER BY l.path
    """
    results = execute_query(query, (company_id,), fetchall=True)
    # ... (tu print de debug, si quieres mantenerlo) ...
    return results if results else []

def get_warehouses_simple(company_id):
    """
    Devuelve una lista simple de almacenes (ID, Nombre, Código) 
    para llenar dropdowns.
    """
    query = """
        SELECT id, name, code
        FROM warehouses
        WHERE company_id = %s AND status = 'activo'
        ORDER BY name
    """
    results = execute_query(query, (company_id,), fetchall=True)
    return results if results else []

def get_warehouses_by_categories(company_id, category_names: list):
    """
    Devuelve una lista simple de almacenes (ID y Nombre) filtrando
    por una lista de nombres de categoría.
    Útil para llenar dropdowns según el tipo de operación.
    """
    if not category_names: # Si la lista está vacía, no devolver nada
        return []

    # Creamos placeholders ( %s,  %s, ...) para la cláusula IN
    placeholders = ', '.join(' %s' for _ in category_names)
    
    query = f"""
        SELECT w.id, w.name
        FROM warehouses w
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE w.company_id =  %s 
          AND w.status = 'activo' 
          AND wc.name IN ({placeholders}) -- Filtramos por las categorías dadas
        ORDER BY w.name
    """
    # Los parámetros serán el company_id + la lista de nombres de categoría
    params = tuple([company_id] + category_names)
    
    results = execute_query(query, params, fetchall=True)
    return results if results else []

def create_location(company_id, name, path, type, category, warehouse_id):
    """Crea una nueva ubicación."""
    if type != 'internal' and warehouse_id is not None:
        warehouse_id = None
    elif type == 'internal' and warehouse_id is None:
        raise ValueError("Se requiere un Almacén Asociado para ubicaciones de tipo 'Interna'.")

    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                # Validación de Path duplicado DENTRO de la transacción
                cursor.execute(
                    "SELECT id FROM locations WHERE path = %s AND company_id = %s",
                    (path, company_id)
                )
                if cursor.fetchone():
                    raise ValueError(f"El Path '{path}' ya existe.")
                
                query = """
                    INSERT INTO locations (company_id, name, path, type, category, warehouse_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                params = (company_id, name, path, type, category, warehouse_id)
                cursor.execute(query, params)
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e:  # Captura genérica de psycopg2
        if "locations_path_key" in str(e):  # Revisa el nombre real del constraint UNIQUE en PostgreSQL
            raise ValueError(f"El Path '{path}' ya existe.")
        raise ValueError(f"No se pudo crear la ubicación. Verifique los datos.") from e

def update_location(location_id, company_id, name, path, type, category, warehouse_id):
    """
    Actualiza una ubicación existente, previniendo dejar almacenes sin ubicaciones internas.
    """
    import traceback
    print(f"[DB-UPDATE-LOC] Intentando actualizar Location ID: {location_id}")
    
    # Validación básica: warehouse_id vs type
    if type != 'internal' and warehouse_id is not None:
        warehouse_id = None
    elif type == 'internal' and warehouse_id is None:
        raise ValueError("Se requiere un Almacén Asociado para ubicaciones de tipo 'Interna'.")

    conn = None
    try:
        conn = connect_db()
        with conn.cursor() as cursor:
            # --- Obtener Datos Actuales de la Ubicación ---
            cursor.execute(
                "SELECT type, warehouse_id FROM locations WHERE id = %s AND company_id = %s",
                (location_id, company_id)
            )
            current_loc = cursor.fetchone()
            if not current_loc:
                raise ValueError(f"No se encontró la ubicación con ID {location_id}.")
            
            current_type = current_loc[0]  # type
            current_warehouse_id = current_loc[1]  # warehouse_id

            print(f" -> Datos actuales: Type='{current_type}', WH_ID={current_warehouse_id}")
            print(f" -> Datos nuevos propuestos: Type='{type}', WH_ID={warehouse_id}")

            # --- VALIDACIÓN ANTI-HUÉRFANOS ---
            is_changing_from_internal = (current_type == 'internal' and type != 'internal')
            is_changing_internal_wh = (current_type == 'internal' and type == 'internal' and current_warehouse_id != warehouse_id)

            if (is_changing_from_internal or is_changing_internal_wh) and current_warehouse_id is not None:
                print(f" -> Verificando si es la última ubicación interna del Almacén ID: {current_warehouse_id}")
                cursor.execute(
                    "SELECT COUNT(*) FROM locations WHERE warehouse_id = %s AND type = 'internal' AND id != %s",
                    (current_warehouse_id, location_id)
                )
                other_internal_count = cursor.fetchone()[0]
                print(f" -> El almacén {current_warehouse_id} tiene {other_internal_count} OTRAS ubicaciones internas.")

                if other_internal_count == 0:
                    print(" -> ¡BLOQUEADO! Es la última ubicación interna.")
                    raise ValueError(f"No se puede modificar: es la última ubicación interna del almacén original (ID: {current_warehouse_id}).")
            # --- FIN VALIDACIÓN ANTI-HUÉRFANOS ---

            # --- Validación Path único ---
            cursor.execute(
                "SELECT id FROM locations WHERE path = %s AND company_id = %s AND id != %s",
                (path, company_id, location_id)
            )
            existing = cursor.fetchone()
            if existing:
                raise ValueError(f"El Path '{path}' ya está en uso por otra ubicación.")

            # --- Ejecutar UPDATE ---
            print(" -> Procediendo a actualizar la ubicación...")
            cursor.execute(
                """UPDATE locations SET
                   name = %s, path = %s, type = %s, category = %s, warehouse_id = %s
                   WHERE id = %s AND company_id = %s""",
                (name, path, type, category, warehouse_id, location_id, company_id)
            )
            conn.commit()
            print(" -> Ubicación actualizada con éxito.")
            return True

    except ValueError as err:
        if conn:
            conn.rollback()
        print(f"[DB-ERROR] Error al actualizar ubicación: {err}")
        raise err
    except Exception as ex:
        if conn:
            conn.rollback()
        print(f"Error CRÍTICO en update_location: {ex}")
        traceback.print_exc()
        raise RuntimeError(f"Error inesperado al actualizar ubicación: {ex}")
    finally:
        if conn:
            conn.close()

def delete_location(location_id):
    """
    Elimina una ubicación si no está en uso.
    """
    print(f"[DB-DELETE-LOC] Intentando eliminar Location ID: {location_id}")
    try:
        with connect_db() as conn:
            # ¡Usamos DictCursor aquí!
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

                # --- Verificaciones de Seguridad ---
                # 1. ¿Tiene stock actual? (stock_quants)
                cursor.execute("SELECT SUM(quantity) as total_stock FROM stock_quants WHERE location_id = %s", (location_id,))
                stock_result = cursor.fetchone()
                
                # Ahora esto funcionará porque stock_result es un diccionario
                if stock_result and stock_result['total_stock'] and abs(stock_result['total_stock']) > 0.001:
                    print(f" -> Bloqueado: Tiene stock ({stock_result['total_stock']}).")
                    return False, f"No se puede eliminar: La ubicación tiene stock ({stock_result['total_stock']} unidades)."

                # 2. ¿Es ubicación por defecto en picking_types?
                cursor.execute("SELECT COUNT(*) as count FROM picking_types WHERE default_location_src_id = %s OR default_location_dest_id = %s", (location_id, location_id))
                pt_result = cursor.fetchone()
                if pt_result['count'] > 0:
                    print(f" -> Bloqueado: Es ubicación por defecto en {pt_result['count']} tipo(s) de operación.")
                    return False, f"No se puede eliminar: Es ubicación por defecto en {pt_result['count']} tipo(s) de operación."

                # 3. ¿Tiene historial de movimientos? (stock_moves)
                cursor.execute("SELECT COUNT(*) as count FROM stock_moves WHERE location_src_id = %s OR location_dest_id = %s", (location_id, location_id))
                move_result = cursor.fetchone()
                if move_result['count'] > 0:
                    print(f" -> Bloqueado: Tiene {move_result['count']} movimientos históricos.")
                    return False, f"No se puede eliminar: La ubicación tiene historial de {move_result['count']} movimientos."

                # --- Si pasa todas las verificaciones, eliminar ---
                print(" -> Verificaciones superadas. Procediendo a eliminar...")
                cursor.execute("DELETE FROM locations WHERE id = %s", (location_id,))
                rows_affected = cursor.rowcount
                conn.commit()

                if rows_affected > 0:
                    print(" -> Ubicación eliminada con éxito.")
                    return True, "Ubicación eliminada correctamente."
                else:
                    print(" -> ADVERTENCIA: No se encontró la ubicación para eliminar.")
                    return False, "La ubicación no se encontró (posiblemente ya fue eliminada)."

    except Exception as e:
        print(f"Error CRÍTICO en delete_location: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al intentar eliminar: {e}"

def get_warehouse_code(warehouse_id):
    """Obtiene el código ('code') de un almacén por su ID."""
    if not warehouse_id: return None
    result = execute_query("SELECT code FROM warehouses WHERE id =  %s", (warehouse_id,), fetchone=True)
    return result['code'] if result else None

def get_warehouses_filtered_sorted(company_id, filters={}, sort_by='name', ascending=True, limit=None, offset=None):
    """
    Obtiene almacenes filtrados y ordenados directamente desde la base de datos.
    """
    base_query = """
    SELECT
        w.id, w.company_id, w.name, w.code,  -- <--- ASEGÚRATE QUE company_id ESTÉ AQUÍ
        w.social_reason, w.ruc, w.email, w.phone, w.address,
        w.status,
        w.category_id, -- <--- ASEGÚRATE QUE category_id ESTÉ AQUÍ
        wc.name as category_name
    FROM warehouses w
    LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
    WHERE w.company_id =  %s
    """
    # ... (El resto de la función: params, where_clauses, order_by, etc. no necesita cambiar) ...
    # ...
    params = [company_id]
    where_clauses = []

    # --- Construcción dinámica de WHERE ---
    for key, value in filters.items():
        if not value: continue 

        column_map = {
            'name': "w.name", 'code': "w.code", 'social_reason': "w.social_reason",
            'ruc': "w.ruc", 'address': "w.address", 'status': "w.status",
            'category_name': "wc.name"
        }

        sql_column = column_map.get(key)
        if not sql_column: continue 

        if key == 'category_name' and value == '_NO_CATEGORY_':
             where_clauses.append("w.category_id IS NULL")
        elif key in ['name', 'code', 'social_reason', 'ruc', 'address']: 
             where_clauses.append(f"{sql_column} ILIKE %s") # PostgreSQL es ILIKE
             params.append(f"%{value}%")
        elif key in ['status', 'category_name']: 
             where_clauses.append(f"{sql_column} = %s")
             params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    sort_column_map = {
         'id': "w.id",
         'name': "w.name", 'code': "w.code", 'social_reason': "w.social_reason",
         'ruc': "w.ruc", 'address': "w.address", 'category_name': "wc.name", 'status': "w.status"
     }
    order_by_col = sort_column_map.get(sort_by, "w.id") 
    direction = "ASC" if ascending else "DESC"
    base_query += f" ORDER BY {order_by_col} {direction}"

    if limit is not None and offset is not None:
        base_query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)
    
    print(f"Executing SQL: {base_query}") 
    print(f"With params: {tuple(params)}")   
    return execute_query(base_query, tuple(params), fetchall=True)

def get_warehouses_count(company_id, filters={}):
    """
    Cuenta el número total de almacenes que coinciden con los filtros,
    sin traer los datos completos. Esencial para la paginación.
    """
    # La lógica es muy similar a la de get_warehouses_filtered_sorted
    base_query = """
    SELECT COUNT(w.id) as total_count
    FROM warehouses w
    LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
    WHERE w.company_id =  %s
    """
    params = [company_id]
    where_clauses = []
    
    # Reutilizamos la misma lógica de construcción de WHERE
    # (puedes copiar y pegar la lógica de filtros de get_warehouses_filtered_sorted aquí)
    for key, value in filters.items():
        if not value: continue
        
        column_map = {
            'name': "w.name", 'code': "w.code", 'social_reason': "w.social_reason",
            'ruc': "w.ruc", 'address': "w.address", 'status': "w.status",
            'category_name': "wc.name"
        }
        
        sql_column = column_map.get(key)
        if not sql_column: continue

        if key == 'category_name' and value == '_NO_CATEGORY_':
            where_clauses.append("w.category_id IS NULL")
        elif key in ['name', 'code', 'social_reason', 'ruc', 'address']:
            where_clauses.append(f"{sql_column} LIKE  %s")
            params.append(f"%{value}%")
        elif key in ['status', 'category_name']:
            where_clauses.append(f"{sql_column} =  %s")
            params.append(value)
            
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)
        
    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

# En app/database.py

def get_products_filtered_sorted(company_id, filters={}, sort_by='name', ascending=True, limit=None, offset=None):
    """ Obtiene productos filtrados, ordenados y paginados desde la base de datos. """
    
    # --- Consulta Principal Corregida (Selecciona todos los campos del schema) ---
    base_query = """
    SELECT 
        p.id, p.company_id, p.name, p.sku, 
        p.category_id, pc.name as category_name, 
        p.uom_id, u.name as uom_name,
        p.tracking, p.ownership, p.standard_price, p.type
    FROM products p
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    WHERE p.company_id = %s
    """
    params = [company_id]
    where_clauses = []

    column_map = {
        'name': "p.name", 'sku': "p.sku", 'category_name': "pc.name",
        'uom_name': "u.name", 'tracking': "p.tracking", 'ownership': "p.ownership"
    }

    for key, value in filters.items():
        if not value: continue
        sql_column = column_map.get(key)
        if not sql_column: continue

        if key in ['name', 'sku']:
            where_clauses.append(f"{sql_column} ILIKE %s") # Usar ILIKE para PostgreSQL
            params.append(f"%{value}%")
        else: 
            where_clauses.append(f"{sql_column} = %s")
            params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    sort_column_map = {
        'id': "p.id", 'name': "p.name", 'sku': "p.sku", 'category_name': "pc.name",
        'uom_name': "u.name", 'tracking': "p.tracking", 'ownership': "p.ownership",
        'standard_price': "p.standard_price"
    }
    order_by_col = sort_column_map.get(sort_by, "p.id")
    direction = "ASC" if ascending else "DESC"
    base_query += f" ORDER BY {order_by_col} {direction}"

    if limit is not None and offset is not None:
        base_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    return execute_query(base_query, tuple(params), fetchall=True)

def get_products_count(company_id, filters={}):
    """ Cuenta el total de productos que coinciden con los filtros. """
    base_query = """
    SELECT COUNT(p.id) as total_count
    FROM products p
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    WHERE p.company_id =  %s
    """
    params = [company_id]
    where_clauses = []

    column_map = {
        'name': "p.name", 'sku': "p.sku", 'category_name': "pc.name",
        'uom_name': "u.name", 'tracking': "p.tracking", 'ownership': "p.ownership"
    }
    
    for key, value in filters.items():
        if not value: continue
        sql_column = column_map.get(key)
        if not sql_column: continue

        if key in ['name', 'sku']:
            where_clauses.append(f"{sql_column} LIKE  %s")
            params.append(f"%{value}%")
        else:
            where_clauses.append(f"{sql_column} =  %s")
            params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

def get_partners_filtered_sorted(company_id, filters={}, sort_by='name', ascending=True, limit=None, offset=None):
    """ Obtiene proveedores/clientes filtrados, ordenados y paginados. """
    base_query = """
    SELECT p.id, p.company_id, p.name, p.social_reason, p.ruc, p.email, p.phone, p.address,
           pc.name as category_name, p.category_id
    FROM partners p
    LEFT JOIN partner_categories pc ON p.category_id = pc.id
    WHERE p.company_id =  %s
    """
    # ... (El resto de la función: params, where_clauses, order_by, etc. no necesita cambiar) ...
    # ...
    params = [company_id]
    where_clauses = []

    # Mapeo de claves de filtro a columnas SQL
    column_map = {
        'name': "p.name", 'ruc': "p.ruc", 'social_reason': "p.social_reason",
        'address': "p.address", 'category_name': "pc.name"
    }

    for key, value in filters.items():
        if not value: continue
        sql_column = column_map.get(key)
        if not sql_column: continue

        if value == "_NO_CATEGORY_":
            where_clauses.append("p.category_id IS NULL")
        elif key in ['name', 'ruc', 'social_reason', 'address']:
            where_clauses.append(f"{sql_column} ILIKE %s") # PostgreSQL es ILIKE
            params.append(f"%{value}%")
        else: # Para el dropdown de categoría
            where_clauses.append(f"{sql_column} = %s")
            params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    # Mapeo de claves de ordenamiento
    sort_column_map = {
        'id': "p.id", 'name': "p.name", 'category_name': "pc.name", 'ruc': "p.ruc",
        'social_reason': "p.social_reason", 'address': "p.address"
    }
    order_by_col = sort_column_map.get(sort_by, "p.id")
    direction = "ASC" if ascending else "DESC"
    base_query += f" ORDER BY {order_by_col} {direction}"

    if limit is not None and offset is not None:
        base_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    return execute_query(base_query, tuple(params), fetchall=True)

def get_partners_count(company_id, filters={}):
    """ Cuenta el total de proveedores/clientes que coinciden con los filtros. """
    base_query = """
    SELECT COUNT(p.id) as total_count
    FROM partners p
    LEFT JOIN partner_categories pc ON p.category_id = pc.id
    WHERE p.company_id =  %s
    """
    params = [company_id]
    where_clauses = []
    # (La lógica de filtros es idéntica a la de la función anterior)
    column_map = {
        'name': "p.name", 'ruc': "p.ruc", 'social_reason': "p.social_reason",
        'address': "p.address", 'category_name': "pc.name"
    }
    for key, value in filters.items():
        if not value: continue
        sql_column = column_map.get(key)
        if not sql_column: continue
        if value == "_NO_CATEGORY_": where_clauses.append("p.category_id IS NULL")
        elif key in ['name', 'ruc', 'social_reason', 'address']:
            where_clauses.append(f"{sql_column} LIKE  %s"); params.append(f"%{value}%")
        else:
            where_clauses.append(f"{sql_column} =  %s"); params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

def get_partner_category_id_by_name(name):
    """Busca el ID de una categoría de partner por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM partner_categories WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def upsert_partner_from_import(company_id, name, category_id, ruc, social_reason, address, email, phone):
    # (Similar, ON CONFLICT en (company_id, name))
    query = """
        INSERT INTO partners (company_id, name, category_id, ruc, social_reason, address, email, phone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, name) DO UPDATE SET
            category_id = EXCLUDED.category_id,
            ruc = EXCLUDED.ruc,
            social_reason = EXCLUDED.social_reason,
            address = EXCLUDED.address,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone
        RETURNING (xmax = 0) AS inserted;
    """
    params = (company_id, name, category_id, ruc, social_reason, address, email, phone)
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                was_inserted = cursor.fetchone()[0]
                conn.commit()
                return "created" if was_inserted else "updated"
    except Exception as e:
        print(f"Error procesando fila para Partner '{name}': {e}")
        return "error"

def get_locations_filtered_sorted(company_id, filters={}, sort_by='path', ascending=True, limit=None, offset=None):
    """ Obtiene ubicaciones filtradas, ordenadas y paginadas. """
    
    # Añadimos l.company_id y l.category, y nos aseguramos que l.warehouse_id sea 'warehouse_id'
    base_query = """
    SELECT 
        l.id, 
        l.company_id,      -- <-- AÑADIDO
        l.name, 
        l.path, 
        l.type, 
        l.category,        -- <-- AÑADIDO
        l.warehouse_id,    -- <-- CORREGIDO (sin alias 'location_wh_id')
        w.name as warehouse_name, 
        w.status as warehouse_status
    FROM locations l
    LEFT JOIN warehouses w ON l.warehouse_id = w.id
    WHERE l.company_id = %s
    """
    # --------------------------

    params = [company_id]
    where_clauses = []

    column_map = {'path': 'l.path', 'type': 'l.type', 'warehouse_name': 'w.name'}

    for key, value in filters.items():
        if not value: continue
        
        if key == 'warehouse_status':
            if value == "activos_y_virtuales":
                where_clauses.append("(w.status = 'activo' OR w.status IS NULL)")
            elif value == "inactivo":
                where_clauses.append("w.status = 'inactivo'")
            continue

        sql_column = column_map.get(key)
        if not sql_column: continue

        if key == 'path' or key == 'warehouse_name':
            where_clauses.append(f"{sql_column} ILIKE %s")
            params.append(f"%{value}%")
        else: # Para el dropdown de tipo
            where_clauses.append(f"{sql_column} = %s")
            params.append(value)
            
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    sort_column_map = {'path': 'l.path', 'type': 'l.type', 'warehouse_name': 'w.name', 'id': 'l.id'}
    order_by_col = sort_column_map.get(sort_by, "l.id")
    direction = "ASC" if ascending else "DESC"
    base_query += f" ORDER BY {order_by_col} {direction}"

    if limit is not None and offset is not None:
        base_query += " LIMIT %s OFFSET %s"; params.extend([limit, offset])

    return execute_query(base_query, tuple(params), fetchall=True)
def get_locations_count(company_id, filters={}):
    """ Cuenta el total de ubicaciones que coinciden con los filtros. """
    base_query = """
    SELECT COUNT(l.id) as total_count
    FROM locations l
    LEFT JOIN warehouses w ON l.warehouse_id = w.id
    WHERE l.company_id =  %s
    """
    params = [company_id]
    # (La lógica de filtros es idéntica a la función anterior)
    where_clauses = []
    column_map = {'path': 'l.path', 'type': 'l.type', 'warehouse_name': 'w.name'}
    for key, value in filters.items():
        if not value: continue
        if key == 'warehouse_status':
            if value == "activos_y_virtuales": where_clauses.append("(w.status = 'activo' OR w.status IS NULL)")
            elif value == "inactivo": where_clauses.append("w.status = 'inactivo'")
            continue
        sql_column = column_map.get(key)
        if not sql_column: continue
        if key == 'path' or key == 'warehouse_name':
            where_clauses.append(f"{sql_column} LIKE  %s"); params.append(f"%{value}%")
        else:
            where_clauses.append(f"{sql_column} =  %s"); params.append(value)
            
    if where_clauses: base_query += " AND " + " AND ".join(where_clauses)
    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0


def get_pickings_count(picking_type_code, company_id, filters={}):
    """
    Cuenta el número total de albaranes que coinciden con los filtros,
    sin traer los datos completos. Esencial para la paginación.
    """
    # Esta función es una copia de la lógica de get_pickings_by_type, pero solo para contar.
    # (El código de esta función es largo, pero es una adaptación directa de la que ya tenías)
    base_query = """
    SELECT COUNT(p.id) as total_count
    FROM pickings p 
    JOIN picking_types pt ON p.picking_type_id = pt.id
    LEFT JOIN locations l_src ON p.location_src_id = l_src.id
    LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
    LEFT JOIN partners partner ON p.partner_id = partner.id
    LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
    LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
    WHERE pt.code =  %s AND p.company_id =  %s AND pt.code != 'ADJ'
    """
    params = [picking_type_code, company_id]
    where_clauses = []

    # Construir WHERE dinámico para filtros
    for key, value in filters.items():
        if value:
            if key in ["date_transfer_from", "date_transfer_to"]:
                try:
                    db_date = datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
                    operator = ">=" if key == "date_transfer_from" else "<="
                    where_clauses.append(f"p.date_transfer {operator}  %s")
                    params.append(db_date)
                except ValueError: pass
            elif key == 'state':
                where_clauses.append("p.state =  %s")
                params.append(value)
            # Mapeo de otras claves de filtro a las columnas correctas
            elif key in ["partner_ref", "custom_operation_type", "name", "purchase_order", "responsible_user"]:
                where_clauses.append(f"p.{key} LIKE  %s")
                params.append(f"%{value}%")
            elif key == 'src_path':
                where_clauses.append("(CASE WHEN pt.code = 'IN' THEN partner.name ELSE COALESCE(w_src.name, l_src.path) END) LIKE  %s")
                params.append(f"%{value}%")
            elif key == 'dest_path':
                where_clauses.append("(CASE WHEN pt.code = 'OUT' THEN partner.name ELSE COALESCE(w_dest.name, l_dest.path) END) LIKE  %s")
                params.append(f"%{value}%")
                
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)
        
    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

def get_pickings_by_type(picking_type_code, company_id, filters={}, sort_by='id', ascending=False, limit=None, offset=None):
    # (La lógica de sort_map no cambia)
    sort_map = {
        'name': "p.name", 'purchase_order': "p.purchase_order",
        'src_path_display': "src_path_display", 'dest_path_display': "dest_path_display",
        'warehouse_src_name': "w_src.name", 'warehouse_dest_name': "w_dest.name",
        'date': "p.scheduled_date", 'transfer_date': "p.date_transfer",
        'state': "p.state", 'id': "p.id", 'custom_operation_type': 'p.custom_operation_type',
        'partner_ref': 'p.partner_ref', 'responsible_user': 'p.responsible_user'
    }
    order_by_column = sort_map.get(sort_by, "p.id")
    direction = "ASC" if ascending else "DESC"

    query_params = [picking_type_code, company_id]
    where_clauses = []

    # (Lógica de filtros con %s e ILIKE)
    for key, value in filters.items():
         if value:
            if key in ["date_transfer_from", "date_transfer_to"]:
                try:
                    db_date = datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
                    operator = ">=" if key == "date_transfer_from" else "<="
                    where_clauses.append(f"p.date_transfer {operator} %s")
                    query_params.append(db_date)
                except ValueError: pass
            elif key == 'state':
                where_clauses.append("p.state = %s")
                query_params.append(value)
            elif key in ["partner_ref", "custom_operation_type", "name", "purchase_order", "responsible_user"]:
                where_clauses.append(f"p.{key} ILIKE %s")
                query_params.append(f"%{value}%")
            elif key == 'src_path':
                where_clauses.append("src_path_display ILIKE %s")
                query_params.append(f"%{value}%")
            elif key == 'dest_path':
                 where_clauses.append("dest_path_display ILIKE %s")
                 query_params.append(f"%{value}%")
            elif key == 'warehouse_src_name':
                 where_clauses.append("w_src.name ILIKE %s")
                 query_params.append(f"%{value}%")
            elif key == 'warehouse_dest_name':
                 where_clauses.append("w_dest.name ILIKE %s")
                 query_params.append(f"%{value}%")

    where_string = " AND " + " AND ".join(where_clauses) if where_clauses else ""

    # (Query principal con TO_CHAR para fechas)
    query = f"""
    SELECT
        p.id, p.name, p.state, p.purchase_order, p.partner_ref, p.custom_operation_type, p.responsible_user,
        TO_CHAR(p.scheduled_date, 'DD/MM/YYYY') as date,
        TO_CHAR(p.date_transfer, 'DD/MM/YYYY') as transfer_date,
        pt.code as type_code,
        CASE WHEN pt.code = 'IN' THEN partner.name ELSE l_src.path END as src_path_display,
        CASE WHEN pt.code = 'OUT' THEN partner.name ELSE l_dest.path END as dest_path_display,
        CASE WHEN l_src.type = 'internal' THEN w_src.name ELSE NULL END as warehouse_src_name,
        CASE WHEN l_dest.type = 'internal' THEN w_dest.name ELSE NULL END as warehouse_dest_name
    FROM pickings p
    JOIN picking_types pt ON p.picking_type_id = pt.id
    LEFT JOIN locations l_src ON p.location_src_id = l_src.id
    LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
    LEFT JOIN partners partner ON p.partner_id = partner.id
    LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
    LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
    WHERE pt.code = %s AND p.company_id = %s AND pt.code != 'ADJ'
    {where_string}
    ORDER BY {order_by_column} {direction}
    """

    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        query_params.extend([limit, offset])

    return execute_query(query, tuple(query_params), fetchall=True)

def get_location_name_details(location_id):
    """
    Obtiene el nombre de la ubicación y el nombre/ID de su almacén asociado.
    """
    if not location_id:
        return None
    query = """
        SELECT
            l.name as location_name,
            w.name as warehouse_name,
            l.warehouse_id,  -- <-- ¡CORRECCIÓN! AÑADIR ESTA LÍNEA
            l.type as location_type
        FROM locations l
        LEFT JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.id = %s
    """
    result = execute_query(query, (location_id,), fetchone=True)
    return dict(result) if result else None

def get_partner_name(partner_id):
    """Obtiene el nombre de un partner por su ID."""
    if not partner_id:
        return None
    result = execute_query("SELECT name FROM partners WHERE id =  %s", (partner_id,), fetchone=True)
    return result['name'] if result else None

def get_location_details_by_names(company_id, warehouse_name, location_name):
    """
    Busca una ubicación INTERNA por el nombre del almacén y el nombre de la ubicación.
    Devuelve {'id': location_id, 'warehouse_id': warehouse_id} o None si no se encuentra.
    """
    # Esta función asume que location_name es 'Stock', 'Input', etc. (no el path completo)
    if not company_id or not warehouse_name or not location_name:
        return None
    query = """
        SELECT
            l.id,
            l.warehouse_id
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.company_id =  %s
          AND w.name =  %s
          AND l.name =  %s
          AND l.type = 'internal'
    """
    result = execute_query(query, (company_id, warehouse_name, location_name), fetchone=True)
    return dict(result) if result else None

def get_location_details_by_id(location_id: int):
    """
    Obtiene los detalles completos de una sola ubicación por su ID,
    incluyendo el nombre del almacén (para el LocationResponse schema).
    """
    query = """
    SELECT
        l.id, l.company_id, l.name, l.path, l.type, l.category,
        l.warehouse_id,
        w.name as warehouse_name
    FROM locations l
    LEFT JOIN warehouses w ON l.warehouse_id = w.id
    WHERE l.id = %s
    """
    return execute_query(query, (location_id,), fetchone=True)

def get_stock_summary_filtered_sorted(company_id, warehouse_id=None, filters={}, sort_by=None, ascending=True):
    """ 
    Obtiene el stock total por producto Y UBICACIÓN, INCLUYENDO RESERVADO Y DISPONIBLE.
    """
    base_query = """
    WITH ReservedStockSummary AS (
        SELECT 
            sm.product_id, sm.location_src_id,
            SUM(sm.product_uom_qty) as reserved_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE p.state = 'listo' AND p.company_id = %s
        GROUP BY sm.product_id, sm.location_src_id
    )
    SELECT
        p.sku, p.name as product_name, pc.name as category_name,
        w.name as warehouse_name, l.name as location_name,
        SUM(sq.quantity) as physical_quantity,
        COALESCE(MAX(rss.reserved_qty), 0) as reserved_quantity,
        (SUM(sq.quantity) - COALESCE(MAX(rss.reserved_qty), 0)) as available_quantity,
        u.name as uom_name,
        -- ALIAS EXPLÍCITOS NECESARIOS AQUÍ:
        w.id as warehouse_id, 
        l.id as location_id, 
        p.id as product_id
    FROM stock_quants sq
    JOIN products p ON sq.product_id = p.id
    JOIN locations l ON sq.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    LEFT JOIN ReservedStockSummary rss ON sq.product_id = rss.product_id AND sq.location_id = rss.location_src_id
    WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0.001
    """
    params = [company_id, company_id]
    where_clauses = []

    filter_map = {
        'warehouse_name': 'w.name', 'location_name': 'l.name', 'sku': 'p.sku', 
        'product_name': 'p.name', 'category_name': 'pc.name', 'uom_name': 'u.name',
        'warehouse_id': 'w.id', 'location_id': 'l.id'
    }
    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value is not None and value != "":
            if key in ['warehouse_id', 'location_id']:
                 where_clauses.append(f"{db_column} = %s")
                 params.append(value)
            else:
                 where_clauses.append(f"{db_column} ILIKE %s")
                 params.append(f"%{value}%")

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    base_query += " GROUP BY w.id, w.name, l.id, l.name, p.id, p.sku, p.name, pc.id, pc.name, u.id, u.name, rss.reserved_qty"

    sort_map = {
        'warehouse_name': 'w.name', 
        'location_name': 'l.name', 
        'sku': 'p.sku',
        'product_name': 'p.name', 
        'category_name': 'pc.name',     # <-- Usar pc.name en lugar de category_name
        'physical_quantity': 'physical_quantity', # Este sí funciona porque es un agregado
        'reserved_quantity': 'reserved_quantity', 
        'available_quantity': 'available_quantity',
        'uom_name': 'u.name'            # <-- Usar u.name en lugar de uom_name
    }
    order_by_col_key = sort_by if sort_by else 'sku'
    order_by_col = sort_map.get(order_by_col_key, 'p.sku')
    direction = "ASC" if ascending else "DESC"
    
    # Ahora COALESCE usará la columna real (pc.name), no el alias.
    if order_by_col in ['pc.name', 'u.name', 'l.name']:
         order_by_clause = f"COALESCE({order_by_col}, 'zzzz')"
    else:
         order_by_clause = order_by_col
         
    base_query += f" ORDER BY {order_by_clause} {direction}"
    
    return execute_query(base_query, tuple(params), fetchall=True)

def get_stock_on_hand_filtered_sorted(company_id, warehouse_id=None, filters={}, sort_by='product_name', ascending=True):
    """ 
    Obtiene el stock detallado por lote/serie, INCLUYENDO RESERVADO Y DISPONIBLE.
    """
    base_query = """
    WITH ReservedStock AS (
        SELECT 
            sm.product_id, sm.location_src_id, sml.lot_id, 
            SUM(sml.qty_done) as reserved_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN stock_move_lines sml ON sm.id = sml.move_id
        WHERE p.state = 'listo' AND p.company_id = %s
        GROUP BY sm.product_id, sm.location_src_id, sml.lot_id
    )
    SELECT
        p.sku, p.name as product_name, pc.name as category_name,
        w.name as warehouse_name, l.name as location_name, sl.name as lot_name, 
        sq.quantity as physical_quantity, u.name as uom_name,
        COALESCE(rs.reserved_qty, 0) as reserved_quantity,
        (sq.quantity - COALESCE(rs.reserved_qty, 0)) as available_quantity,
        -- ALIAS EXPLÍCITOS NECESARIOS AQUÍ:
        p.id as product_id, 
        w.id as warehouse_id, 
        l.id as location_id, 
        sl.id as lot_id,
        COALESCE(sl.name, '---') as lot_name_ordered
    FROM stock_quants sq
    JOIN products p ON sq.product_id = p.id
    JOIN locations l ON sq.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN stock_lots sl ON sq.lot_id = sl.id
    LEFT JOIN uom u ON p.uom_id = u.id
    LEFT JOIN ReservedStock rs ON sq.product_id = rs.product_id AND sq.location_id = rs.location_src_id AND (sq.lot_id = rs.lot_id OR (sq.lot_id IS NULL AND rs.lot_id IS NULL))
    WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0.001
    """
    params = [company_id, company_id]
    where_clauses = []

    filter_map = {
        'warehouse_name': 'w.name','location_name': 'l.name', 'sku': 'p.sku', 'product_name': 'p.name',
        'category_name': 'pc.name', 'lot_name': 'sl.name', 'uom_name': 'u.name',
        'warehouse_id': 'w.id', 'location_id': 'l.id'
    }
    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value is not None and value != "":
            if key in ['warehouse_id', 'location_id']:
                 where_clauses.append(f"{db_column} = %s")
                 params.append(value)
            elif key == 'lot_name' and value == '-':
                 where_clauses.append("sl.id IS NULL")
            else:
                 where_clauses.append(f"{db_column} ILIKE %s")
                 params.append(f"%{value}%")

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    sort_map = {
        'warehouse_name': 'w.name', 'location_name': 'l.name', 'sku': 'p.sku',
        'product_name': 'p.name', 'category_name': 'pc.name', 'lot_name': 'lot_name_ordered',
        'physical_quantity': 'physical_quantity', 'reserved_quantity': 'reserved_quantity', 'available_quantity': 'available_quantity',
        'uom_name': 'u.name'
    }
    order_by_col_key = sort_by if sort_by else 'sku'
    order_by_col = sort_map.get(order_by_col_key, 'p.sku')
    direction = "ASC" if ascending else "DESC"
    
    if order_by_col in ['pc.name', 'u.name', 'lot_name_ordered', 'l.name']:
         order_by_clause = f"COALESCE({order_by_col}, 'zzzz')"
    else:
         order_by_clause = order_by_col
         
    base_query += f" ORDER BY {order_by_clause} {direction}, p.sku ASC, lot_name_ordered ASC"
    
    return execute_query(base_query, tuple(params), fetchall=True)

def get_adjustments_filtered_sorted(company_id, filters={}, sort_by='id', ascending=False, limit=None, offset=None):
    sort_map = {
        'id': "p.id", 'name': "p.name", 'state': "p.state",
        'date': "p.scheduled_date", 'src_path': "l_src.path",
        'dest_path': "l_dest.path", 'responsible_user': "p.responsible_user",
        'adjustment_reason': "p.adjustment_reason"
    }
    order_by_column = sort_map.get(sort_by, "p.id")
    direction = "ASC" if ascending else "DESC"

    base_query = """
    SELECT 
        p.id, p.company_id, p.name, p.state, TO_CHAR(p.scheduled_date, 'YYYY-MM-DD') as date,
        l_src.path as src_path, l_dest.path as dest_path,
        p.responsible_user, p.adjustment_reason, p.notes, p.loss_confirmation
    FROM pickings p
    JOIN picking_types pt ON p.picking_type_id = pt.id
    LEFT JOIN locations l_src ON p.location_src_id = l_src.id
    LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
    WHERE p.company_id = %s AND pt.code = 'ADJ'
    """
    
    params = [company_id]
    where_clauses = []
    # ... (resto de la lógica de filtros no cambia) ...
    for key, value in filters.items():
        if not value: continue
        column_map = {
            'name': "p.name", 'state': "p.state", 'responsible_user': "p.responsible_user",
            'adjustment_reason': "p.adjustment_reason", 'src_path': "l_src.path", 'dest_path': "l_dest.path"
        }
        sql_column = column_map.get(key)
        if not sql_column: continue
        if key == 'state':
            where_clauses.append(f"{sql_column} = %s")
            params.append(value)
        else:
            where_clauses.append(f"{sql_column} ILIKE %s")
            params.append(f"%{value}%")
            
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)
    base_query += f" ORDER BY {order_by_column} {direction}"
    if limit is not None and offset is not None:
        base_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    return execute_query(base_query, tuple(params), fetchall=True)

# En app/database.py

def save_adjustment_draft(picking_id, header_data: dict, lines_data: list):
    """
    Guarda el progreso de un borrador de Ajuste.
    Actualiza la cabecera y reemplaza todas las líneas.
    """
    print(f"[DB-ADJ-SAVE] Guardando borrador para Picking ID: {picking_id}")
    try:
        with connect_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                
                # 1. Actualizar Cabecera (usando la función que ya existe)
                allowed_header_fields = {"location_src_id", "location_dest_id", "adjustment_reason", "loss_confirmation", "notes"}
                fields_to_update = {k: v for k, v in header_data.items() if k in allowed_header_fields}
                
                if fields_to_update:
                    set_clause = ", ".join([f"{key} = %s" for key in fields_to_update.keys()])
                    params = list(fields_to_update.values()) + [picking_id]
                    cursor.execute(f"UPDATE pickings SET {set_clause} WHERE id = %s", tuple(params))
                    print(f" -> Cabecera actualizada: {fields_to_update}")

                # 2. Borrar líneas 'draft' antiguas
                cursor.execute("SELECT id FROM stock_moves WHERE picking_id = %s AND state = 'draft'", (picking_id,))
                old_moves = cursor.fetchall()
                if old_moves:
                    old_move_ids = tuple([m[0] for m in old_moves])
                    placeholders = ','.join(['%s'] * len(old_move_ids))
                    cursor.execute(f"DELETE FROM stock_move_lines WHERE move_id IN ({placeholders})", old_move_ids)
                    cursor.execute(f"DELETE FROM stock_moves WHERE id IN ({placeholders})", old_move_ids)
                    print(f" -> {len(old_move_ids)} líneas borrador antiguas eliminadas.")

                # 3. Crear nuevas líneas 'draft'
                loc_src_id = header_data.get("location_src_id")
                loc_dest_id = header_data.get("location_dest_id")
                if not loc_src_id or not loc_dest_id:
                    raise ValueError("Se requieren ubicaciones de origen y destino para guardar líneas.")

                moves_with_tracking = {}
                for line in lines_data: # 'line' es un objeto schemas.StockMoveData
                    
                    # --- ¡CORRECCIÓN AQUÍ! Usar . en lugar de [] ---
                    product_id = line.product_id
                    quantity = line.quantity
                    cost = line.cost_at_adjustment
                    tracking_data = line.tracking_data
                    # ----------------------------------------------
                    
                    cursor.execute(
                        """INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done,
                                                     location_src_id, location_dest_id, state, cost_at_adjustment)
                           VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
                           RETURNING id""",
                        (picking_id, product_id, quantity, quantity, loc_src_id, loc_dest_id, cost)
                    )
                    move_id = cursor.fetchone()[0]

                    if tracking_data:
                        moves_with_tracking[move_id] = tracking_data
                        for lot_name, qty_done in tracking_data.items():
                             lot_row = get_lot_by_name(cursor, product_id, lot_name)
                             lot_id = lot_row[0] if lot_row else create_lot(cursor, product_id, lot_name)
                             cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move_id, lot_id, qty_done))
                
                print(f" -> {len(lines_data)} líneas nuevas creadas.")
                conn.commit()
                return True, "Progreso de ajuste guardado.", moves_with_tracking

    except Exception as e:
        print(f"[ERROR] en save_adjustment_draft: {e}")
        traceback.print_exc()
        return False, f"Error al guardar borrador: {e}", None

def get_adjustments_count(company_id, filters={}):
    """
    Cuenta el número total de albaranes de ajuste que coinciden con los filtros.
    Esencial para la paginación.
    """
    base_query = """
    SELECT COUNT(p.id) as total_count
    FROM pickings p
    JOIN picking_types pt ON p.picking_type_id = pt.id
    LEFT JOIN locations l_src ON p.location_src_id = l_src.id
    LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
    WHERE p.company_id =  %s AND pt.code = 'ADJ'
    """
    
    params = [company_id]
    where_clauses = []

    # --- Lógica de filtros (idéntica a la función anterior) ---
    for key, value in filters.items():
        if not value: continue
        column_map = {
            'name': "p.name", 'state': "p.state", 'responsible_user': "p.responsible_user",
            'adjustment_reason': "p.adjustment_reason", 'src_path': "l_src.path", 'dest_path': "l_dest.path"
        }
        sql_column = column_map.get(key)
        if not sql_column: continue

        if key == 'state':
            where_clauses.append(f"{sql_column} =  %s"); params.append(value)
        else:
            where_clauses.append(f"{sql_column} LIKE  %s"); params.append(f"%{value}%")
            
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

# En database.py

def _create_or_update_draft_picking_internal(
    cursor: psycopg2.extensions.cursor, 
    wo_id: int, 
    picking_code: str, # 'OUT' o 'RET'
    picking_data: dict, 
    company_id: int, 
    user_name: str
):
    """
    Función interna que crea o actualiza un picking borrador (OUT o RET)
    usando un cursor existente. (Versión PostgreSQL Robusta)
    Devuelve (picking_id, moves_with_tracking)
    """
    print(f"[DB-INTERNAL] Procesando picking borrador {picking_code} para WO ID: {wo_id}")
    
    warehouse_id = picking_data['warehouse_id']
    # Para 'OUT', usamos la ubicación específica seleccionada por el usuario.
    # Para 'RET', usamos None por ahora y dejamos que el sistema decida basedo en el tipo de operación.
    location_src_id_override = picking_data.get('location_src_id') if picking_code == 'OUT' else None
    
    date_attended = picking_data['date_attended_db']
    service_act_number = picking_data['service_act_number']
    lines_data = picking_data['lines_data']

    if not warehouse_id:
        raise ValueError(f"Se requiere un warehouse_id para el picking {picking_code}.")

    # --- 1. Buscar si ya existe el borrador ---
    cursor.execute(
        """SELECT p.id, pt.default_location_src_id, pt.default_location_dest_id
           FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
           WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = %s""",
        (wo_id, picking_code)
    )
    draft_picking = cursor.fetchone()

    picking_id = None
    # Variables para determinar las ubicaciones finales a usar en el picking
    final_loc_src_id = None
    final_loc_dest_id = None

    # --- 2. Obtener configuración del tipo de operación (NECESARIO SIEMPRE para defaults) ---
    cursor.execute(
        "SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id = %s AND code = %s",
        (warehouse_id, picking_code)
    )
    picking_type = cursor.fetchone()
    if not picking_type: 
        # Fallback de emergencia si no existe el tipo de operación específico para este almacén
        print(f"[WARN] No se encontró picking_type '{picking_code}' para WH {warehouse_id}. Buscando genérico...")
        # Podrías intentar buscar uno genérico si tu lógica lo permite, o fallar.
        # Por ahora, fallamos con un mensaje claro.
        raise ValueError(f"No está configurado el tipo de operación '{picking_code}' para el almacén ID {warehouse_id}.")

    picking_type_id = picking_type[0]
    default_src_id = picking_type[1]
    default_dest_id = picking_type[2]

    # --- 3. Determinar ubicaciones finales ---
    if picking_code == 'OUT':
        # Para OUT: Origen es la seleccionada por usuario (override), Destino es el default (Cliente)
        final_loc_src_id = location_src_id_override or default_src_id
        final_loc_dest_id = default_dest_id
    elif picking_code == 'RET':
        # Para RET: Origen es el default (Cliente), Destino es el default (Averiados/Principal)
        final_loc_src_id = default_src_id
        final_loc_dest_id = default_dest_id

    # Validar que tenemos ubicaciones
    if not final_loc_src_id or not final_loc_dest_id:
         raise ValueError(f"Configuración incompleta para '{picking_code}' en almacén {warehouse_id}. Faltan ubicaciones por defecto.")

    if draft_picking:
        # --- ACTUALIZAR EXISTENTE ---
        picking_id = draft_picking[0]
        print(f" -> Picking {picking_code} borrador encontrado (ID: {picking_id}). Actualizando...")
        
        # Si no hay líneas nuevas y es un RET, podríamos optar por borrarlo si existía.
        # Por ahora, simplemente lo actualizamos.
        
        cursor.execute(
            """UPDATE pickings
               SET warehouse_id = %s, location_src_id = %s, location_dest_id = %s, 
                   attention_date = %s, service_act_number = %s, responsible_user = %s
               WHERE id = %s""",
            (warehouse_id, final_loc_src_id, final_loc_dest_id, 
             date_attended, service_act_number, user_name, picking_id)
        )
    else:
        # --- CREAR NUEVO ---
        if not lines_data and picking_code == 'RET':
            print(f" -> No hay picking {picking_code} previo ni líneas nuevas. Omitiendo creación.")
            return None, {}
        
        print(f" -> Creando nuevo picking {picking_code} borrador...")
        # Obtener prefijo para el nombre
        cursor.execute("SELECT wt.code, pt.code FROM picking_types pt JOIN warehouses wt ON pt.warehouse_id = wt.id WHERE pt.id = %s", (picking_type_id,))
        codes = cursor.fetchone()
        prefix = f"{codes[0]}/{codes[1]}/"
        
        cursor.execute("SELECT COUNT(*) FROM pickings WHERE name LIKE %s", (f"{prefix}%",))
        count = cursor.fetchone()[0]
        picking_name = f"{prefix}{str(count + 1).zfill(5)}"

        cursor.execute(
            """INSERT INTO pickings (company_id, name, picking_type_id, warehouse_id, location_src_id, location_dest_id,
                                      state, work_order_id, custom_operation_type,
                                      service_act_number, attention_date, responsible_user)
               VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s, %s, %s, %s)
               RETURNING id""",
            (company_id, picking_name, picking_type_id, warehouse_id, final_loc_src_id, final_loc_dest_id,
             wo_id, "Liquidación por OT" if picking_code == 'OUT' else "Retiro de Campo",
             service_act_number, date_attended, user_name)
        )
        picking_id = cursor.fetchone()[0]
        print(f" -> Nuevo picking {picking_code} creado (ID: {picking_id}).")

    # --- 4. Gestionar Movimientos (Líneas) ---
    
    # 4.1. Borrar líneas anteriores
    cursor.execute("SELECT id FROM stock_moves WHERE picking_id = %s AND state = 'draft'", (picking_id,))
    old_moves = cursor.fetchall()
    if old_moves:
        old_move_ids = tuple([m[0] for m in old_moves])
        placeholders = ','.join(['%s'] * len(old_move_ids))
        cursor.execute(f"DELETE FROM stock_move_lines WHERE move_id IN ({placeholders})", old_move_ids)
        cursor.execute(f"DELETE FROM stock_moves WHERE id IN ({placeholders})", old_move_ids)
        print(f" -> {len(old_move_ids)} líneas {picking_code} antiguas eliminadas.")

    # 4.2. Insertar nuevas líneas
    partner_id_to_set = None
    if picking_code == 'OUT':
        # Para OUT, intentamos asignar un partner genérico al movimiento si no tiene uno específico
        cursor.execute("SELECT id FROM partners WHERE name = 'Cliente Varios' AND company_id = %s", (company_id,))
        res = cursor.fetchone()
        if res: partner_id_to_set = res[0]

    moves_created = 0
    moves_with_tracking = {}
    
    for line in lines_data:
        product_id = line['product_id']
        quantity = line['quantity']
        tracking_data = line.get('tracking_data', {}) # Series/Lotes seleccionados

        cursor.execute(
            """INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done,
                                         location_src_id, location_dest_id, state, partner_id)
               VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
               RETURNING id""",
            (picking_id, product_id, quantity, quantity, final_loc_src_id, final_loc_dest_id, partner_id_to_set)
        )
        move_id = cursor.fetchone()[0]
        moves_created += 1

        if tracking_data:
            moves_with_tracking[move_id] = tracking_data
            for serial_name, qty in tracking_data.items():
                 # Buscar o crear el lote/serie
                 cursor.execute("SELECT id FROM stock_lots WHERE product_id = %s AND name = %s", (product_id, serial_name))
                 lot_res = cursor.fetchone()
                 if lot_res:
                     lot_id = lot_res[0]
                 else:
                     cursor.execute("INSERT INTO stock_lots (name, product_id) VALUES (%s, %s) RETURNING id", (serial_name, product_id))
                     lot_id = cursor.fetchone()[0]
                 
                 # Insertar la línea de detalle
                 cursor.execute(
                     "INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)",
                     (move_id, lot_id, qty)
                 )

    print(f" -> {moves_created} líneas nuevas creadas para picking {picking_code} (ID: {picking_id}).")
    return picking_id, moves_with_tracking

def save_liquidation_progress(wo_id, wo_updates: dict, consumo_data: dict, retiro_data: dict, company_id, user_name):
    """
    Actualiza la WO y crea/actualiza AMBOS pickings (OUT y RET) en UNA SOLA TRANSACCIÓN.
    """
    print(f"[DB-SAVE-LIQ] Iniciando guardado ATÓMICO para WO ID: {wo_id}")
    try:
        with connect_db() as conn:
            # ¡CAMBIO! Usar DictCursor aquí también por consistencia
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                
                # --- 1. Actualizar Work Order ---
                if wo_updates:
                    set_clause = ", ".join([f"{key} = %s" for key in wo_updates.keys()])
                    params = list(wo_updates.values()) + [wo_id]
                    cursor.execute(f"UPDATE work_orders SET {set_clause} WHERE id = %s", tuple(params))
                    print(f" -> work_orders (ID: {wo_id}) actualizada: {wo_updates}")
                
                # --- 2. Procesar Picking de Consumo (OUT) ---
                if consumo_data:
                    # ¡CAMBIO! Ya no nos importa lo que devuelve, solo que se ejecute
                    _create_or_update_draft_picking_internal(
                        cursor, wo_id, 'OUT', consumo_data, company_id, user_name
                    )
                
                # --- 3. Procesar Picking de Retiro (RET) ---
                if retiro_data:
                    # ¡CAMBIO! Ya no nos importa lo que devuelve
                    _create_or_update_draft_picking_internal(
                        cursor, wo_id, 'RET', retiro_data, company_id, user_name
                )
                else:
                    # (La lógica de eliminar el borrador 'RET' no cambia)
                    print(f" -> No hay datos de Retiro. Buscando y eliminando picking 'RET' borrador para WO ID: {wo_id}...")
                    cursor.execute(
                        """SELECT p.id FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
                           WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = 'RET'""",
                        (wo_id,)
                    )
                    draft_ret_picking = cursor.fetchone()
                    if draft_ret_picking:
                        picking_id_to_delete = draft_ret_picking['id']
                        print(f"     -> Picking 'RET' borrador (ID: {picking_id_to_delete}) encontrado. Eliminando...")
                        # (La lógica de DELETE no cambia)
                        cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN (SELECT id FROM stock_moves WHERE picking_id = %s)", (picking_id_to_delete,))
                        cursor.execute("DELETE FROM stock_moves WHERE picking_id = %s", (picking_id_to_delete,))
                        cursor.execute("DELETE FROM pickings WHERE id = %s", (picking_id_to_delete,))
                        print(f"     -> Picking 'RET' borrador (ID: {picking_id_to_delete}) eliminado.")
                    else:
                        print("     -> No se encontró ningún picking 'RET' borrador. No se requiere eliminación.")
                
                # --- 4. Commit ---
                conn.commit()
                print("[DB-SAVE-LIQ] Transacción completada (COMMIT).")
                return True, "Progreso de liquidación guardado."

    except Exception as e:
        print(f"[ERROR] en save_liquidation_progress: {e}")
        traceback.print_exc()
        return False, f"Error al guardar borrador: {e}"

def get_work_orders_filtered_sorted(company_id, filters={}, sort_by='id', ascending=False, limit=None, offset=None):
    """
    Obtiene las OTs con paginación, filtros y ordenamiento (versión PostgreSQL).
    """
    sort_map = {
        'id': "wo.id", 'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
        'job_type': "wo.job_type", 'customer_name': "wo.customer_name", 'address': "wo.address",
        'phase': "wo.phase", 'warehouse_name': "warehouse_name",
        'location_src_path': "location_src_path", 'service_act_number': "service_act_number",
        'attention_date_str': "attention_date_sortable",
        'date_registered': "wo.date_registered"
    }
    order_by_column = sort_map.get(sort_by, "wo.id")
    direction = "ASC" if ascending else "DESC"

    base_query = """
    FROM work_orders wo
    LEFT JOIN pickings p_draft ON wo.id = p_draft.work_order_id AND p_draft.state = 'draft' AND p_draft.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_draft ON p_draft.warehouse_id = w_draft.id
    LEFT JOIN locations l_draft ON p_draft.location_src_id = l_draft.id
    LEFT JOIN pickings p_done ON wo.id = p_done.work_order_id AND p_done.state = 'done' AND p_done.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_done ON p_done.warehouse_id = w_done.id
    LEFT JOIN locations l_done ON p_done.location_src_id = l_done.id
    """
    
    # --- ¡CORRECCIÓN AQUÍ! ---
    select_clause = """
    SELECT
        wo.id, wo.company_id, wo.ot_number, wo.customer_name, wo.address, -- <-- AÑADIDO wo.company_id
        wo.service_type, wo.job_type, wo.phase, wo.date_registered,
        COALESCE(w_draft.name, w_done.name, 'N/A') as warehouse_name,
        COALESCE(l_draft.path, l_done.path, '-') as location_src_path,
        COALESCE(p_draft.service_act_number, p_done.service_act_number, '') as service_act_number,
        COALESCE(
            TO_CHAR(p_draft.attention_date, 'DD/MM/YYYY'),
            TO_CHAR(p_done.attention_date, 'DD/MM/YYYY'),
            ''
        ) as attention_date_str,
        COALESCE(p_draft.attention_date, p_done.attention_date, '1970-01-01') as attention_date_sortable
    """
    # --- FIN CORRECCIÓN ---

    params = [company_id]
    where_clauses = ["wo.company_id = %s"]
    filter_map = {
        'id': "wo.id", # <-- AÑADIR ESTA LÍNEA
        'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
        'job_type': "wo.job_type", 'customer_name': "wo.customer_name",
        'address': "wo.address", 'phase': "wo.phase",
        'warehouse_name': "warehouse_name", 'location_src_path': "location_src_path",
        'service_act_number': "service_act_number"
    }

    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value:
            # --- MODIFICAR ESTE BLOQUE ---
            if key == 'phase' or key == 'id': # <-- Añadir 'id' aquí
                where_clauses.append(f"{db_column} = %s")
                params.append(value)
            else:
                where_clauses.append(f"{db_column} ILIKE %s")
                params.append(f"%{value}%")

    where_string = " WHERE " + " AND ".join(where_clauses)
    final_query = f"{select_clause} {base_query} {where_string} ORDER BY {order_by_column} {direction}"
    
    if limit is not None and offset is not None:
        final_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    return execute_query(final_query, tuple(params), fetchall=True)

def get_work_orders_count(company_id, filters={}):
    # (La lógica es similar, usa %s e ILIKE)
    base_query = """
    FROM work_orders wo
    LEFT JOIN pickings p_draft ON wo.id = p_draft.work_order_id AND p_draft.state = 'draft' AND p_draft.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_draft ON p_draft.warehouse_id = w_draft.id
    LEFT JOIN locations l_draft ON p_draft.location_src_id = l_draft.id
    LEFT JOIN pickings p_done ON wo.id = p_done.work_order_id AND p_done.state = 'done' AND p_done.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_done ON p_done.warehouse_id = w_done.id
    LEFT JOIN locations l_done ON p_done.location_src_id = l_done.id
    """
    # (Los alias en SELECT no son necesarios para COUNT, pero sí para el WHERE)
    select_clause = """
    SELECT COUNT(DISTINCT wo.id) as total_count, 
        COALESCE(w_draft.name, w_done.name, 'N/A') as warehouse_name,
        COALESCE(l_draft.path, l_done.path, '-') as location_src_path,
        COALESCE(p_draft.service_act_number, p_done.service_act_number, '') as service_act_number
    """

    params = [company_id]
    where_clauses = ["wo.company_id = %s"]
    filter_map = {
        'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
        'job_type': "wo.job_type", 'customer_name': "wo.customer_name",
        'address': "wo.address", 'phase': "wo.phase",
        'warehouse_name': "warehouse_name", 'location_src_path': "location_src_path",
        'service_act_number': "service_act_number"
    }

    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value:
            if key == 'phase':
                where_clauses.append(f"{db_column} = %s")
                params.append(value)
            else:
                where_clauses.append(f"{db_column} ILIKE %s")
                params.append(f"%{value}%")
                
    where_string = " WHERE " + " AND ".join(where_clauses)
    
    # (La subconsulta ya no es necesaria si usamos DISTINCT)
    count_query = f"""
        SELECT COUNT(DISTINCT wo.id) as total_count
        {base_query}
        {where_string}
    """
    
    result = execute_query(count_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

def validate_user_and_get_permissions(username, plain_password):
    """
    Valida al usuario y, si tiene éxito, devuelve sus detalles y un set de permisos.
    Devuelve: (user_data, permissions_set) o (None, None)
    """
    try:
        user = execute_query("SELECT * FROM users WHERE username =  %s", (username,), fetchone=True)
        
        if not user:
            print(f"[AUTH] Fallo: Usuario '{username}' no encontrado.")
            return None, None # Usuario no encontrado
        
        if not user['is_active']:
            print(f"[AUTH] Fallo: Usuario '{username}' está inactivo.")
            return None, None # Usuario inactivo

        # Verificar la contraseña
        if not check_password(user['hashed_password'], plain_password):
            print(f"[AUTH] Fallo: Contraseña incorrecta para '{username}'.")
            return None, None # Contraseña incorrecta
        
        # ¡Éxito! Cargar permisos
        print(f"[AUTH] Éxito: Usuario '{username}' validado. Cargando permisos...")
        user_data = dict(user)
        role_id = user_data['role_id']
        
        permissions = execute_query(
            """SELECT p.key
               FROM role_permissions rp
               JOIN permissions p ON rp.permission_id = p.id
               WHERE rp.role_id =  %s""",
            (role_id,),
            fetchall=True
        )
        
        # Convertir la lista de diccionarios en un set de strings para búsquedas rápidas
        permissions_set = {perm['key'] for perm in permissions}
        print(f" -> {len(permissions_set)} permisos cargados.")
        
        return user_data, permissions_set

    except Exception as e:
        print(f"[ERROR] en validate_user_and_get_permissions: {e}")
        traceback.print_exc()
        return None, None
    
def get_users_for_admin():
    """Obtiene todos los usuarios con el nombre de su rol."""
    query = """
        SELECT u.id, u.username, u.full_name, u.is_active, r.name as role_name, u.role_id
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        ORDER BY u.username
    """
    return execute_query(query, fetchall=True)

def get_roles_for_admin():
    """Obtiene todos los roles."""
    return execute_query("SELECT id, name, description FROM roles ORDER BY name", fetchall=True)

def get_permissions_for_admin():
    """Obtiene todos los permisos disponibles."""
    return execute_query("SELECT id, key, description FROM permissions ORDER BY key", fetchall=True)

def get_permission_matrix():
    """
    Obtiene la matriz completa de permisos vs roles.
    Devuelve: ({role_id: role_name}, {perm_id: perm_key}, {(role_id, perm_id): True})
    """
    roles = {r['id']: r['name'] for r in get_roles_for_admin()}
    permissions = {p['id']: p['key'] for p in get_permissions_for_admin()}
    
    matrix_data = execute_query("SELECT role_id, permission_id FROM role_permissions", fetchall=True)
    matrix = {(m['role_id'], m['permission_id']): True for m in matrix_data}
    
    return roles, permissions, matrix

def update_role_permissions(role_id, permission_id, has_permission: bool):
    """Añade o quita un permiso a un rol. (Versión PostgreSQL)"""
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                if has_permission:
                    # --- ¡CAMBIO DE SINTAXIS! ---
                    cursor.execute("INSERT INTO role_permissions (role_id, permission_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (role_id, permission_id))
                    print(f"[DB-RBAC] Permiso {permission_id} AÑADIDO a Rol {role_id}")
                else:
                    cursor.execute("DELETE FROM role_permissions WHERE role_id = %s AND permission_id = %s", (role_id, permission_id))
                    print(f"[DB-RBAC] Permiso {permission_id} QUITADO de Rol {role_id}")
                conn.commit()
            return True, "Permiso actualizado"
    except Exception as e:
        print(f"[ERROR] en update_role_permissions: {e}")
        return False, str(e)

def create_role(name, description):
    """Crea un nuevo rol."""
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO roles (name, description) VALUES (%s, %s) RETURNING id",
                    (name, description)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e:  # Captura genérica de psycopg2
        if "roles_name_key" in str(e):  # Revisa el nombre real del constraint UNIQUE en PostgreSQL
            raise ValueError(f"El rol '{name}' ya existe.")
        else:
            raise e

def update_role(role_id, name, description):
    """Actualiza un rol existente."""
    try:
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE roles SET name = %s, description = %s WHERE id = %s",
                    (name, description, role_id)
                )
                conn.commit()
    except Exception as e:  # Captura genérica de psycopg2
        if "roles_name_key" in str(e):  # Revisa el nombre real del constraint UNIQUE en PostgreSQL
            raise ValueError(f"El rol '{name}' ya existe.")
        else:
            raise e

def create_user(username, plain_password, full_name, role_id):
    """Crea un nuevo usuario con contraseña hasheada."""
    if not username or not plain_password or not full_name or not role_id:
        raise ValueError("Todos los campos son obligatorios.")

    try:
        hashed_pass = hash_password(plain_password)
        with connect_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO users (username, hashed_password, full_name, role_id, is_active)
                    VALUES (%s, %s, %s, %s, 1)
                    RETURNING id
                    """,
                    (username, hashed_pass, full_name, role_id)
                )
                new_id = cursor.fetchone()[0]
                conn.commit()
                return new_id
    except Exception as e:  # Captura genérica de psycopg2
        if "users_username_key" in str(e):  # Revisa el nombre real del constraint UNIQUE en PostgreSQL
            raise ValueError(f"El nombre de usuario '{username}' ya existe.")
        else:
            raise e

def update_user(user_id, full_name, role_id, is_active, new_password=None):
    """Actualiza un usuario. Si new_password se provee, la cambia."""
    try:
        with connect_db() as conn:
            cursor = conn.cursor()
            if new_password:
                # Si hay nueva contraseña, hashearla y actualizarla
                hashed_pass = hash_password(new_password)
                cursor.execute(
                    "UPDATE users SET full_name =  %s, role_id =  %s, is_active =  %s, hashed_password =  %s WHERE id =  %s",
                    (full_name, role_id, 1 if is_active else 0, hashed_pass, user_id)
                )
                print(f"[DB-RBAC] Usuario {user_id} actualizado (CON nueva contraseña).")
            else:
                # Si no hay nueva contraseña, no tocar ese campo
                cursor.execute(
                    "UPDATE users SET full_name =  %s, role_id =  %s, is_active =  %s WHERE id =  %s",
                    (full_name, role_id, 1 if is_active else 0, user_id)
                )
                print(f"[DB-RBAC] Usuario {user_id} actualizado (SIN nueva contraseña).")
            conn.commit()
    except Exception as e:
        raise ValueError(f"Error al actualizar usuario: {e}")

def get_product_reservations(product_id, location_id, lot_id=None):
    """
    Devuelve una lista de operaciones 'listo' que reservan stock,
    incluyendo destino y usuario responsable.
    """
    params = [product_id, location_id]
    
    # Selección de columnas ampliada
    select_clause = """
        SELECT 
            p.name as picking_name,
            pt.code as op_type,
            p.scheduled_date,
            p.responsible_user,          -- <-- NUEVO: Usuario
            l_dest.path as dest_location, -- <-- NUEVO: Ubicación Destino
            SUM(sm.product_uom_qty) as reserved_qty
    """

    # Joins adicionales para llegar a la ubicación de destino
    joins_clause = """
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN locations l_dest ON sm.location_dest_id = l_dest.id  -- <-- NUEVO JOIN
    """

    where_clause = """
        WHERE sm.product_id = %s
          AND sm.location_src_id = %s
          AND p.state = 'listo'
    """

    group_by_clause = """
        GROUP BY p.name, pt.code, p.scheduled_date, p.responsible_user, l_dest.path
    """

    if lot_id is not None:
        # Caso Vista Detalle (filtrar por lote)
        joins_clause += " JOIN stock_move_lines sml ON sm.id = sml.move_id"
        where_clause += " AND sml.lot_id = %s"
        params.append(lot_id)
        # Nota: Si agrupamos por lote, la cantidad reservada debe ser SUM(sml.qty_done)
        select_clause = select_clause.replace("SUM(sm.product_uom_qty)", "SUM(sml.qty_done)")
    
    query = f"{select_clause} {joins_clause} {where_clause} {group_by_clause} ORDER BY p.scheduled_date ASC"
    
    return execute_query(query, tuple(params), fetchall=True)

def get_real_available_stock(product_id, location_id):
    """
    Calcula el stock REALMENTE disponible: (Físico - Reservado).
    Es la única fuente de verdad para saber si se puede tomar algo.
    """
    if not product_id or not location_id: return 0.0

    # 1. Stock Físico Total
    res_phy = execute_query(
        "SELECT SUM(quantity) as total FROM stock_quants WHERE product_id = %s AND location_id = %s",
        (product_id, location_id), fetchone=True
    )
    physical = res_phy['total'] if res_phy and res_phy['total'] else 0.0

    # 2. Stock Reservado por otros pickings 'listo'
    # (No necesitamos excluir el picking actual aquí porque esta función es genérica.
    #  Si se usa dentro de una validación de picking específico, se debe ajustar la lógica allí o restar manualmente).
    res_reserved = execute_query(
        """SELECT SUM(sm.product_uom_qty) as reserved
           FROM stock_moves sm
           JOIN pickings p ON sm.picking_id = p.id
           WHERE sm.product_id = %s AND sm.location_src_id = %s AND p.state = 'listo' AND sm.state != 'cancelled'""",
        (product_id, location_id), fetchone=True
    )
    reserved = res_reserved['reserved'] if res_reserved and res_reserved['reserved'] else 0.0

    return max(0.0, physical - reserved) # Nunca devolver negativo por seguridad