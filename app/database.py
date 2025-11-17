# app/database.py
import psycopg2
import psycopg2.extras # Para acceder a los datos como diccionario
import psycopg2.extensions
import psycopg2.pool
import os
from datetime import datetime, date, timedelta
import traceback
import functools
from collections import defaultdict
import hashlib
from dotenv import load_dotenv
import json

# --- CONFIGURACIÓN DEL POOL GLOBAL ---
# Este pool se creará UNA VEZ al iniciar la app.
db_pool = None
DATABASE_URL = None
#DATABASE_URL = os.environ.get("DATABASE_URL")

ALLOWED_PICKING_FIELDS_TO_UPDATE = {
    'name', 
    'partner_id', 
    'state', 
    'scheduled_date', 
    'responsible_user',
    'service_act_number',
    'date_attended'
    # ... añade aquí CUALQUIER otro campo que sea seguro actualizar
}

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

    print(f"DEBUG URL: {repr(DATABASE_URL)}")

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
    
    # --- Extensiones ---
    try:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        print(" -> Extensión 'pg_trgm' habilitada.")
    except Exception as e:
        print(f"[ADVERTENCIA] No se pudo habilitar 'pg_trgm': {e}")
        conn.rollback()
        cursor = conn.cursor()

    # --- 1. CREACIÓN DE TABLA COMPANIES (CORREGIDO) ---
    # Definimos la columna directamente aquí. Ya no necesitamos ALTER.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY, 
            name TEXT UNIQUE NOT NULL,
            country_code TEXT DEFAULT 'PE'
        );
    """)
    
    # ¡IMPORTANTE! Hacemos commit INMEDIATAMENTE para guardar la tabla
    # antes de seguir con el resto. Esto evita que un error futuro la borre.
    conn.commit() 
    
    # (Opcional) Bloque de compatibilidad por si restauras una BD vieja
    # Solo intenta añadir la columna si la tabla ya existía sin ella
    try:
        cursor.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS country_code TEXT DEFAULT 'PE';")
        conn.commit()
    except Exception:
        conn.rollback() # Si falla, solo revertimos el ALTER, no la creación
        cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_categories (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE (company_id, name)
        );
    """)
    cursor.execute("CREATE TABLE IF NOT EXISTS uom (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL);")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_categories (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE (company_id, name)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS partner_categories (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE (company_id, name)
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
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, name TEXT NOT NULL, sku TEXT NOT NULL, 
            type TEXT NOT NULL DEFAULT 'storable', barcode TEXT, notes TEXT, 
            category_id INTEGER, uom_id INTEGER,
            tracking TEXT NOT NULL DEFAULT 'none',
            ownership TEXT NOT NULL DEFAULT 'owned' CHECK(ownership IN ('owned', 'consigned')),
            standard_price REAL DEFAULT 0,
            UNIQUE (company_id, sku)
        );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS warehouses (
            id SERIAL PRIMARY KEY, 
            company_id INTEGER NOT NULL, 
            name TEXT NOT NULL, 
            code TEXT NOT NULL, -- Quitamos UNIQUE de aquí
            social_reason TEXT, ruc TEXT, email TEXT, phone TEXT, address TEXT,
            category_id INTEGER, 
            status TEXT NOT NULL DEFAULT 'activo' CHECK(status IN ('activo', 'inactivo')),
            UNIQUE (company_id, code) -- Añadimos restricción compuesta
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id SERIAL PRIMARY KEY, company_id INTEGER NOT NULL, name TEXT NOT NULL,
            social_reason TEXT, ruc TEXT, email TEXT, phone TEXT, address TEXT,
            category_id INTEGER, UNIQUE (company_id, name)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            id SERIAL PRIMARY KEY, 
            company_id INTEGER NOT NULL, 
            name TEXT NOT NULL, 
            path TEXT NOT NULL, -- Quitamos UNIQUE de aquí
            type TEXT NOT NULL DEFAULT 'internal', 
            category TEXT, 
            warehouse_id INTEGER,
            UNIQUE (company_id, path) -- Añadimos restricción compuesta
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS picking_types (
            id SERIAL PRIMARY KEY, 
            company_id INTEGER NOT NULL, 
            name TEXT NOT NULL, -- Quitamos UNIQUE de aquí
            code TEXT NOT NULL, 
            warehouse_id INTEGER NOT NULL, 
            default_location_src_id INTEGER, 
            default_location_dest_id INTEGER,
            UNIQUE (company_id, name) -- Añadimos restricción compuesta
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
    # --- ¡NUEVA TABLA! Relación Muchos-a-Muchos Usuario-Compañía ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_companies (
            user_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, company_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
        );
    """)
    print(" -> Tabla 'user_companies' creada/verificada.")

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
        cursor.execute("ALTER TABLE product_categories ADD CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES companies(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE warehouse_categories ADD CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES companies(id);")
    except psycopg2.Error: pass
    try:
        cursor.execute("ALTER TABLE partner_categories ADD CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES companies(id);")
    except psycopg2.Error: pass
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
        
        print(" -> Añadiendo nuevos índices (productos, socios, almacenes)...")
        
        # Índices para 'products'
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_company_id ON products (company_id);")
        # Índices TRGM para búsqueda 'ILIKE' en nombre y SKU (requiere la extensión pg_trgm)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin (name gin_trgm_ops);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_sku_trgm ON products USING gin (sku gin_trgm_ops);")
        
        # Índice para 'partners' (acelera la búsqueda por compañía y categoría)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_partners_company_id_category_id ON partners (company_id, category_id);")
        
        # Índice para 'warehouses' (acelera la búsqueda por compañía y categoría)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_warehouses_company_id_category_id ON warehouses (company_id, category_id);")
        
    except Exception as e:
        print(f"Error al crear índices: {e}")
        # (No detenemos la ejecución, solo lo reportamos)
        pass
    print(" -> Índices creados/verificados.")

    conn.commit()


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
        ("Transferencia entre Contratas", "INT", "Mueve stock entre contratistas.", "internal", "internal"),
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


def hash_password(password):
    """Genera un hash SHA-256 para la contraseña."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def check_password(hashed_password, plain_password):
    """Verifica si la contraseña coincide con el hash."""
    return hashed_password == hash_password(plain_password)

def get_product_categories(company_id: int):
    """Obtiene categorías de producto FILTRADAS POR COMPAÑÍA."""
    return execute_query(
        "SELECT id, name FROM product_categories WHERE company_id = %s ORDER BY name", 
        (company_id,), 
        fetchall=True
    )
def create_product_category(name: str, company_id: int):
    """Crea una categoría de producto PARA UNA COMPAÑÍA."""
    try:
        new_item = execute_commit_query(
            "INSERT INTO product_categories (name, company_id) VALUES (%s, %s) RETURNING id, name",
            (name, company_id),
            fetchone=True
        )
        return new_item
    except Exception as e:
        if "product_categories_company_id_name_key" in str(e): # Error de duplicado
            raise ValueError(f"La categoría '{name}' ya existe para esta compañía.")
        raise e

def update_product_category(category_id: int, name: str, company_id: int):
    """Actualiza una categoría de producto, verificando la compañía."""
    try:
        updated_item = execute_commit_query(
            "UPDATE product_categories SET name = %s WHERE id = %s AND company_id = %s RETURNING id, name",
            (name, category_id, company_id),
            fetchone=True
        )
        if not updated_item:
            raise ValueError("Categoría no encontrada o no pertenece a esta compañía.")
        return updated_item
    except Exception as e:
        if "product_categories_company_id_name_key" in str(e):
            raise ValueError(f"El nombre '{name}' ya existe (duplicado).")
        raise e
    
def delete_product_category(category_id: int, company_id: int):
    """Elimina una categoría de producto, verificando la compañía."""
    try:
        execute_commit_query(
            "DELETE FROM product_categories WHERE id = %s AND company_id = %s",
            (category_id, company_id)
        )
        return True, "Categoría eliminada."
    except Exception as e:
        # (Si falla por FK, e.g. un producto la usa, aquí se captura)
        if "foreign key constraint" in str(e):
            return False, "Error: Esta categoría ya está siendo usada por productos."
        return False, f"Error inesperado: {e}"

def delete_product(product_id):
    """
    Elimina un producto y sus datos asociados (quants, lots)
    SÓLO SI no tiene movimientos de stock.
    Usa el pool, pero maneja la transacción manualmente.
    """
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    conn = None # 1. Definimos la conexión fuera del try
    try:
        # 2. Obtenemos UNA conexión del pool para toda la transacción
        conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor

        # Usamos un cursor para toda la operación
        with conn.cursor() as cursor:
            
            # --- EL GUARDIÁN (LECTURA) ---
            cursor.execute("SELECT COUNT(*) FROM stock_moves WHERE product_id = %s", (product_id,))
            move_count = cursor.fetchone()[0]
            
            # --- LÓGICA DE NEGOCIO ---
            if move_count > 0:
                # Si retornamos aquí, el 'finally' se ejecutará
                # y devolverá la conexión al pool. ¡Perfecto!
                return (False, "Este producto no se puede eliminar porque tiene movimientos de inventario registrados.")
            
            # --- LA ELIMINACIÓN (ESCRITURA MÚLTIPLE) ---
            # Si move_count == 0, procedemos a borrar todo
            cursor.execute("DELETE FROM stock_quants WHERE product_id = %s", (product_id,))
            cursor.execute("DELETE FROM stock_lots WHERE product_id = %s", (product_id,))
            cursor.execute("DELETE FROM stock_moves WHERE product_id = %s", (product_id,)) # (Aunque ya sabemos que son 0)
            cursor.execute("DELETE FROM products WHERE id = %s", (product_id,))
            
            # 3. Si todos los DELETEs fueron bien, hacemos COMMIT
            conn.commit()
            
            return (True, "Producto eliminado correctamente.")
            
    except Exception as e:
        # 4. Si CUALQUIER COSA falla (el SELECT, un DELETE),
        #    hacemos rollback para revertir todo.
        if conn:
            conn.rollback()
        print(f"[DB-ERROR] delete_product: {e}")
        traceback.print_exc() # Muy útil para depurar
        return (False, f"Error inesperado en la base de datos: {e}")
        
    finally:
        # 5. PASE LO QUE PASE (éxito, error, o return anticipado),
        #    DEVOLVEMOS la conexión al pool.
        if conn:
            db_pool.putconn(conn)

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

    base_query += " GROUP BY w.id, w.name, p.id, p.sku, p.name, sl.id, sl.name, pc.id, pc.name, u.id, u.name"
    
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
    """
    Crea una nueva Unidad de Medida (UOM) usando el pool de conexiones.
    Maneja la restricción de nombre único.
    """
    query = "INSERT INTO uom (name) VALUES (%s) RETURNING id"
    params = (name,)
    
    try:
        # 1. Llamamos a la función de escritura, pidiendo que retorne el resultado
        result = execute_commit_query(query, params, fetchone=True)
        
        if result:
            new_id = result[0] # O result['id'] si tu cursor devuelve dict
            return new_id
        else:
            raise Exception("No se pudo crear la UOM o no se retornó el ID.")

    except Exception as e: 
        # 2. La lógica para detectar el error de duplicado sigue
        #    funcionando porque execute_commit_query re-lanza el error.
        if "uom_name_key" in str(e): 
            raise ValueError(f"La unidad de medida '{name}' ya existe.")
        else:
            # Re-lanzar cualquier otro error de BD
            raise e

def update_uom(uom_id, name):
    """
    Actualiza el nombre de una Unidad de Medida (UOM) usando el pool de conexiones.
    Maneja la restricción de nombre único.
    """
    query = "UPDATE uom SET name = %s WHERE id = %s"
    params = (name, uom_id)
    
    try:
        # 1. Llamamos a la función de escritura centralizada.
        # No necesitamos fetchone=True para un UPDATE.
        execute_commit_query(query, params)
        
    except Exception as e: 
        # 2. La excepción de la BD es re-lanzada por execute_commit_query,
        #    así que la capturamos aquí para manejarla.
        if "uom_name_key" in str(e): 
            raise ValueError(f"La unidad de medida '{name}' ya existe.")
        else:
            # Re-lanzar cualquier otro error de BD
            raise e

def delete_uom(uom_id):
    """
    Elimina una Unidad de Medida (UOM) usando el pool de conexiones.
    Maneja errores de integridad referencial (foreign key).
    """
    query = "DELETE FROM uom WHERE id = %s"
    params = (uom_id,)
    
    try:
        # 1. Usamos la función centralizada de escritura
        # No se necesita fetchone=True para un DELETE
        execute_commit_query(query, params)
        
        # 2. Si no hubo error, la eliminación fue exitosa
        return True, "Unidad de medida eliminada."
        
    except Exception as e:
        # 3. execute_commit_query re-lanza el error de la BD,
        #    así que podemos inspeccionarlo aquí.
        
        # Detectar error de llave foránea
        if "violates foreign key constraint" in str(e):
            return False, "No se puede eliminar: Esta UdM está asignada a uno o más productos."
        
        # Cualquier otro error
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
    """
    Crea un nuevo producto usando el pool de conexiones.
    [VERSIÓN CORREGIDA]
    """
    global db_pool # Necesitas acceso al pool global
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado (desde create_product). Intentando...")
        init_db_pool()
        if not db_pool:
             raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    conn = None
    try:
        conn = db_pool.getconn() # <-- 1. Obtener conexión del POOL
        
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO products (name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price)
            )
            new_id = cursor.fetchone()[0]
            conn.commit() # <-- 2. Hacer commit (¡MUY IMPORTANTE para INSERT/UPDATE!)
            return new_id
    
    except Exception as e:
        if conn:
            conn.rollback() # <-- 3. Deshacer cambios si algo falla
        
        # Mantenemos tu lógica para el SKU duplicado
        if "products_sku_key" in str(e):
            raise ValueError(f"El SKU '{sku}' ya existe.")
        else:
            # Imprime el error real de la BD en la consola del backend
            print(f"Error DB [create_product]: {e}") 
            traceback.print_exc()
            raise e # Re-lanza la excepción para que FastAPI la capture (y muestre el 500)
    
    finally:
        if conn:
            db_pool.putconn(conn) # <-- 4. Devolver la conexión al POOL (pase lo que pase)

def update_product(product_id, name, sku, category_id, tracking, uom_id, ownership, standard_price):
    """
    Actualiza un producto existente usando el pool de conexiones.
    Maneja la restricción de SKU único.
    """
    query = """
        UPDATE products 
        SET name = %s, sku = %s, category_id = %s, tracking = %s, 
            uom_id = %s, ownership = %s, standard_price = %s 
        WHERE id = %s
    """
    params = (name, sku, category_id, tracking, uom_id, ownership, standard_price, product_id)

    try:
        # 1. Usamos la función centralizada de escritura.
        execute_commit_query(query, params)
        
    except Exception as e:
        # 2. Manejamos errores comunes (como SKU duplicado)
        #    que execute_commit_query re-lanza desde la BD.
        
        # ¡Ajusta 'products_sku_key' al nombre real de tu constraint si es diferente!
        if "products_sku_key" in str(e): 
            raise ValueError(f"El SKU '{sku}' ya existe para otro producto.")
        
        # Puedes añadir más 'elif' si, por ejemplo, el nombre también es único
        # elif "products_name_key" in str(e):
        #    raise ValueError(f"El nombre de producto '{name}' ya existe.")
        
        else:
            # Re-lanzar cualquier otro error de BD
            raise e

def get_work_orders(company_id):
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
        
        COALESCE(
            TO_CHAR(p_draft.attention_date, 'DD/MM/YYYY'),
            TO_CHAR(p_done.attention_date, 'DD/MM/YYYY'),
            ''
        ) as attention_date_str

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
    """
    Crea una nueva Orden de Trabajo (Work Order) usando el pool de conexiones.
    Maneja la restricción de OT duplicada.
    """
    print(f"[DB-DEBUG] Creando Work Order: OT={ot_number}, Cliente={customer}, Comp={company_id}")
    
    query = """
        INSERT INTO work_orders
            (company_id, ot_number, customer_name, address, service_type, job_type)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    params = (company_id, ot_number, customer, address, service, job_type)
    
    try:
        # 1. Usamos la función de escritura, pidiendo que retorne el ID
        result = execute_commit_query(query, params, fetchone=True)
        
        if result:
            new_wo_id = result[0] # O result['id']
            return new_wo_id
        else:
            raise Exception("No se pudo crear la Work Order o no se retornó el ID.")
            
    except Exception as e: 
        # 2. La excepción de la BD es re-lanzada,
        #    así que la capturamos para el manejo personalizado.
        if "work_orders_ot_number_key" in str(e): # El nombre de la restricción UNIQUE
            print(f"[DB-WARN] Intento de crear OT duplicada: {ot_number}")
            raise ValueError(f"La Orden de Trabajo '{ot_number}' ya existe.")
        else:
            print(f"Error DB: {e}")
            traceback.print_exc()
            raise e

def get_work_orders_for_export(company_id, filters={}):
    """
    Obtiene TODAS las OTs para exportar (sin paginación).
    (Basado en get_work_orders_filtered_sorted)
    """
    sort_map = {
        'id': "wo.id", 'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
        'job_type': "wo.job_type", 'customer_name': "wo.customer_name", 'address': "wo.address",
        'phase': "wo.phase", 'warehouse_name': "warehouse_name",
        'location_src_path': "location_src_path", 'service_act_number': "service_act_number",
        'attention_date_str': "attention_date_sortable",
        'date_registered': "wo.date_registered"
    }
    order_by_column = "wo.id" # Default sort para exportación
    direction = "DESC"

    base_query = """
    FROM work_orders wo
    LEFT JOIN pickings p_draft ON wo.id = p_draft.work_order_id AND p_draft.state = 'draft' AND p_draft.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_draft ON p_draft.warehouse_id = w_draft.id
    LEFT JOIN locations l_draft ON p_draft.location_src_id = l_draft.id
    LEFT JOIN pickings p_done ON wo.id = p_done.work_order_id AND p_done.state = 'done' AND p_done.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_done ON p_done.warehouse_id = w_done.id
    LEFT JOIN locations l_done ON p_done.location_src_id = l_done.id
    """
    
    select_clause = """
    SELECT
        wo.id, wo.company_id, wo.ot_number, wo.customer_name, wo.address,
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

    params = [company_id]
    where_clauses = ["wo.company_id = %s"]
    filter_map = {
        'id': "wo.id", 'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
        'job_type': "wo.job_type", 'customer_name': "wo.customer_name",
        'address': "wo.address", 'phase': "wo.phase",
        'warehouse_name': "warehouse_name", 'location_src_path': "location_src_path",
        'service_act_number': "service_act_number"
    }

    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value:
            if key == 'phase' or key == 'id':
                where_clauses.append(f"{db_column} = %s")
                params.append(value)
            else:
                where_clauses.append(f"{db_column} ILIKE %s")
                params.append(f"%{value}%")

    where_string = " WHERE " + " AND ".join(where_clauses)
    
    # NO 'LIMIT' OR 'OFFSET'
    final_query = f"{select_clause} {base_query} {where_string} ORDER BY {order_by_column} {direction}"

    return execute_query(final_query, tuple(params), fetchall=True)

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
    try:
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
    (CORREGIDO) Prepara y ejecuta movimientos.
    Distingue entre Operaciones (IN/OUT) y Ajustes/Transferencias (ADJ/INT).
    """
    
    cursor.execute("SELECT p.*, pt.code as type_code FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id WHERE p.id = %s", (picking_id,))
    picking = cursor.fetchone()
    
    if not picking:
        return False, "Albarán no encontrado."

    cursor.execute("SELECT sm.*, p.tracking FROM stock_moves sm JOIN products p ON sm.product_id = p.id WHERE sm.picking_id = %s", (picking_id,))
    all_moves = cursor.fetchall()

    # --- Obtener IDs de ubicaciones virtuales (Solo para IN/OUT) ---
    vendor_loc_id = None
    customer_loc_id = None
    contractor_customer_loc_id = None
    
    # Solo cargamos las ubicaciones virtuales si las necesitamos
    if picking['type_code'] == 'IN' or picking['type_code'] == 'OUT':
        cursor.execute("SELECT id FROM locations WHERE category = 'PROVEEDOR'")
        vendor_loc_id_row = cursor.fetchone()
        vendor_loc_id = vendor_loc_id_row['id'] if vendor_loc_id_row else None
        
        cursor.execute("SELECT id FROM locations WHERE category = 'CLIENTE'")
        customer_loc_id_row = cursor.fetchone()
        customer_loc_id = customer_loc_id_row['id'] if customer_loc_id_row else None

        cursor.execute("SELECT id FROM locations WHERE category = 'CONTRATA CLIENTE'")
        contractor_customer_loc_id_row = cursor.fetchone()
        contractor_customer_loc_id = contractor_customer_loc_id_row['id'] if contractor_customer_loc_id_row else None
    
    processed_moves = []
    for move in all_moves:
        move_dict = dict(move)
        source_loc_id = move_dict['location_src_id']
        dest_loc_id = move_dict['location_dest_id']
        
        # --- ¡INICIO DE LA CORRECCIÓN! ---
        # Esta lógica de reemplazo ahora SOLO se aplica a IN y OUT.
        # ADJ (Ajustes) e INT (Transferencias) usarán las IDs
        # exactas que tienen guardadas.
        
        if picking['type_code'] == 'IN':
            if not vendor_loc_id: raise Exception("Configuración de Ubicación Virtual 'PROVEEDOR' no encontrada.")
            source_loc_id = vendor_loc_id # Sobrescribir origen
            
        elif picking['type_code'] == 'OUT':
            op_rule = get_operation_type_details_by_name(picking['custom_operation_type']) # ¡Usar la función con caché!
            if op_rule:
                if op_rule['destination_location_category'] == 'CLIENTE':
                    if not customer_loc_id: raise Exception("Configuración de Ubicación Virtual 'CLIENTE' no encontrada.")
                    dest_loc_id = customer_loc_id # Sobrescribir destino
                elif op_rule['destination_location_category'] == 'PROVEEDOR':
                    if not vendor_loc_id: raise Exception("Configuración de Ubicación Virtual 'PROVEEDOR' no encontrada.")
                    dest_loc_id = vendor_loc_id # Sobrescribir destino
                elif op_rule['destination_location_category'] == 'CONTRATA CLIENTE':
                    if not contractor_customer_loc_id: raise Exception("Configuración de Ubicación Virtual 'CONTRATA CLIENTE' no encontrada.")
                    dest_loc_id = contractor_customer_loc_id # Sobrescribir destino
            else:
                # Fallback si no hay regla (ej. un 'OUT' antiguo)
                if not customer_loc_id: raise Exception("Configuración de Ubicación Virtual 'CLIENTE' no encontrada.")
                dest_loc_id = customer_loc_id
        
        # (Para 'ADJ' e 'INT', las variables source_loc_id y dest_loc_id
        # permanecen sin cambios, tal como las seleccionó el usuario).
        # --- FIN DE LA CORRECCIÓN ---

        move_dict['final_source_id'] = source_loc_id
        move_dict['final_dest_id'] = dest_loc_id
        processed_moves.append(move_dict)

    # --- Pre-Validación de Stock (Sin cambios) ---
    success, message = _check_stock_with_cursor(cursor, picking_id, picking['type_code'])
    if not success:
        print(f"[DEBUG-STOCK] PRE-VALIDACIÓN FALLIDA: {message}")
        return False, message

    # --- Fase 2: Ejecución de Movimientos (Sin cambios) ---
    print(f"[DEBUG-STOCK] FASE 2: Ejecutando movimientos de stock...")
    processed_serials_in_transaction = set()

    for move in processed_moves:
        product_id = move['product_id']; qty_done = move['quantity_done']
        final_source_id = move['final_source_id']; final_dest_id = move['final_dest_id']
        product_tracking = move['tracking']
        
        # ¡Este log ahora mostrará los IDs correctos para tu ajuste!
        # (Ej: ... de loc_id=5 a loc_id=3)
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
                lot_id = lot_row['id'] if lot_row else create_lot(cursor, product_id, lot_name) # Asumiendo DictRow
                
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
                    # Ajuste negativo (ej. -1): Suma a Scrap (destino), Resta de Averiados (origen)
                    update_stock_quant(cursor, product_id, final_source_id, qty_done, lot_id) # Resta -1
                    update_stock_quant(cursor, product_id, final_dest_id, -qty_done, lot_id) # Suma 1
                else:
                    # Ajuste positivo o movimiento normal
                    update_stock_quant(cursor, product_id, final_source_id, -qty, lot_id) # Resta
                    update_stock_quant(cursor, product_id, final_dest_id, qty, lot_id) # Suma
                
                cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move['id'], lot_id, qty))

    # --- Actualizar estados (Sin cambios) ---
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
    AHORA USA EL POOL y maneja la transacción manualmente con COMMIT/ROLLBACK.
    """
    print(f"[DB-LIQ-FULL] Iniciando FINALIZACIÓN ATÓMICA para WO ID: {wo_id}, LocSrcID: {current_ui_location_id}")
    
    # 1. Obtenemos el pool (como en las otras funciones)
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # Validación previa esencial
    if not current_ui_location_id:
        return False, "Error interno: Falta ID de Ubicación de Origen para determinar el Almacén."
    if user_name is None: user_name = "Sistema"

    # 2. Preparamos la conexión fuera del try
    conn = None 
    try:
        # 3. Obtenemos UNA conexión del pool para toda la transacción
        conn = db_pool.getconn()
        
        # Usamos DictCursor para facilitar el acceso a columnas por nombre
        # (Nota: lo pasamos al crear el cursor, no en la conexión)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            # --- 0. Obtener y validar datos comunes (Warehouse ID) ---
            cursor.execute("SELECT warehouse_id FROM locations WHERE id = %s", (current_ui_location_id,))
            wh_row = cursor.fetchone()
            if not wh_row:
                return False, f"No se pudo encontrar el almacén asociado a la ubicación ID {current_ui_location_id}"
            
            main_warehouse_id = wh_row['warehouse_id']
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
                    'warehouse_id': main_warehouse_id,
                    'location_src_id': current_ui_location_id,
                    'date_attended_db': date_attended_db, 
                    'service_act_number': service_act_number,
                    'lines_data': consumptions
                }
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
                    'warehouse_id': main_warehouse_id,
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

            # 4. Si todo salió bien, ¡COMMIT!
            conn.commit()
            print("[DB-LIQ-FULL] Transacción completada (COMMIT).")
            
            msg_parts = []
            if picking_name_out: msg_parts.append(f"Liquidación {picking_name_out} validada")
            if picking_name_ret: msg_parts.append(f"Retiro {picking_name_ret} validado")
            
            final_msg = ". ".join(msg_parts) + "." if msg_parts else "OT finalizada sin movimientos."
            return True, final_msg

    except Exception as e:
        # 5. ¡CRÍTICO! Si algo falló, hacemos ROLLBACK
        if conn:
            conn.rollback()
            
        print(f"[ERROR-LIQ-FULL] Error en process_full_liquidation (ROLLBACK EJECUTADO): {e}")
        traceback.print_exc()
        return False, f"Error inesperado al liquidar: {e}"
        
    finally:
        # 6. PASE LO QUE PASE, devolvemos la conexión al pool
        if conn:
            db_pool.putconn(conn)

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
    Inserta o actualiza un producto (UPSERT).
    [OPTIMIZADO] Usa ON CONFLICT para atomicidad y eficiencia.
    """
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        init_db_pool()

    conn = None
    
    # Definimos la consulta UPSERT
    # Fíjate en: ON CONFLICT (company_id, sku)
    query = """
        INSERT INTO products (company_id, sku, name, category_id, uom_id, tracking, ownership, standard_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, sku) DO UPDATE SET
            name = EXCLUDED.name,
            category_id = EXCLUDED.category_id,
            uom_id = EXCLUDED.uom_id,
            tracking = EXCLUDED.tracking,
            ownership = EXCLUDED.ownership,
            standard_price = EXCLUDED.standard_price
        RETURNING (xmax = 0) AS inserted;
    """
    
    params = (company_id, sku, name, category_id, uom_id, tracking, ownership, price)

    try:
        conn = db_pool.getconn()
        
        # Usamos el cursor estándar (no necesitamos DictCursor para esto)
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            was_inserted = cursor.fetchone()[0]
            
            conn.commit()
            return "created" if was_inserted else "updated"

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error procesando fila para SKU {sku}: {e}")
        # Es importante lanzar el error para que el contador de errores del importador funcione
        raise e
        
    finally:
        if conn: db_pool.putconn(conn)

def get_picking_details(picking_id, company_id): # <-- 1. ACEPTAR company_id
    query = """
        SELECT p.*, pt.code as type_code 
        FROM pickings p 
        JOIN picking_types pt ON p.picking_type_id = pt.id 
        WHERE p.id = %(picking_id)s -- <-- 2. Usar placeholder con nombre
    """
    # 3. Pasar un diccionario
    p_info = execute_query(query, {"picking_id": picking_id}, fetchone=True) 

    moves_query = """
            SELECT 
                sm.id, pr.name, pr.sku, sm.product_uom_qty, 
                sm.quantity_done, pr.tracking, pr.id as product_id,
                u.name as uom_name,
                sm.price_unit,
                pr.standard_price,
                sm.cost_at_adjustment
            FROM stock_moves sm 
            JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s) -- (Este ya estaba bien)
            LEFT JOIN uom u ON pr.uom_id = u.id
            WHERE sm.picking_id = %(picking_id)s -- <-- 4. Usar placeholder con nombre
        """
    # 5. Pasar un diccionario con AMBOS valores
    moves = execute_query(moves_query, {"picking_id": picking_id, "company_id": company_id}, fetchall=True)
    
    return p_info, moves

def add_stock_move_to_picking(
    picking_id, product_id, qty, loc_src_id, loc_dest_id, company_id, # <-- 1. ACEPTAR company_id
    price_unit=0, partner_id=None
):
    """
    (OPTIMIZADO) Añade una línea y devuelve el objeto completo
    (con JOIN a productos/uom) que la UI necesita.
    """
    
    # 2. TODA la consulta usa placeholders nombrados (%(...))
    query = """
    WITH new_move AS (
        INSERT INTO stock_moves (
            picking_id, product_id, product_uom_qty, quantity_done, 
            location_src_id, location_dest_id, price_unit, partner_id
        ) 
        VALUES (
            %(picking_id)s, %(product_id)s, %(qty)s, %(qty)s, 
            %(loc_src_id)s, %(loc_dest_id)s, %(price_unit)s, %(partner_id)s
        ) 
        RETURNING * -- Devuelve la fila completa de stock_moves
    )
    SELECT 
        sm.id, pr.name, pr.sku, sm.product_uom_qty, 
        sm.quantity_done, pr.tracking, pr.id as product_id,
        u.name as uom_name,
        sm.price_unit,
        pr.standard_price,
        sm.cost_at_adjustment
    FROM new_move sm
    JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s) -- <-- 3. El company_id ahora funciona
    LEFT JOIN uom u ON pr.uom_id = u.id;
    """
    
    # 4. Pasar los parámetros como un DICCIONARIO
    params = {
        "picking_id": picking_id,
        "product_id": product_id,
        "qty": qty,
        "loc_src_id": loc_src_id,
        "loc_dest_id": loc_dest_id,
        "company_id": company_id,
        "price_unit": price_unit,
        "partner_id": partner_id
    }
    
    new_move_object = execute_commit_query(query, params, fetchone=True)
    
    if new_move_object:
        return new_move_object
    else:
        raise Exception("No se pudo crear la línea de stock, no se devolvió objeto.")

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

def create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=False, warehouse_id=None):
    """
    Crea ubicaciones y tipos de operación para un almacén. 
    (Versión PostgreSQL - Corregida para Multi-Compañía)
    """
    
    if not for_existing:
        # --- CORRECCIÓN 1: Unicidad Compuesta en Warehouses ---
        cursor.execute(
            """INSERT INTO warehouses (company_id, name, code, category_id, status) 
               VALUES (%s, %s, %s, %s, 'activo') 
               ON CONFLICT (company_id, code) DO NOTHING 
               RETURNING id""", 
            (company_id, name, code, category_id)
        )
        # -------------------------------------------------------
        warehouse_id_row = cursor.fetchone()
        if not warehouse_id_row:
            print(f" -> Almacén '{code}' ya existía (llamado desde create_warehouse_with_data). Omitiendo.")
            return
        warehouse_id = warehouse_id_row[0]

    # --- 1. Crear Ubicación de Stock Principal ---
    stock_loc_name = f"{code}/Stock"
    # --- CORRECCIÓN 2: Unicidad Compuesta en Locations ---
    cursor.execute(
        """INSERT INTO locations (company_id, name, path, type, category, warehouse_id) 
           VALUES (%s, 'Stock', %s, 'internal', %s, %s) 
           ON CONFLICT (company_id, path) DO NOTHING 
           RETURNING id""",
        (company_id, stock_loc_name, "ALMACEN PRINCIPAL", warehouse_id)
    )
    # -----------------------------------------------------
    stock_loc_id_row = cursor.fetchone()
    if not stock_loc_id_row: # Si ya existía, buscarlo con filtro de compañía
        cursor.execute("SELECT id FROM locations WHERE path = %s AND company_id = %s", (stock_loc_name, company_id))
        stock_loc_id_row = cursor.fetchone()
    stock_loc_id = stock_loc_id_row[0]

    # --- 2. Crear Ubicación de Averiados ---
    damaged_loc_name = f"{code}/Averiados"
    # --- CORRECCIÓN 3: Unicidad Compuesta en Locations ---
    cursor.execute(
        """INSERT INTO locations (company_id, name, path, type, category, warehouse_id) 
           VALUES (%s, 'Averiados', %s, 'internal', %s, %s) 
           ON CONFLICT (company_id, path) DO NOTHING 
           RETURNING id""",
        (company_id, damaged_loc_name, "AVERIADO", warehouse_id)
    )
    # -----------------------------------------------------
    damaged_loc_id_row = cursor.fetchone()
    if not damaged_loc_id_row: # Si ya existía
        cursor.execute("SELECT id FROM locations WHERE path = %s AND company_id = %s", (damaged_loc_name, company_id))
        damaged_loc_id_row = cursor.fetchone()
    damaged_loc_id = damaged_loc_id_row[0]
    
    # --- 3. Obtener IDs de Ubicaciones Virtuales (Filtrado por Compañía) ---
    # Añadimos 'AND company_id = %s' para evitar mezclar datos entre empresas
    cursor.execute("SELECT id FROM locations WHERE category = 'PROVEEDOR' AND company_id = %s LIMIT 1", (company_id,))
    vendor_loc_row = cursor.fetchone()
    if not vendor_loc_row: raise Exception(f"Ubicación virtual 'PROVEEDOR' no encontrada para cia {company_id}.")
    vendor_loc_id = vendor_loc_row[0]

    cursor.execute("SELECT id FROM locations WHERE category = 'CLIENTE' AND company_id = %s LIMIT 1", (company_id,))
    customer_loc_row = cursor.fetchone()
    if not customer_loc_row: raise Exception(f"Ubicación virtual 'CLIENTE' no encontrada para cia {company_id}.")
    customer_loc_id = customer_loc_row[0]
    
    # --- 4. Crear Tipos de Operación ---
    picking_types_to_create = [
        (company_id, f"Recepciones {code}", 'IN', warehouse_id, vendor_loc_id, stock_loc_id),
        (company_id, f"Liquidaciones {code}", 'OUT', warehouse_id, stock_loc_id, customer_loc_id),
        (company_id, f"Despachos {code}", 'INT', warehouse_id, None, None),
        (company_id, f"Retiros {code}", 'RET', warehouse_id, customer_loc_id, damaged_loc_id)
    ]
    # --- CORRECCIÓN 4: Unicidad Compuesta en Picking Types ---
    cursor.executemany("""
        INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON CONFLICT (company_id, name) DO NOTHING
    """, picking_types_to_create)
    # ---------------------------------------------------------
    
    print(f" -> Datos (ubicaciones, tipos op) creados para Almacén '{code}' (ID: {warehouse_id}).")

def create_warehouse(name, code, category_id, company_id, social_reason, ruc, email, phone, address, status):
    """
    Función pública que AHORA incluye el estado.
    [REFACTORIZADO] Usa el pool y maneja la transacción completa (commit/rollback).
    """
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool para toda la transacción
        conn = db_pool.getconn()
        
        # 4. Abrir el cursor
        # (Tu código usa fetchone()[0], así que un cursor estándar está bien)
        with conn.cursor() as cursor:
            
            # 5. Llamar a la función interna (worker)
            # Esta función ejecutará el INSERT y llamará a 
            # create_warehouse_with_data, todo con el MISMO cursor.
            _create_warehouse_with_cursor(
                cursor, name, code, category_id, company_id, 
                social_reason, ruc, email, phone, address, status
            )
        
        # 6. Si todo salió bien, hacer COMMIT
        conn.commit()
        print(f"[DB] Almacén '{name}' y datos asociados creados/verificados exitosamente.")

    except Exception as e:
        # 7. Si algo falló (el INSERT o create_warehouse_with_data),
        #    hacer ROLLBACK
        if conn:
            conn.rollback()
        print(f"[DB-ERROR] Fallo al crear almacén '{name}' (ROLLBACK ejecutado): {e}")
        traceback.print_exc()
        raise e # Re-lanzar la excepción para que la API la maneje

    finally:
        # 8. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

def _create_warehouse_with_cursor(cursor, name, code, category_id, company_id, social_reason, ruc, email, phone, address, status):
    """Función interna que AHORA incluye el estado. (Versión PostgreSQL)"""
    cursor.execute(
        """INSERT INTO warehouses (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (company_id, code) DO NOTHING
           RETURNING id""",
        (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status)
    )
    warehouse_id_row = cursor.fetchone()
    if not warehouse_id_row:
        print(f" -> Almacén '{code}' ya existe en esta compañía. Omitiendo.")
        return 
    warehouse_id = warehouse_id_row[0]
    # Pasamos el cursor que ya tenemos
    create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=True, warehouse_id=warehouse_id)

def update_warehouse(wh_id, name, code, category_id, social_reason, ruc, email, phone, address, status):
    """
    Actualiza un almacén y sus ubicaciones en cascada.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    print(f"[DB-UPDATE-WH] Intentando actualizar Warehouse ID: {wh_id} con nuevo código: {code}")
    new_code_upper = code.strip().upper() if code else None
    if not new_code_upper:
        raise ValueError("El código de almacén no puede estar vacío.")

    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. CAMBIO: Obtener conexión del pool
        conn = db_pool.getconn()
        
        with conn.cursor() as cursor:
            # --- El resto de tu lógica está perfecta ---
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
        # 4. Tu lógica de rollback es correcta y se mantiene
        if conn: conn.rollback()
        print(f"[DB-ERROR] Error al actualizar almacén: {err}")
        if 'warehouses_code_key' in str(err):
            raise ValueError(f"El código '{new_code_upper}' ya está en uso por otro almacén.")
        else:
            traceback.print_exc(); raise err
            
    finally:
        # 5. CAMBIO: Devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

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

def update_picking_header(pid: int, updates: dict):
    """
    [OPTIMIZADO] Actualiza dinámicamente los campos de la cabecera de un picking.
    'updates' es un diccionario.
    """
    if not updates:
        print(f" -> [DB-WARN] update_picking_header llamado sin actualizaciones para PID: {pid}")
        return # No hay nada que hacer

    # Lista de campos permitidos para actualizar
    ALLOWED_FIELDS = [
        'location_src_id', 'location_dest_id', 'partner_ref', 'date_transfer',
        'purchase_order', 'custom_operation_type', 'partner_id',
        'adjustment_reason', 'loss_confirmation', 'notes', 'responsible_user'
    ]

    # Filtrar el diccionario 'updates' para incluir solo campos permitidos
    # y que no sean 'None' (aunque 'None' es válido para resetear)
    fields_to_update = {}
    for key, value in updates.items():
        if key in ALLOWED_FIELDS:
            # Convertir objetos 'date' a string ISO si es necesario
            if isinstance(value, date):
                fields_to_update[key] = value.isoformat()
            else:
                fields_to_update[key] = value
    
    if not fields_to_update:
         print(f" -> [DB-WARN] update_picking_header: No hay campos válidos para actualizar para PID: {pid}")
         return

    # Construir la consulta SQL dinámicamente
    set_clause_parts = []
    params = []
    for key, value in fields_to_update.items():
        set_clause_parts.append(f"{key} = %s")
        params.append(value)
    
    # Añadir el 'pid' al final para la cláusula WHERE
    params.append(pid)
    
    set_clause = ", ".join(set_clause_parts)
    
    query = f"UPDATE pickings SET {set_clause} WHERE id = %s"
    
    print(f" -> [DB] Actualizando Picking ID: {pid}. Campos: {list(fields_to_update.keys())}")
    
    # execute_commit_query maneja la conexión, cursor y commit
    execute_commit_query(query, tuple(params))

def get_picking_type_details(type_id): return execute_query("SELECT * FROM picking_types WHERE id =  %s", (type_id,), fetchone=True)

def update_move_quantity_done(move_id, quantity_done, company_id): # <-- 1. ACEPTAR company_id
    """
    (OPTIMIZADO) Actualiza la cantidad de un move y devuelve
    el objeto completo (con JOIN a productos/uom) que la UI necesita.
    """
    
    # 2. TODA la consulta usa placeholders nombrados (%(...))
    query = """
    WITH updated_move AS (
        UPDATE stock_moves 
        SET product_uom_qty = %(quantity)s, quantity_done = %(quantity)s 
        WHERE id = %(move_id)s
        RETURNING * -- Devuelve la fila actualizada de stock_moves
    )
    SELECT 
        sm.id, pr.name, pr.sku, sm.product_uom_qty, 
        sm.quantity_done, pr.tracking, pr.id as product_id,
        u.name as uom_name,
        sm.price_unit,
        pr.standard_price,
        sm.cost_at_adjustment
    FROM updated_move sm
    JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s) -- <-- 3. El company_id ahora funciona
    LEFT JOIN uom u ON pr.uom_id = u.id;
    """
    # 4. Pasar los parámetros como un DICCIONARIO
    params = {
        "quantity": quantity_done,
        "move_id": move_id,
        "company_id": company_id
    }
    
    updated_move_object = execute_commit_query(query, params, fetchone=True)
    
    return updated_move_object # Devolvemos el DictRow completo

def get_warehouse_categories(company_id: int):
    """Obtiene categorías de almacén FILTRADAS POR COMPAÑÍA."""
    return execute_query(
        "SELECT id, name FROM warehouse_categories WHERE company_id = %s ORDER BY name",
        (company_id,),
        fetchall=True
    )
def get_warehouse_category_details(cat_id):
    return execute_query("SELECT * FROM warehouse_categories WHERE id =  %s", (cat_id,), fetchone=True)

def create_warehouse_category(name: str, company_id: int):
    """Crea una categoría de almacén PARA UNA COMPAÑÍA."""
    try:
        new_item = execute_commit_query(
            "INSERT INTO warehouse_categories (name, company_id) VALUES (%s, %s) RETURNING id, name",
            (name, company_id),
            fetchone=True
        )
        return new_item
    except Exception as e:
        if "warehouse_categories_company_id_name_key" in str(e):
            raise ValueError(f"La categoría '{name}' ya existe para esta compañía.")
        raise e

def update_warehouse_category(category_id: int, name: str, company_id: int):
    """Actualiza una categoría de almacén, verificando la compañía."""
    try:
        updated_item = execute_commit_query(
            "UPDATE warehouse_categories SET name = %s WHERE id = %s AND company_id = %s RETURNING id, name",
            (name, category_id, company_id),
            fetchone=True
        )
        if not updated_item:
            raise ValueError("Categoría no encontrada o no pertenece a esta compañía.")
        return updated_item
    except Exception as e:
        if "warehouse_categories_company_id_name_key" in str(e):
            raise ValueError(f"El nombre '{name}' ya existe (duplicado).")
        raise e

def delete_warehouse_category(category_id: int, company_id: int):
    """Elimina una categoría de almacén, verificando la compañía."""
    try:
        execute_commit_query(
            "DELETE FROM warehouse_categories WHERE id = %s AND company_id = %s",
            (category_id, company_id)
        )
        return True, "Categoría eliminada."
    except Exception as e:
        if "foreign key constraint" in str(e):
            return False, "Error: Esta categoría ya está siendo usada por almacenes."
        return False, f"Error inesperado: {e}"

def inactivate_warehouse(warehouse_id):
    """
    Archiva (desactiva) un almacén.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    print(f"[DB-INACTIVATE-WH] Intentando archivar Warehouse ID: {warehouse_id}")
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        # 4. ¡CRÍTICO! Usar el DictCursor que tu lógica requiere
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # --- VERIFICACIÓN DE SEGURIDAD (HARD BLOCK) ---
            # Tu lógica original se preserva intacta aquí
            cursor.execute(
                """SELECT SUM(sq.quantity) as total_stock
                   FROM stock_quants sq
                   JOIN locations l ON sq.location_id = l.id
                   WHERE l.warehouse_id = %s""",
                (warehouse_id,)
            )
            stock_result = cursor.fetchone()
            
            # Esto funciona gracias al DictCursor
            if stock_result and stock_result['total_stock'] and abs(stock_result['total_stock']) > 0.001:
                stock_total = stock_result['total_stock']
                print(f" -> Bloqueado: Tiene stock ({stock_total}).")
                # El 'finally' se ejecutará y devolverá la conexión.
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
        # 5. ¡MEJORA! Añadimos rollback en caso de error
        if conn:
            conn.rollback()
            
        print(f"Error CRÍTICO en inactivate_warehouse: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al archivar: {e}"
        
    finally:
        # 6. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

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
def get_partner_categories(company_id: int):
    return execute_query(
        "SELECT id, name FROM partner_categories WHERE company_id = %s ORDER BY name",
        (company_id,),
        fetchall=True
    )

def create_partner_category(name: str, company_id: int):
    try:
        new_item = execute_commit_query(
            "INSERT INTO partner_categories (name, company_id) VALUES (%s, %s) RETURNING id, name",
            (name, company_id),
            fetchone=True
        )
        return new_item
    except Exception as e:
        if "partner_categories_company_id_name_key" in str(e):
            raise ValueError(f"La categoría '{name}' ya existe para esta compañía.")
        raise e

def update_partner_category(category_id: int, name: str, company_id: int):
    try:
        updated_item = execute_commit_query(
            "UPDATE partner_categories SET name = %s WHERE id = %s AND company_id = %s RETURNING id, name",
            (name, category_id, company_id),
            fetchone=True
        )
        if not updated_item:
            raise ValueError("Categoría no encontrada o no pertenece a esta compañía.")
        return updated_item
    except Exception as e:
        if "partner_categories_company_id_name_key" in str(e):
            raise ValueError(f"El nombre '{name}' ya existe (duplicado).")
        raise e

def delete_partner_category(category_id: int, company_id: int):
    try:
        execute_commit_query(
            "DELETE FROM partner_categories WHERE id = %s AND company_id = %s",
            (category_id, company_id)
        )
        return True, "Categoría eliminada."
    except Exception as e:
        if "foreign key constraint" in str(e):
            return False, "Error: Esta categoría ya está siendo usada por socios."
        return False, f"Error inesperado: {e}"

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
    """
    Crea un nuevo partner (proveedor/cliente) usando el pool de conexiones.
    Maneja la restricción de nombre único por compañía.
    """
    query = """
        INSERT INTO partners
        (name, category_id, company_id, social_reason, ruc, email, phone, address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    params = (name, category_id, company_id, social_reason, ruc, email, phone, address)
    
    try:
        # 1. Llamamos a la función de escritura, pidiendo que retorne el ID
        result = execute_commit_query(query, params, fetchone=True)
        
        if result:
            new_id = result[0] # O result['id']
            return new_id
        else:
            raise Exception("No se pudo crear el partner o no se retornó el ID.")

    except Exception as e: 
        # 2. La lógica para detectar el duplicado se mantiene
        if "partners_company_id_name_key" in str(e):
            print(f"[DB-WARN] Intento de crear Partner duplicado: {name} para Company ID: {company_id}")
            raise ValueError(f"Ya existe un proveedor/cliente con el nombre '{name}'.")
        else:
            # Re-lanzar cualquier otro error de BD
            raise e

def get_partner_category_id_by_name(name):
    """Busca el ID de una categoría de partner por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM partner_categories WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def update_partner(partner_id, name, category_id, social_reason, ruc, email, phone, address):
    """
    Actualiza los detalles de un partner existente usando el pool de conexiones.
    Maneja la restricción de nombre único por compañía.
    """
    query = """
        UPDATE partners SET
            name = %s,
            category_id = %s,
            social_reason = %s,
            ruc = %s,
            email = %s,
            phone = %s,
            address = %s
        WHERE id = %s
    """
    params = (name, category_id, social_reason, ruc, email, phone, address, partner_id)
    
    try:
        # 1. Llamamos a la función de escritura centralizada.
        # No necesitamos fetchone=True para un UPDATE.
        execute_commit_query(query, params)
        
    except Exception as e: 
        # 2. La excepción de la BD es re-lanzada por execute_commit_query,
        #    así que la capturamos aquí para manejarla.
        
        if "partners_company_id_name_key" in str(e): # Nombre del constraint UNIQUE
            raise ValueError(f"Ya existe otro proveedor/cliente con el nombre '{name}'.")
        else:
            # Re-lanzar cualquier otro error de BD
            raise e

def delete_partner(partner_id):
    """
    Elimina un partner si no está siendo usado en operaciones.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        # 4. Usar un cursor estándar (tu código usa fetchone()[0])
        with conn.cursor() as cursor:

            # --- VERIFICACIÓN DE SEGURIDAD ---
            # (Tu lógica original se preserva)
            cursor.execute("SELECT COUNT(*) FROM pickings WHERE partner_id = %s", (partner_id,))
            picking_count = cursor.fetchone()[0]
            if picking_count > 0:
                # El 'finally' se ejecutará y devolverá la conexión
                return False, f"No se puede eliminar: está asociado a {picking_count} operación(es)."

            # --- Si no se usa, proceder a eliminar ---
            cursor.execute("DELETE FROM partners WHERE id = %s", (partner_id,))
            
            # 5. Confirmar la transacción
            conn.commit()
            return True, "Proveedor/Cliente eliminado correctamente."

    except Exception as e:
        # 6. ¡MEJORA! Añadimos rollback por si el DELETE falla
        if conn:
            conn.rollback()
            
        print(f"Error en delete_partner: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al eliminar: {e}"
        
    finally:
        # 7. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

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

def get_location_by_path(company_id, path):
    """
    Busca una ubicación por su path exacto y company_id.
    """
    query = "SELECT * FROM locations WHERE path = %s AND company_id = %s"
    return execute_query(query, (path, company_id), fetchone=True)

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
    """Obtiene todas las compañías."""
    # Opción A: Seleccionar todo (Recomendado)
    query = "SELECT * FROM companies ORDER BY id"
    
    # Opción B: Seleccionar explícitamente (Si prefieres)
    # query = "SELECT id, name, country_code FROM companies ORDER BY id"
    
    return execute_query(query, fetchall=True)

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
    """
    Crea un nuevo ajuste en borrador.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    print(f"[DB-ADJ] Creando nuevo ajuste en borrador para Cia: {company_id}")
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        # 4. ¡CRÍTICO! Usar el DictCursor que tu lógica requiere
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # --- Tu lógica original se preserva intacta ---
            
            # Leer tipo de operación
            cursor.execute("SELECT id FROM picking_types WHERE code = 'ADJ' AND company_id = %s", (company_id,))
            adj_picking_type = cursor.fetchone()
            if not adj_picking_type:
                raise ValueError("No se encontró un tipo de operación 'ADJ' para esta compañía.")
            pt_id = adj_picking_type['id']

            # Leer ubicación de ajuste
            cursor.execute("SELECT id FROM locations WHERE category = 'AJUSTE' AND company_id = %s", (company_id,))
            adj_loc = cursor.fetchone()
            if not adj_loc:
                raise ValueError("No se encontró la ubicación virtual de 'Ajuste' (category='AJUSTE').")
            adj_loc_id = adj_loc['id']

            # Obtener nombre (esta función ahora usará el pool indirectamente si fue refactorizada)
            new_name = get_next_picking_name(pt_id)
            s_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Escribir el picking
            cursor.execute(
                """INSERT INTO pickings (company_id, name, picking_type_id, location_src_id, location_dest_id, 
                                     scheduled_date, state, responsible_user, custom_operation_type) 
                   VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s) RETURNING id""",
                (company_id, new_name, pt_id, adj_loc_id, adj_loc_id, 
                 s_date, user_name, "Ajuste de Inventario")
            )
            new_picking_id = cursor.fetchone()[0]
            
            # 5. Confirmar la transacción
            conn.commit()
            print(f" -> Ajuste borrador '{new_name}' (ID: {new_picking_id}) creado.")
            return new_picking_id

    except Exception as e:
        # 6. ¡MEJORA! Añadimos rollback en caso de error
        if conn:
            conn.rollback()
        print(f"Error en create_draft_adjustment: {e}")
        traceback.print_exc()
        return None
        
    finally:
        # 7. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

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
    """
    Actualiza múltiples campos de un albarán de forma SEGURA.
    [REFACTORIZADO] Usa el pool y previene Inyección SQL con una whitelist.
    """
    
    if not fields_to_update:
        print("[WARN] update_picking_fields llamado sin campos para actualizar.")
        return # No hay nada que hacer

    # --- 2. VALIDACIÓN (EL PASO DE SEGURIDAD CRÍTICO) ---
    validated_keys = []
    params = []
    
    for key, value in fields_to_update.items():
        if key in ALLOWED_PICKING_FIELDS_TO_UPDATE:
            validated_keys.append(f"{key} = %s") # Construye la parte 'key = %s'
            params.append(value) # Añade el valor a los parámetros
        else:
            # Si una llave no está en la whitelist, RECHAZA la operación
            raise ValueError(f"Intento de actualizar un campo no permitido o desconocido: '{key}'")

    # --- 3. CONSTRUCCIÓN DE LA CONSULTA ---
    set_clause = ", ".join(validated_keys)
    params.append(picking_id) # Añadir el picking_id al final para el WHERE
    
    query = f"UPDATE pickings SET {set_clause} WHERE id = %s"
    
    # --- 4. EJECUCIÓN CON EL POOL ---
    try:
        # Usamos la función de escritura centralizada
        execute_commit_query(query, tuple(params))
        
    except Exception as e: 
        print(f"Error en update_picking_fields para ID {picking_id}: {e}")
        traceback.print_exc()
        # Re-lanzar para que la API lo capture
        raise e

def delete_picking(picking_id):
    """
    Elimina permanentemente un albarán y todos sus movimientos asociados.
    Solo permite la eliminación si el estado es 'draft'.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        # 4. ¡CRÍTICO! Usar el DictCursor que tu lógica requiere
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # --- 1. VERIFICACIÓN DE SEGURIDAD ---
            # (CORREGIDO: .execute() y .fetchone() deben estar separados)
            cursor.execute("SELECT state FROM pickings WHERE id = %s", (picking_id,))
            picking = cursor.fetchone()
            
            if not picking:
                return False, "El albarán no existe."
            
            # (Tu lógica de negocio se preserva)
            if picking['state'] != 'draft':
                return False, "Solo se pueden eliminar permanentemente los albaranes en estado 'Borrador'."

            # --- 2. Si es seguro, proceder con la eliminación ---
            print(f"[DEBUG-DB] Iniciando eliminación del albarán ID: {picking_id}")

            # Obtenemos los IDs de los movimientos asociados
            # (CORREGIDO: .execute() y .fetchall() deben estar separados)
            cursor.execute("SELECT id FROM stock_moves WHERE picking_id = %s", (picking_id,))
            moves = cursor.fetchall()

            if moves:
                move_ids = tuple([move['id'] for move in moves])
                print(f"[DEBUG-DB]... IDs de moves a eliminar: {move_ids}")

                # --- REFACTORIZACIÓN DE SEGURIDAD ---
                # Reemplazamos el f-string por la parametrización estándar de psycopg2
                # para cláusulas IN, que es mucho más segura y limpia.
                cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN %s", (move_ids,))
                print(f"[DEBUG-DB]... stock_move_lines eliminadas.")

                # Eliminar los movimientos de stock (stock_moves)
                cursor.execute("DELETE FROM stock_moves WHERE picking_id = %s", (picking_id,))
                print(f"[DEBUG-DB]... stock_moves eliminados.")

            # Finalmente, eliminar el albarán principal (pickings)
            cursor.execute("DELETE FROM pickings WHERE id = %s", (picking_id,))
            print(f"[DEBUG-DB]... albarán principal eliminado.")
            
            # 5. Confirmar toda la transacción
            conn.commit()
            return True, "Albarán eliminado permanentemente."

    except Exception as e:
        # 6. ¡MEJORA! Añadimos rollback en caso de error
        if conn:
            conn.rollback()
            
        print(f"Error en delete_picking (ROLLBACK ejecutado): {e}")
        traceback.print_exc()
        return False, f"Error inesperado al eliminar: {e}"
        
    finally:
        # 7. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

def update_work_order_fields(wo_id, fields_to_update: dict):
    """
    Actualiza campos específicos de una Orden de Trabajo.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente para retornar rowcount.
    """
    allowed_fields = [
        "customer_name", "address", "warehouse_id", "date_attended",
        "service_type", "job_type", "phase"
    ]
    # --- Tu lógica de whitelist (¡Excelente!) se mantiene ---
    update_dict = {k: v for k, v in fields_to_update.items() if k in allowed_fields and v is not None}

    print(f"[ESPÍA DB UPDATE DICT] Diccionario a actualizar para OT {wo_id}: {update_dict}")
    if not update_dict:
        print(f"[DB-WARN] No se proporcionaron campos válidos para actualizar la OT {wo_id}.")
        return 0

    set_clause = ", ".join([f"{key} = %s" for key in update_dict.keys()])
    params = list(update_dict.values()) + [wo_id]

    # --- REFACTORIZACIÓN A CONTINUACIÓN ---
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        with conn.cursor() as cursor:
            query = f"UPDATE work_orders SET {set_clause} WHERE id = %s"
            
            print(f"[ESPÍA DB SAVE] SQL: {query}")
            print(f"[ESPÍA DB SAVE] Params: {tuple(params)}")

            cursor.execute(query, tuple(params))
            
            # 4. Confirmar la transacción
            conn.commit()
            
            # 5. Retornar el rowcount (la razón por la que hicimos esto manualmente)
            return cursor.rowcount 

    except Exception as e:
        # 6. ¡MEJORA! Añadimos rollback en caso de error
        if conn:
            conn.rollback()
        print(f"Error en update_work_order_fields para OT {wo_id}: {e}")
        traceback.print_exc()
        raise e # Re-lanzar para que la API lo capture
        
    finally:
        # 7. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

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

    base_query += " GROUP BY w.id, w.name, p.id, p.sku, p.name, pc.id, pc.name, u.id, u.name"    
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

def create_or_update_draft_picking(wo_id, company_id, user_name, warehouse_id, date_attended, service_act_number, lines_data: list):
    """
    Guarda el borrador de un picking (Crear/Actualizar).
    [REFACTORIZADO] Usa el pool y maneja la transacción atómica con
    commit/rollback, y corrige errores de psycopg2 (lastrowid, f-strings, execute).
    """
    print(f"[DB-DEBUG] Iniciando create/update BORRADOR para WO ID: {wo_id}")
    if warehouse_id is None:
        return False, "Se requiere seleccionar una Contrata/Almacén para guardar."

    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        # 4. Usar DictCursor (requerido por tu lógica)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # --- A. Buscar Picking Borrador Existente ---
            # (CORREGIDO: .execute() y .fetchone() separados)
            cursor.execute(
                """SELECT p.id, pt.default_location_src_id, pt.default_location_dest_id
                   FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
                   WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = 'OUT'""",
                (wo_id,)
            )
            draft_picking = cursor.fetchone()

            picking_id = None
            loc_src_id = None
            loc_dest_id = None

            if draft_picking:
                picking_id = draft_picking['id']
                print(f" -> Picking borrador encontrado (ID: {picking_id}). Actualizando...")

                # Actualizar cabecera
                cursor.execute(
                    "SELECT id FROM locations WHERE warehouse_id = %s AND type = 'internal' AND name = 'Stock'",
                    (warehouse_id,)
                )
                location_src_row = cursor.fetchone()
                if not location_src_row:
                    raise ValueError(f"No se encontró ubicación interna para el nuevo almacén ID {warehouse_id}.")
                
                loc_src_id = location_src_row['id']
                loc_dest_id = draft_picking['default_location_dest_id']

                cursor.execute(
                    """UPDATE pickings
                       SET warehouse_id = %s, location_src_id = %s, attention_date = %s, service_act_number = %s, responsible_user = %s
                       WHERE id = %s""",
                    (warehouse_id, loc_src_id, date_attended, service_act_number, user_name, picking_id)
                )
                print(f" -> Cabecera del picking {picking_id} actualizada (Wh={warehouse_id}, LocSrc={loc_src_id}).")

            else:
                # --- B. Crear Picking Borrador Nuevo ---
                print(" -> No se encontró picking borrador. Creando uno nuevo...")
                
                cursor.execute(
                    "SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id = %s AND code = 'OUT'",
                    (warehouse_id,)
                )
                picking_type = cursor.fetchone()
                if not picking_type:
                    raise ValueError(f"No se encontró tipo 'OUT' para el almacén ID {warehouse_id}")

                loc_src_id = picking_type['default_location_src_id']
                loc_dest_id = picking_type['default_location_dest_id']
                picking_type_id = picking_type['id']

                picking_name = get_next_picking_name(picking_type_id) # Asumimos que esta función usa el pool

                # (CORREGIDO: Añadido RETURNING id)
                cursor.execute(
                    """INSERT INTO pickings (company_id, name, picking_type_id, warehouse_id, location_src_id, location_dest_id,
                                           state, work_order_id, custom_operation_type,
                                           service_act_number, attention_date, responsible_user)
                       VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (company_id, picking_name, picking_type_id, warehouse_id, loc_src_id, loc_dest_id,
                     wo_id, "Liquidación por OT",
                     service_act_number, date_attended, user_name)
                )
                
                # (CORREGIDO: Reemplazado .lastrowid por .fetchone())
                picking_id_row = cursor.fetchone()
                if not picking_id_row:
                    raise Exception("Fallo al crear el picking (no se retornó ID).")
                picking_id = picking_id_row['id']
                
                print(f" -> Nuevo picking borrador creado (ID: {picking_id}, Nombre: {picking_name}, Wh={warehouse_id}, LocSrc={loc_src_id}).")

            # --- C. Borrar Movimientos Borrador Anteriores ---
            cursor.execute("SELECT id FROM stock_moves WHERE picking_id = %s AND state = 'draft'", (picking_id,))
            old_moves = cursor.fetchall()
            
            if old_moves:
                old_move_ids = tuple([m['id'] for m in old_moves])
                
                # (CORREGIDO: f-string reemplazado por parámetro %s seguro)
                cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN %s", (old_move_ids,))
                cursor.execute("DELETE FROM stock_moves WHERE id IN %s", (old_move_ids,))
                
                print(f" -> {len(old_move_ids)} movimiento(s) borrador anteriores eliminados.")

            # --- D. Crear Nuevos Movimientos Borrador ---
            cursor.execute("SELECT id FROM partners WHERE name = 'Cliente Varios' AND company_id = %s", (company_id,))
            customer_partner_row = cursor.fetchone()
            partner_id_to_set = customer_partner_row['id'] if customer_partner_row else None

            moves_created_count = 0
            for line in lines_data:
                product_id = line['product_id']
                quantity = line['quantity']
                tracking_data = line.get('tracking_data', {})

                # (CORREGIDO: Añadido RETURNING id)
                cursor.execute(
                    """INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done,
                                              location_src_id, location_dest_id, state, partner_id)
                       VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
                       RETURNING id""",
                    (picking_id, product_id, quantity, quantity,
                     loc_src_id, loc_dest_id, partner_id_to_set)
                )
                
                # (CORREGIDO: Reemplazado .lastrowid por .fetchone())
                move_id_row = cursor.fetchone()
                if not move_id_row:
                    raise Exception("Fallo al crear stock_move (no se retornó ID).")
                move_id = move_id_row['id']
                
                moves_created_count += 1

                if tracking_data:
                    for lot_name, qty_done in tracking_data.items():
                         # Asumimos que get_lot_by_name y create_lot usan el cursor
                         lot_row = get_lot_by_name(cursor, product_id, lot_name)
                         lot_id = lot_row['id'] if lot_row else create_lot(cursor, product_id, lot_name)
                         cursor.execute(
                             "INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)",
                             (move_id, lot_id, qty_done)
                         )

            print(f" -> {moves_created_count} nuevos movimientos borrador creados.")
            
            # 5. ¡COMMIT DE TODA LA TRANSACCIÓN!
            conn.commit()
            return True, "Progreso de liquidación guardado."

    except Exception as e:
        # 6. ¡CRÍTICO! ROLLBACK si algo falla
        if conn:
            conn.rollback()
        print(f"[ERROR] en create_or_update_draft_picking (ROLLBACK ejecutado): {e}")
        traceback.print_exc()
        return False, f"Error al guardar borrador: {e}"
        
    finally:
        # 7. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

def get_warehouse_category_id_by_name(name):
    """Busca el ID de una categoría de almacén por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM warehouse_categories WHERE name =  %s", (name,), fetchone=True)
    return result['id'] if result else None

def upsert_warehouse_from_import(company_id, code, name, status, social_reason, ruc, email, phone, address, category_id):
    """
    Inserta o actualiza un almacén desde la importación.
    (Corregido para Multi-Compañía)
    """
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        init_db_pool()

    # 2. Preparar la conexión
    conn = None
    
    # --- ¡CORRECCIÓN AQUÍ! ---
    # Cambiamos ON CONFLICT (code) -> ON CONFLICT (company_id, code)
    query = """
        INSERT INTO warehouses (company_id, code, name, status, social_reason, ruc, email, phone, address, category_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, code) DO UPDATE SET
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
    # -------------------------

    params = (company_id, code, name, status, social_reason, ruc, email, phone, address, category_id)

    try:
        conn = db_pool.getconn()
        
        with conn.cursor() as cursor:
            
            # --- 1. Ejecutar el UPSERT ---
            cursor.execute(query, params)
            was_inserted = cursor.fetchone()[0]
            
            # --- 2. Lógica Condicional ---
            if was_inserted:
                print(f" -> Almacén nuevo '{code}'. Creando datos asociados...")
                # Recuperar ID (Filtro por company_id + code)
                cursor.execute("SELECT id FROM warehouses WHERE code = %s AND company_id = %s", (code, company_id))
                new_wh_id_row = cursor.fetchone()
                
                if not new_wh_id_row:
                    raise Exception(f"No se pudo re-encontrar el almacén '{code}' justo después de crearlo.")
                
                new_wh_id = new_wh_id_row[0]
                
                # Llamar a la función auxiliar (que ya corregimos en el paso anterior)
                create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=True, warehouse_id=new_wh_id)
                print(f" -> Datos asociados creados para el almacén ID {new_wh_id}.")

            # 5. Si todo salió bien, hacer COMMIT
            conn.commit()
            
            return "created" if was_inserted else "updated"

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error procesando fila para CÓDIGO {code} (ROLLBACK ejecutado): {e}")
        traceback.print_exc()
        # Es mejor lanzar la excepción para que la API sepa que falló
        raise e 
        
    finally:
        if conn:
            db_pool.putconn(conn)

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

@functools.lru_cache
def get_operation_type_details_by_name(name):
    """
    Busca los detalles de una regla de operación por su nombre.
    CORREGIDO: Usa TRIM para ignorar espacios en blanco al inicio/final.
    ¡AHORA CON CACHÉ!
    """
    # Este log SÓLO aparecerá cuando la BD es consultada (la 1ra vez)
    print(f"*** [CACHE-MISS] Consultando BD para Regla: {name} ***")
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
    """
    Crea una nueva ubicación.
    [REFACTORIZADO] Usa el pool y maneja la transacción "check-then-write" manualmente.
    """
    
    # --- 1. Lógica de negocio (pre-DB) ---
    if type != 'internal' and warehouse_id is not None:
        warehouse_id = None
    elif type == 'internal' and warehouse_id is None:
        raise ValueError("Se requiere un Almacén Asociado para ubicaciones de tipo 'Interna'.")

    # --- 2. Lógica de BD (Transaccional) ---
    
    # 2.1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2.2. Preparar la conexión
    conn = None
    try:
        # 2.3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        with conn.cursor() as cursor:
            
            # --- 3. Tu lógica transaccional "check-then-write" ---
            
            # 3.1. Validación de Path duplicado DENTRO de la transacción
            cursor.execute(
                "SELECT id FROM locations WHERE path = %s AND company_id = %s",
                (path, company_id)
            )
            if cursor.fetchone():
                # Error amigable proactivo (¡bien hecho!)
                raise ValueError(f"El Path '{path}' ya existe.")
            
            # 3.2. Si está libre, INSERTAR
            query = """
                INSERT INTO locations (company_id, name, path, type, category, warehouse_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            params = (company_id, name, path, type, category, warehouse_id)
            cursor.execute(query, params)
            new_id = cursor.fetchone()[0]
            
            # 4. COMMIT
            conn.commit()
            return new_id
            
    except Exception as e:
        # 5. ¡MEJORA! ROLLBACK si algo falla
        if conn:
            conn.rollback()
        
        # 6. Tu lógica de error reactiva (safety-net) se preserva
        if "locations_path_key" in str(e): 
            raise ValueError(f"El Path '{path}' ya existe.")
            
        # Re-lanzar el error original con más contexto
        raise ValueError(f"No se pudo crear la ubicación. Verifique los datos.") from e
        
    finally:
        # 7. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

def update_location(location_id, company_id, name, path, type, category, warehouse_id):
    """
    Actualiza una ubicación existente, previniendo dejar almacenes sin ubicaciones internas.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    # (El import traceback no es necesario aquí si está al inicio del archivo)
    
    print(f"[DB-UPDATE-LOC] Intentando actualizar Location ID: {location_id}")
    
    # --- 1. Tu lógica de negocio (pre-DB) se mantiene intacta ---
    if type != 'internal' and warehouse_id is not None:
        warehouse_id = None
    elif type == 'internal' and warehouse_id is None:
        raise ValueError("Se requiere un Almacén Asociado para ubicaciones de tipo 'Interna'.")

    # --- 2. REFACTORIZACIÓN DEL POOL ---
    
    # 2.1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2.2. Preparar la conexión
    conn = None
    try:
        # 2.3. CAMBIO: Obtener conexión del pool
        conn = db_pool.getconn()
        
        with conn.cursor() as cursor:
            # --- 3. Tu lógica transaccional se mantiene 100% intacta ---
            
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

    # 4. Tu lógica de error (rollback) se mantiene intacta
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
        # 5. CAMBIO: Devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

def delete_location(location_id):
    """
    Elimina una ubicación si no está en uso.
    [REFACTORIZADO] Usa el pool y maneja la transacción manualmente (commit/rollback).
    """
    print(f"[DB-DELETE-LOC] Intentando eliminar Location ID: {location_id}")
    
    # 1. Obtener el pool
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    # 2. Preparar la conexión
    conn = None
    try:
        # 3. Obtener UNA conexión del pool
        conn = db_pool.getconn()
        
        # 4. ¡CRÍTICO! Usar el DictCursor que tu lógica requiere
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # --- 5. Tu lógica de Verificaciones (se mantiene 100% intacta) ---
            
            # 1. ¿Tiene stock actual? (stock_quants)
            cursor.execute("SELECT SUM(quantity) as total_stock FROM stock_quants WHERE location_id = %s", (location_id,))
            stock_result = cursor.fetchone()
            if stock_result and stock_result['total_stock'] and abs(stock_result['total_stock']) > 0.001:
                print(f" -> Bloqueado: Tiene stock ({stock_result['total_stock']}).")
                # El 'finally' se ejecutará y devolverá la conexión
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
            
            # 6. COMMIT
            conn.commit()

            if rows_affected > 0:
                print(" -> Ubicación eliminada con éxito.")
                return True, "Ubicación eliminada correctamente."
            else:
                print(" -> ADVERTENCIA: No se encontró la ubicación para eliminar.")
                return False, "La ubicación no se encontró (posiblemente ya fue eliminada)."

    except Exception as e:
        # 7. ¡MEJORA! Añadimos rollback en caso de error
        if conn:
            conn.rollback()
            
        print(f"Error CRÍTICO en delete_location: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al intentar eliminar: {e}"
        
    finally:
        # 8. PASE LO QUE PASE, devolver la conexión al pool
        if conn:
            db_pool.putconn(conn)

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

# (En app/database.py)

def get_products_filtered_sorted(company_id, filters={}, sort_by='name', ascending=True, limit=None, offset=None):
    """ 
    Obtiene productos filtrados, ordenados y paginados.
    [CORREGIDO] Asegura que se seleccionen todos los campos requeridos por el schema ProductResponse.
    """
    
    # --- ¡INICIO DE LA CORRECCIÓN! ---
    # Aseguramos que 'p.type' y 'p.company_id' estén en el SELECT
    base_query = """
    SELECT 
        p.id, p.company_id, p.name, p.sku, 
        p.category_id, pc.name as category_name, 
        p.uom_id, u.name as uom_name,
        p.tracking, p.ownership, p.standard_price, 
        p.type  -- Este campo era requerido por el schema
    FROM products p
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    WHERE p.company_id = %s
    """
    # --- FIN DE LA CORRECCIÓN ---
    
    params = [company_id]
    where_clauses = []

    # (El resto de tu lógica de filtros es correcta)
    column_map = {
        'name': "p.name", 'sku': "p.sku", 'category_name': "pc.name",
        'uom_name': "u.name", 'tracking': "p.tracking", 'ownership': "p.ownership"
    }

    for key, value in filters.items():
        if not value: continue
        sql_column = column_map.get(key)
        if not sql_column: continue

        if key in ['name', 'sku']:
            where_clauses.append(f"{sql_column} ILIKE %s") 
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
    """
    Inserta o actualiza un partner (proveedor/cliente) desde la importación.
    [REFACTORIZADO] Usa el helper execute_commit_query para el UPSERT.
    """
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
        # 1. Usamos la función de escritura, pidiendo el 'RETURNING'
        # execute_commit_query se encargará del commit y de devolver la conexión
        result = execute_commit_query(query, params, fetchone=True)
        
        if result:
            was_inserted = result[0] # O result['inserted'] si tu helper usa DictCursor por defecto
            return "created" if was_inserted else "updated"
        else:
            # Esto no debería ocurrir si RETURNING siempre devuelve una fila
            print(f"ADVERTENCIA: UPSERT para Partner '{name}' no retornó un estado.")
            return "error"

    except Exception as e:
        # 2. El helper (execute_commit_query) ya imprimió el error
        #    y ejecutó un rollback.
        #    Solo necesitamos cumplir con el contrato de la función y retornar "error".
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

    for key, value in filters.items():
         if value:
            if key in ["date_transfer_from", "date_transfer_to"]:
                try:
                    db_date = datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
                    operator = ">=" if key == "date_transfer_from" else "<="
                    where_clauses.append(f"p.date_transfer {operator} %s")
                    query_params.append(db_date)
                except ValueError: pass
            
            # Comprobar la clave 'p.state' (antes 'state')
            elif key == 'p.state':
                where_clauses.append("p.state = %s")
                query_params.append(value)
            
            # Comprobar claves 'p.name', 'p.purchase_order', etc.
            elif key in ["p.partner_ref", "p.custom_operation_type", "p.name", "p.purchase_order", "p.responsible_user"]:
                # La SQL ya no necesita añadir 'p.' porque la clave ya lo tiene
                where_clauses.append(f"{key} ILIKE %s")
                query_params.append(f"%{value}%")
            
            # Comprobar 'src_path_display' (antes 'src_path')
            elif key == 'src_path_display':
                where_clauses.append("src_path_display ILIKE %s")
                query_params.append(f"%{value}%")
            
            # Comprobar 'dest_path_display' (antes 'dest_path')
            elif key == 'dest_path_display':
                 where_clauses.append("dest_path_display ILIKE %s")
                 query_params.append(f"%{value}%")
            
            # Comprobar 'w_src.name' (antes 'warehouse_src_name')
            elif key == 'w_src.name':
                 where_clauses.append("w_src.name ILIKE %s")
                 query_params.append(f"%{value}%")
            
            # Comprobar 'w_dest.name' (antes 'warehouse_dest_name')
            elif key == 'w_dest.name':
                 where_clauses.append("w_dest.name ILIKE %s")
                 query_params.append(f"%{value}%")

    where_string = " AND " + " AND ".join(where_clauses) if where_clauses else ""

    # (La consulta principal SQL no cambia, es correcta)
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

def save_adjustment_draft(picking_id, header_data: dict, lines_data: list):
    """
    Guarda el progreso de un borrador de Ajuste.
    Actualiza la cabecera y reemplaza todas las líneas.
    (Versión PostgreSQL - ADAPTADA AL POOL de conexiones como una transacción)
    """
    print(f"[DB-ADJ-SAVE] Guardando borrador para Picking ID: {picking_id}")
    
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    conn = None # Declarar conn aquí para que esté disponible en 'finally'
    try:
        # 1. Obtener UNA conexión del pool para TODA la transacción
        conn = db_pool.getconn() 
        
        # 2. Establecer la fábrica de cursores para esta conexión
        # (Esto es temporal para la conexión, no afecta al pool)
        conn.cursor_factory = psycopg2.extras.DictCursor

        # 3. Iniciar el cursor
        with conn.cursor() as cursor:            
            # 3.1. Actualizar Cabecera
            allowed_header_fields = {"location_src_id", "location_dest_id", "adjustment_reason", "loss_confirmation", "notes"}
            fields_to_update = {k: v for k, v in header_data.items() if k in allowed_header_fields}
            
            if fields_to_update:
                set_clause = ", ".join([f"{key} = %s" for key in fields_to_update.keys()])
                params = list(fields_to_update.values()) + [picking_id]
                cursor.execute(f"UPDATE pickings SET {set_clause} WHERE id = %s", tuple(params))
                print(f" -> Cabecera actualizada: {fields_to_update}")

            # 3.2. Borrar líneas 'draft' antiguas
            cursor.execute("SELECT id FROM stock_moves WHERE picking_id = %s AND state = 'draft'", (picking_id,))
            old_moves = cursor.fetchall() # Devuelve lista de DictRows
            
            if old_moves:
                # CORRECCIÓN: Acceder por 'id' (gracias a DictCursor)
                old_move_ids = tuple([m['id'] for m in old_moves]) 
                
                if old_move_ids: # Evitar 'IN ()' si la tupla está vacía
                    placeholders = ','.join(['%s'] * len(old_move_ids))
                    cursor.execute(f"DELETE FROM stock_move_lines WHERE move_id IN ({placeholders})", old_move_ids)
                    cursor.execute(f"DELETE FROM stock_moves WHERE id IN ({placeholders})", old_move_ids)
                    print(f" -> {len(old_move_ids)} líneas borrador antiguas eliminadas.")

            # 3.3. Crear nuevas líneas 'draft'
            loc_src_id = header_data.get("location_src_id")
            loc_dest_id = header_data.get("location_dest_id")
            if not loc_src_id or not loc_dest_id:
                raise ValueError("Se requieren ubicaciones de origen y destino para guardar líneas.")

            moves_with_tracking = {}
            for line in lines_data: 
                product_id = line.product_id
                quantity = line.quantity
                cost = line.cost_at_adjustment
                tracking_data = line.tracking_data
                
                cursor.execute(
                    """INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done,
                                               location_src_id, location_dest_id, state, cost_at_adjustment)
                       VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
                       RETURNING id""",
                    (picking_id, product_id, quantity, quantity, loc_src_id, loc_dest_id, cost)
                )
                # CORRECCIÓN: Acceder por 'id' (gracias a DictCursor)
                move_id = cursor.fetchone()['id'] 

                if tracking_data:
                    moves_with_tracking[move_id] = tracking_data
                    for lot_name, qty_done in tracking_data.items():
                        
                        # Asumiendo que get_lot_by_name y create_lot toman el 'cursor'
                        lot_row = get_lot_by_name(cursor, product_id, lot_name)
                        
                        # CORRECCIÓN: Acceder por 'id' (gracias a DictCursor)
                        lot_id = lot_row['id'] if lot_row else create_lot(cursor, product_id, lot_name)
                        
                        cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move_id, lot_id, qty_done))
            
            print(f" -> {len(lines_data)} líneas nuevas creadas.")
            # 4. Hacer COMMIT de TODA la transacción
            conn.commit() 
            
            # Restaurar la cursor_factory por defecto (buena práctica)
            conn.cursor_factory = None 
            return True, "Progreso de ajuste guardado.", moves_with_tracking

    except Exception as e:
        print(f"[ERROR] en save_adjustment_draft: {e}")
        traceback.print_exc()
        
        # 5. Hacer ROLLBACK si algo falló
        if conn:
            conn.rollback() 
            
        return False, f"Error al guardar borrador: {e}", None
    
    finally:
        # 6. Devolver la conexión al pool SIEMPRE
        if conn:
            conn.cursor_factory = None # Asegurarse de resetearla
            db_pool.putconn(conn)

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
    location_src_id_override = picking_data.get('location_src_id') if picking_code == 'OUT' else None
    
    date_attended = picking_data['date_attended_db']
    service_act_number = picking_data['service_act_number']
    lines_data = picking_data['lines_data']

    if not warehouse_id:
        raise ValueError(f"Se requiere un warehouse_id para el picking {picking_code}.")

    # --- 1. Buscar si ya existe el borrador ---
    # ... (código sin cambios)
    cursor.execute(
        """SELECT p.id, pt.default_location_src_id, pt.default_location_dest_id
           FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
           WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = %s""",
        (wo_id, picking_code)
    )
    draft_picking = cursor.fetchone()

    picking_id = None
    final_loc_src_id = None
    final_loc_dest_id = None

    # --- 2. Obtener configuración del tipo de operación (NECESARIO SIEMPRE para defaults) ---
    # ... (código sin cambios)
    cursor.execute(
        "SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id = %s AND code = %s",
        (warehouse_id, picking_code)
    )
    picking_type = cursor.fetchone()
    if not picking_type: 
        raise ValueError(f"No está configurado el tipo de operación '{picking_code}' para el almacén ID {warehouse_id}.")

    picking_type_id = picking_type[0]
    default_src_id = picking_type[1]
    default_dest_id = picking_type[2]

    # --- 3. Determinar ubicaciones finales ---
    # ... (código sin cambios)
    if picking_code == 'OUT':
        final_loc_src_id = location_src_id_override or default_src_id
        final_loc_dest_id = default_dest_id
    elif picking_code == 'RET':
        final_loc_src_id = default_src_id
        final_loc_dest_id = default_dest_id
    if not final_loc_src_id or not final_loc_dest_id:
        raise ValueError(f"Configuración incompleta para '{picking_code}' en almacén {warehouse_id}. Faltan ubicaciones por defecto.")

    if draft_picking:
        # --- ACTUALIZAR EXISTENTE ---
        # ... (código sin cambios)
        picking_id = draft_picking[0]
        print(f" -> Picking {picking_code} borrador encontrado (ID: {picking_id}). Actualizando...")
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
    # 4.1. Borrar líneas anteriores (sin cambios)
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
        cursor.execute("SELECT id FROM partners WHERE name = 'Cliente Varios' AND company_id = %s", (company_id,))
        res = cursor.fetchone()
        if res: partner_id_to_set = res[0]

    moves_created = 0
    moves_with_tracking = {}
    # Antes de insertar, validamos que TODOS los productos existan
    product_ids_to_check = [line['product_id'] for line in lines_data if line.get('product_id')]
    print(f"[DEBUG-TRAMPA-PUT] IDs recibidos de Flet: {product_ids_to_check}")
    if product_ids_to_check:
        placeholders_check = ','.join(['%s'] * len(product_ids_to_check))
        cursor.execute(
            f"SELECT id FROM products WHERE company_id = %s AND id IN ({placeholders_check})", 
            (company_id,) + tuple(product_ids_to_check)
        )
        found_products = {row['id'] for row in cursor.fetchall()}
        
        missing_ids = set(product_ids_to_check) - found_products
        if missing_ids:
            # ¡Este es el error que te estaba colapsando!
            raise ValueError(f"Error de datos: Los siguientes ID de producto no existen o no pertenecen a esta compañía: {list(missing_ids)}")

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
            # ... (código de 'if tracking_data' sin cambios) ...
            moves_with_tracking[move_id] = tracking_data
            for serial_name, qty in tracking_data.items():
                cursor.execute("SELECT id FROM stock_lots WHERE product_id = %s AND name = %s", (product_id, serial_name))
                lot_res = cursor.fetchone()
                if lot_res:
                    lot_id = lot_res[0]
                else:
                    cursor.execute("INSERT INTO stock_lots (name, product_id) VALUES (%s, %s) RETURNING id", (serial_name, product_id))
                    lot_id = cursor.fetchone()[0]
                
                cursor.execute(
                    "INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)",
                    (move_id, lot_id, qty)
                )

    print(f" -> {moves_created} líneas nuevas creadas para picking {picking_code} (ID: {picking_id}).")
    return picking_id, moves_with_tracking


def save_liquidation_progress(wo_id, wo_updates: dict, consumo_data: dict, retiro_data: dict, company_id, user_name):
    """
    Actualiza la WO y crea/actualiza AMBOS pickings (OUT y RET) en UNA SOLA TRANSACCIÓN.
    (Versión PostgreSQL - ADAPTADA AL POOL de conexiones como una transacción)
    """
    print(f"[DB-SAVE-LIQ] Iniciando guardado ATÓMICO para WO ID: {wo_id}")
    
    global db_pool
    if not db_pool:
        print("[WARN] El Pool de BD no está inicializado. Intentando inicializar ahora...")
        init_db_pool()
        if not db_pool:
            raise Exception("Fallo crítico: No se pudo inicializar el pool de BD.")

    conn = None # Declarar conn aquí para que esté disponible en 'finally'
    try:
        # 1. Obtener UNA conexión del pool para TODA la transacción
        conn = db_pool.getconn() 
        # 2. Establecer DictCursor para esta conexión
        conn.cursor_factory = psycopg2.extras.DictCursor
        # 3. Iniciar el cursor
        with conn.cursor() as cursor:
            # --- 1. Actualizar Work Order ---
            if wo_updates:
                set_clause = ", ".join([f"{key} = %s" for key in wo_updates.keys()])
                params = list(wo_updates.values()) + [wo_id]
                cursor.execute(f"UPDATE work_orders SET {set_clause} WHERE id = %s", tuple(params))
                print(f" -> work_orders (ID: {wo_id}) actualizada: {wo_updates}")
            # --- 2. Procesar Picking de Consumo (OUT) ---
            if consumo_data:
                # Se asume que _create_or_update_draft_picking_internal está en este
                # mismo archivo y acepta el 'cursor' como primer argumento.
                _create_or_update_draft_picking_internal(
                    cursor, wo_id, 'OUT', consumo_data, company_id, user_name
                )
            # --- 3. Procesar Picking de Retiro (RET) ---
            if retiro_data:
                # Esta función ahora participa en la misma transacción
                _create_or_update_draft_picking_internal(
                    cursor, wo_id, 'RET', retiro_data, company_id, user_name
                )
            else:
                # La lógica de eliminar el borrador 'RET' usa el mismo cursor
                print(f" -> No hay datos de Retiro. Buscando y eliminando picking 'RET' borrador para WO ID: {wo_id}...")
                cursor.execute(
                    """SELECT p.id FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
                       WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = 'RET'""",
                    (wo_id,)
                )
                draft_ret_picking = cursor.fetchone()
                
                if draft_ret_picking:
                    # El código original ya accedía bien por 'id' gracias a DictCursor
                    picking_id_to_delete = draft_ret_picking['id']
                    print(f"    -> Picking 'RET' borrador (ID: {picking_id_to_delete}) encontrado. Eliminando...")
                    
                    cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN (SELECT id FROM stock_moves WHERE picking_id = %s)", (picking_id_to_delete,))
                    cursor.execute("DELETE FROM stock_moves WHERE picking_id = %s", (picking_id_to_delete,))
                    cursor.execute("DELETE FROM pickings WHERE id = %s", (picking_id_to_delete,))
                    print(f"    -> Picking 'RET' borrador (ID: {picking_id_to_delete}) eliminado.")
                else:
                    print("    -> No se encontró ningún picking 'RET' borrador. No se requiere eliminación.")
            # 4. Hacer COMMIT de TODA la transacción
            conn.commit()
            print("[DB-SAVE-LIQ] Transacción completada (COMMIT).")
            # Restaurar la cursor_factory por defecto (buena práctica)
            conn.cursor_factory = None 
            return True, "Progreso de liquidación guardado."
    except Exception as e:
        print(f"[ERROR] en save_liquidation_progress: {e}")
        traceback.print_exc()
        # 5. Hacer ROLLBACK si algo falló
        if conn:
            conn.rollback() 
        return False, f"Error al guardar borrador: {e}"
    finally:
        # 6. Devolver la conexión al pool SIEMPRE
        if conn:
            conn.cursor_factory = None # Asegurarse de resetearla
            db_pool.putconn(conn)


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
    """
    Obtiene todos los usuarios con el nombre de su rol Y 
    la lista de IDs de compañías a las que tienen acceso.
    """
    global db_pool
    if not db_pool: init_db_pool()
    
    conn = None
    try:
        conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor
        
        with conn.cursor() as cursor:
            # 1. Obtener usuarios básicos
            query_users = """
                SELECT u.id, u.username, u.full_name, u.is_active, r.name as role_name, u.role_id
                FROM users u
                LEFT JOIN roles r ON u.role_id = r.id
                ORDER BY u.username
            """
            cursor.execute(query_users)
            users_rows = cursor.fetchall()
            
            final_users = []
            
            # 2. Para cada usuario, obtener sus compañías
            # (Nota: Se podría optimizar con array_agg en SQL, pero esto es más legible y seguro por ahora)
            for u_row in users_rows:
                user_dict = dict(u_row)
                
                query_companies = "SELECT company_id FROM user_companies WHERE user_id = %s"
                cursor.execute(query_companies, (user_dict['id'],))
                company_rows = cursor.fetchall()
                
                # Convertir a una lista simple de enteros [1, 2, 5]
                user_dict['company_ids'] = [row['company_id'] for row in company_rows]
                final_users.append(user_dict)
                print(f"[DEBUG DB] Usuario '{user_dict['username']}' companies: {user_dict['company_ids']}")
                
            return final_users

    except Exception as e:
        print(f"[ERROR DB] get_users_for_admin: {e}")
        raise e
    finally:
        if conn: db_pool.putconn(conn)

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
    """
    Añade o quita un permiso a un rol. 
    (Versión PostgreSQL - ADAPTADA AL POOL de conexiones)
    """
    try:
        if has_permission:
            # Usar execute_commit_query para INSERT
            query = "INSERT INTO role_permissions (role_id, permission_id) VALUES (%s, %s) ON CONFLICT DO NOTHING"
            params = (role_id, permission_id)
            
            execute_commit_query(query, params)
            
            print(f"[DB-RBAC] Permiso {permission_id} AÑADIDO a Rol {role_id}")
        
        else:
            # Usar execute_commit_query para DELETE
            query = "DELETE FROM role_permissions WHERE role_id = %s AND permission_id = %s"
            params = (role_id, permission_id)
            
            execute_commit_query(query, params)
            
            print(f"[DB-RBAC] Permiso {permission_id} QUITADO de Rol {role_id}")
        
        # El 'commit' y el manejo de la conexión ya están dentro de 'execute_commit_query'
        return True, "Permiso actualizado"
    
    except Exception as e:
        # El error ya fue impreso por 'execute_commit_query', 
        # pero lo capturamos aquí para devolver el mensaje de error.
        print(f"[ERROR] en update_role_permissions: {e}")
        return False, str(e)

def create_role(name, description):
    """
    Crea un nuevo rol.
    (Versión PostgreSQL - ADAPTADA AL POOL de conexiones)
    """
    try:
        query = "INSERT INTO roles (name, description) VALUES (%s, %s) RETURNING id"
        params = (name, description)
        result = execute_commit_query(query, params, fetchone=True)
        
        if result:
            new_id = result['id'] # Accedemos al ID por su nombre
            return new_id
        else:
            raise Exception("No se pudo obtener el ID del rol creado.")
            
    except Exception as e:
        if "roles_name_key" in str(e): 
            raise ValueError(f"El rol '{name}' ya existe.")
        else:
            raise e

def update_role(role_id, name, description):
    """
    Actualiza un rol existente.
    (Versión PostgreSQL - ADAPTADA AL POOL de conexiones)
    """
    try:
        query = "UPDATE roles SET name = %s, description = %s WHERE id = %s"
        params = (name, description, role_id)
        execute_commit_query(query, params)

    except Exception as e: 
        if "roles_name_key" in str(e): 
            raise ValueError(f"El rol '{name}' ya existe.")
        else:
            raise e

def create_user(username, plain_password, full_name, role_id, company_ids=None):
    """
    Crea un usuario y asigna sus compañías permitidas.
    """
    if not username or not plain_password or not full_name or not role_id:
        raise ValueError("Todos los campos son obligatorios.")

    global db_pool
    if not db_pool: init_db_pool()
    
    conn = None
    try:
        conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor

        with conn.cursor() as cursor:
            # 1. Hashear contraseña
            hashed_pass = hash_password(plain_password) 
            
            # 2. Insertar Usuario
            query_user = """
                INSERT INTO users (username, hashed_password, full_name, role_id, is_active)
                VALUES (%s, %s, %s, %s, 1) 
                RETURNING id
            """
            cursor.execute(query_user, (username, hashed_pass, full_name, role_id))
            new_id_row = cursor.fetchone()
            
            if not new_id_row:
                raise Exception("No se pudo obtener el ID del nuevo usuario.")
            
            new_user_id = new_id_row['id']

            # 3. Insertar Relación con Compañías (Si hay)
            if company_ids and isinstance(company_ids, list) and len(company_ids) > 0:
                # Preparamos los datos: [(user_id, 1), (user_id, 2), ...]
                values = [(new_user_id, int(c_id)) for c_id in company_ids]
                
                query_rel = "INSERT INTO user_companies (user_id, company_id) VALUES (%s, %s)"
                cursor.executemany(query_rel, values)
                print(f" -> Asignadas {len(values)} compañías al usuario {username}.")

            # 4. Confirmar todo
            conn.commit()
            return new_user_id

    except Exception as e:
        if conn: conn.rollback() # Revertir si falla algo
        if "users_username_key" in str(e): 
            raise ValueError(f"El nombre de usuario '{username}' ya existe.")
        raise e
    finally:
        if conn: db_pool.putconn(conn)

def get_user_by_username(username: str):
    """
    Obtiene los datos básicos de un usuario por su nombre de usuario.
    Usado para buscar el ID del usuario logueado.
    """
    query = "SELECT id, username, role_id, full_name FROM users WHERE username = %s"
    # Usamos fetchone=True para obtener un solo diccionario
    return execute_query(query, (username,), fetchone=True)

def update_user(user_id, full_name, role_id, is_active, new_password=None, company_ids=None):
    """
    Actualiza datos del usuario y sus compañías.
    Si 'company_ids' es None, no se tocan las compañías.
    Si 'company_ids' es [], se le quitan todas las compañías.
    """
    global db_pool
    if not db_pool: init_db_pool()
    
    conn = None



    try:
        conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor

        with conn.cursor() as cursor:
            # 1. Actualizar datos básicos
            if new_password:
                hashed_pass = hash_password(new_password)
                query = """
                    UPDATE users 
                    SET full_name = %s, role_id = %s, is_active = %s, hashed_password = %s 
                    WHERE id = %s
                """
                # --- ¡CORRECCIÓN AQUÍ! Usamos int(is_active) ---
                params = (full_name, role_id, int(is_active), hashed_pass, user_id)
                
                cursor.execute(query, params)
                print(f"[DB-RBAC] Usuario {user_id} actualizado (CON nueva contraseña).")
        
            else:
                query = """
                    UPDATE users 
                    SET full_name = %s, role_id = %s, is_active = %s 
                    WHERE id = %s
                """
                # --- ¡CORRECCIÓN AQUÍ TAMBIÉN! ---
                params = (full_name, role_id, int(is_active), user_id)
                
                cursor.execute(query, params)
                print(f"[DB-RBAC] Usuario {user_id} actualizado (SIN nueva contraseña).")

            # 2. Actualizar Compañías
            if company_ids is not None:
                # ... (resto de tu lógica de compañías, que está bien) ...
                cursor.execute("DELETE FROM user_companies WHERE user_id = %s", (user_id,))
                if company_ids:
                    values = [(user_id, int(c_id)) for c_id in company_ids]
                    query_rel = "INSERT INTO user_companies (user_id, company_id) VALUES (%s, %s)"
                    cursor.executemany(query_rel, values)

            conn.commit()
            return True

    except Exception as e:
        if conn: conn.rollback()
        raise ValueError(f"Error al actualizar usuario: {e}")
    finally:
        if conn: db_pool.putconn(conn)

def get_user_companies(user_id):
    """Devuelve una lista de dicts con las compañías permitidas para el usuario."""
    query = """
        SELECT c.id, c.name, c.country_code
        FROM companies c
        JOIN user_companies uc ON c.id = uc.company_id
        WHERE uc.user_id = %s
        ORDER BY c.name
    """
    return execute_query(query, (user_id,), fetchall=True)


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

def get_picking_ui_details_optimized(picking_id: int, company_id: int):
    """
    [OPTIMIZADO-JSON-CORREGIDO] Obtiene la mayoría de los datos de la UI
    en una sola consulta, filtrando correctamente por compañía.
    """
    
    sql_query = """
    WITH 
    -- 1. Obtener la cabecera y la regla de operación
    picking_data AS (
        SELECT 
            p.*, 
            pt.code as type_code,
            json_build_object(
                'id', op_rule.id,
                'name', op_rule.name,
                'source_location_category', op_rule.source_location_category,
                'destination_location_category', op_rule.destination_location_category
            ) as op_rule
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN operation_types op_rule ON p.custom_operation_type = op_rule.name
        WHERE p.id = %(picking_id)s AND p.company_id = %(company_id)s -- Asegurar que el picking pertenezca a la Cia.
    ),
    
    -- 2. Obtener las líneas (moves) y empaquetarlas en un JSON array
    moves_data AS (
        SELECT 
            json_agg(json_build_object(
                'id', sm.id,
                'product_id', pr.id,
                'name', pr.name,
                'sku', pr.sku,
                'product_uom_qty', sm.product_uom_qty,
                'quantity_done', sm.quantity_done,
                'tracking', pr.tracking,
                'uom_name', u.name,
                'price_unit', sm.price_unit,
                'standard_price', pr.standard_price,
                'cost_at_adjustment', sm.cost_at_adjustment,
                
                -- --- ¡CAMPOS CORREGIDOS! (Para Liquidaciones) ---
                'service_act_number', (SELECT service_act_number FROM picking_data),
                'attention_date', (SELECT attention_date FROM picking_data)
                
            )) as moves
        FROM stock_moves sm
        JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s)
        LEFT JOIN uom u ON pr.uom_id = u.id
        WHERE sm.picking_id = %(picking_id)s
    ),
    
    -- 3. Obtener las series/lotes
    serials_data AS (
        SELECT 
            COALESCE(json_object_agg(
                s.move_id, s.lots
            ), '{}'::json) as serials
        FROM (
            SELECT 
                sml.move_id, 
                json_object_agg(sl.name, sml.qty_done) as lots
            FROM stock_move_lines sml
            JOIN stock_lots sl ON sml.lot_id = sl.id
            WHERE sml.move_id IN (SELECT id FROM stock_moves WHERE picking_id = %(picking_id)s)
            GROUP BY sml.move_id
        ) s
    ),
    
    -- 4. Obtener todos los dropdowns (excepto almacenes)
    dropdowns AS (
        SELECT
            (SELECT json_agg(json_build_object('name', ot.name)) 
             FROM operation_types ot 
             WHERE ot.code = (SELECT type_code FROM picking_data)
            ) AS operation_types,
            
            (SELECT json_agg(p.*) 
             FROM (
                SELECT p.id, p.name 
                FROM partners p
                JOIN partner_categories pc ON p.category_id = pc.id
                WHERE p.company_id = %(company_id)s AND pc.name = 'Proveedor Externo'
                ORDER BY p.name LIMIT 100
             ) p
            ) AS partners_vendor,
            
            (SELECT json_agg(p.*) 
             FROM (
                SELECT p.id, p.name 
                FROM partners p
                JOIN partner_categories pc ON p.category_id = pc.id
                WHERE p.company_id = %(company_id)s AND pc.name = 'Proveedor Cliente'
                ORDER BY p.name LIMIT 100
             ) p
            ) AS partners_customer,
            
            (SELECT json_agg(p_data) 
             FROM (
                SELECT pr.id, pr.name, pr.sku, pr.tracking, pr.ownership, pr.uom_id, pr.standard_price, u.name as uom_name
                FROM products pr
                LEFT JOIN uom u ON pr.uom_id = u.id
                
                -- --- ¡FILTRO DE COMPAÑÍA AÑADIDO! ---
                WHERE pr.company_id = %(company_id)s
                -- --- FIN DE LA CORRECCIÓN ---
                
                ORDER BY pr.name LIMIT 100
             ) p_data
            ) AS all_products
    )
    
    -- 5. Unir todo en un solo JSON
    SELECT json_build_object(
        'picking_data', (SELECT to_jsonb(pd) - 'op_rule' FROM picking_data pd),
        'op_rule', (SELECT op_rule FROM picking_data),
        'moves_data', (SELECT moves FROM moves_data),
        'serials_data', (SELECT serials FROM serials_data),
        'all_products', (SELECT all_products FROM dropdowns),
        'dropdown_options', json_build_object(
            'operation_types', (SELECT operation_types FROM dropdowns),
            'partners_vendor', (SELECT partners_vendor FROM dropdowns),
            'partners_customer', (SELECT partners_customer FROM dropdowns)
        )
    ) AS result;
    """
    
    params = {"picking_id": picking_id, "company_id": company_id}
    
    result_row = execute_query(sql_query, params, fetchone=True)
    
    if result_row and result_row['result']:
        # Corregir listas vacías que SQL devuelve como 'null'
        data = result_row['result']
        
        # ¡Validar que el picking exista y pertenezca a la compañía!
        if data.get('picking_data') is None:
             return None, "Albarán no encontrado o no pertenece a esta compañía."

        if data.get('moves_data') is None: data['moves_data'] = []
        if data.get('all_products') is None: data['all_products'] = []
        if data['dropdown_options'].get('operation_types') is None: data['dropdown_options']['operation_types'] = []
        if data['dropdown_options'].get('partners_vendor') is None: data['dropdown_options']['partners_vendor'] = []
        if data['dropdown_options'].get('partners_customer') is None: data['dropdown_options']['partners_customer'] = []
        return data, None
    
    return None, "No se encontraron datos."


def get_liquidation_details_combo(wo_id: int, company_id: int):
    """
    [COMBO-CORREGIDO] Obtiene TODOS los datos para la UI de detalle de Liquidación,
    asegurando que se seleccionen todos los campos del picking.
    """
    print(f"[DB-COMBO-LIQ] Obteniendo datos para WO ID: {wo_id}")
    
    sql_query = """
    WITH 
    -- --- ¡INICIO DE LA CORRECCIÓN! ---
    -- Seleccionamos p.* para obtener TODOS los campos (incluyendo name, company_id, etc.)
    picking_info AS (
        SELECT 
            p.*, 
            pt.code as type_code
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.work_order_id = %(wo_id)s AND p.state IN ('draft', 'done')
        AND p.company_id = %(company_id)s
        AND pt.code = 'OUT'
        LIMIT 1
    ),
    picking_info_ret AS (
        SELECT 
            p.*, 
            pt.code as type_code
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.work_order_id = %(wo_id)s AND p.state IN ('draft', 'done')
        AND p.company_id = %(company_id)s
        AND pt.code = 'RET'
        LIMIT 1
    ),
    -- --- FIN DE LA CORRECCIÓN ---
    
    moves_consumo AS (
        SELECT json_agg(json_build_object(
            'id', sm.id, 'product_id', pr.id, 'name', pr.name, 'sku', pr.sku,
            'product_uom_qty', sm.product_uom_qty, 'quantity_done', sm.quantity_done,
            'tracking', pr.tracking, 'uom_name', u.name,
            'price_unit', sm.price_unit, 'standard_price', pr.standard_price,
            'cost_at_adjustment', sm.cost_at_adjustment
        )) as moves
        FROM stock_moves sm
        JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s)
        LEFT JOIN uom u ON pr.uom_id = u.id
        WHERE sm.picking_id = (SELECT id FROM picking_info)
    ),
    moves_retiro AS (
        SELECT json_agg(json_build_object(
            'id', sm.id, 'product_id', pr.id, 'name', pr.name, 'sku', pr.sku,
            'product_uom_qty', sm.product_uom_qty, 'quantity_done', sm.quantity_done,
            'tracking', pr.tracking, 'uom_name', u.name,
            'price_unit', sm.price_unit, 'standard_price', pr.standard_price,
            'cost_at_adjustment', sm.cost_at_adjustment
        )) as moves
        FROM stock_moves sm
        JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s)
        LEFT JOIN uom u ON pr.uom_id = u.id
        WHERE sm.picking_id = (SELECT id FROM picking_info_ret)
    ),
    serials_consumo AS (
        SELECT COALESCE(json_object_agg(s.move_id, s.lots), '{}'::json) as serials
        FROM (
            SELECT sml.move_id, json_object_agg(sl.name, sml.qty_done) as lots
            FROM stock_move_lines sml
            JOIN stock_lots sl ON sml.lot_id = sl.id
            WHERE sml.move_id IN (SELECT id FROM stock_moves WHERE picking_id = (SELECT id FROM picking_info))
            GROUP BY sml.move_id
        ) s
    ),
    serials_retiro AS (
        SELECT COALESCE(json_object_agg(s.move_id, s.lots), '{}'::json) as serials
        FROM (
            SELECT sml.move_id, json_object_agg(sl.name, sml.qty_done) as lots
            FROM stock_move_lines sml
            JOIN stock_lots sl ON sml.lot_id = sl.id
            WHERE sml.move_id IN (SELECT id FROM stock_moves WHERE picking_id = (SELECT id FROM picking_info_ret))
            GROUP BY sml.move_id
        ) s
    ),
    dropdowns AS (
        SELECT
            (SELECT json_agg(wh_data) 
             FROM (
                SELECT w.id, w.name, w.code 
                FROM warehouses w
                WHERE w.company_id = %(company_id)s AND w.status = 'activo' ORDER BY w.name
             ) wh_data
            ) AS warehouses,
            
            (SELECT json_agg(loc_data)
             FROM (
                 SELECT l.id, l.name, l.path, l.company_id, l.type 
                 FROM locations l 
                 WHERE l.warehouse_id = (SELECT warehouse_id FROM picking_info)
                 ORDER BY l.path
             ) loc_data
            ) AS locations,
            
            (SELECT json_agg(p_data) 
             FROM (
                SELECT 
                    pr.id, pr.name, pr.sku, pr.tracking, pr.ownership, 
                    pr.uom_id, pr.standard_price, u.name as uom_name,
                    pr.company_id, pr.type, pc.name as category_name
                FROM products pr
                LEFT JOIN uom u ON pr.uom_id = u.id
                LEFT JOIN product_categories pc ON pr.category_id = pc.id
                WHERE pr.company_id = %(company_id)s
                ORDER BY pr.name
             ) p_data
            ) AS all_products
    )
    -- 5. Unir todo
    SELECT json_build_object(
        'wo_data', (SELECT to_jsonb(wo.*) FROM work_orders wo WHERE wo.id = %(wo_id)s AND wo.company_id = %(company_id)s),
        'picking_consumo', (SELECT to_jsonb(pi.*) FROM picking_info pi),
        'moves_consumo', (SELECT moves FROM moves_consumo),
        'serials_consumo', (SELECT serials FROM serials_consumo),
        'picking_retiro', (SELECT to_jsonb(pir.*) FROM picking_info_ret pir),
        'moves_retiro', (SELECT moves FROM moves_retiro),
        'serials_retiro', (SELECT serials FROM serials_retiro),
        'dropdowns', (SELECT to_jsonb(d.*) FROM dropdowns d)
    ) AS result;
    """
    
    params = {"wo_id": wo_id, "company_id": company_id}
    
    try:
        result_row = execute_query(sql_query, params, fetchone=True)
        
        if result_row and result_row['result']:
            data = result_row['result']
            print(f"[DEBUG-TRAMPA-GET] moves_consumo: {data.get('moves_consumo')}")
            if data.get('wo_data') is None:
                 return None, f"OT (ID: {wo_id}) no encontrada o no pertenece a esta compañía ({company_id})."

            if data.get('moves_consumo') is None: data['moves_consumo'] = []
            if data.get('moves_retiro') is None: data['moves_retiro'] = []
            if data.get('dropdowns') is None: data['dropdowns'] = {}
            if data.get('dropdowns', {}).get('all_products') is None: data['dropdowns']['all_products'] = []
            if data.get('dropdowns', {}).get('warehouses') is None: data['dropdowns']['warehouses'] = []
            if data.get('dropdowns', {}).get('locations') is None: data['dropdowns']['locations'] = []
                
            return data, None
        
        return None, "No se encontraron datos."

    except Exception as e:
        traceback.print_exc()
        return None, f"Error interno de base de datos al cargar combo de liquidación: {e}"

def create_company(name: str, country_code: str = "PE"):
    """
    Crea una nueva compañía e inicializa TODA su infraestructura base:
    - Categorías
    - Ubicaciones Virtuales
    - Almacén Principal (y sus ubicaciones)
    - Tipos de Operación (incluyendo ADJ)
    """
    print(f" -> [DB] Iniciando transacción para crear compañía: {name} ({country_code})")

    global db_pool
    if not db_pool: init_db_pool()
    conn = None
    try:
        conn = db_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor

        with conn.cursor() as cursor:
            # 1. Crear Compañía
            cursor.execute(
                "INSERT INTO companies (name, country_code) VALUES (%s, %s) RETURNING *", 
                (name, country_code)
            )
            new_company = cursor.fetchone()
            new_company_id = new_company['id']

            # 2. Categorías de Almacén
            wh_categories = [(new_company_id, "ALMACEN PRINCIPAL"), (new_company_id, "CONTRATISTA")]
            cursor.executemany("INSERT INTO warehouse_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", wh_categories)
            
            # 3. Categorías de Socio
            partner_categories = [(new_company_id, "Proveedor Externo"), (new_company_id, "Proveedor Cliente")]
            cursor.executemany("INSERT INTO partner_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", partner_categories)
            
            # 4. Categoría de Producto
            cursor.execute("INSERT INTO product_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", (new_company_id, 'General'))

            # 5. Crear Ubicaciones Virtuales (¡NUEVO E IMPORTANTE!)
            # Estas son necesarias para que funcionen los tipos de operación
            virtual_locs = [
                (new_company_id, "Proveedores", "PA/Vendors", "vendor", "PROVEEDOR"),
                (new_company_id, "Clientes", "PA/Customers", "customer", "CLIENTE"),
                (new_company_id, "Pérdida de Inventario", "Virtual/Scrap", "inventory", "AJUSTE")
            ]
            cursor.executemany("""
                INSERT INTO locations (company_id, name, path, type, category) 
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (company_id, path) DO NOTHING
            """, virtual_locs)

            # 6. Crear Almacén Principal por Defecto (¡NUEVO!)
            # Buscamos el ID de la categoría que acabamos de crear
            cursor.execute("SELECT id FROM warehouse_categories WHERE company_id = %s AND name = 'ALMACEN PRINCIPAL'", (new_company_id,))
            main_wh_cat = cursor.fetchone()
            
            main_wh_id = None
            if main_wh_cat:
                # Usamos el helper interno para crear el almacén y sus ubicaciones (Stock, Averiados)
                # Generamos un código único simple (ej: PRI-ID)
                wh_code = f"PRI-{new_company_id}" 
                
                # Nota: _create_warehouse_with_cursor DEBE estar definido en este archivo (lo revisamos antes)
                _create_warehouse_with_cursor(
                    cursor, 
                    "Almacén Principal", 
                    wh_code, 
                    main_wh_cat['id'], 
                    new_company_id, 
                    "", "", "", "", "", "activo"
                )
                
                # Recuperar el ID del almacén recién creado para usarlo en el ADJ
                cursor.execute("SELECT id FROM warehouses WHERE company_id = %s AND code = %s", (new_company_id, wh_code))
                wh_row = cursor.fetchone()
                if wh_row: main_wh_id = wh_row['id']

            # 7. Crear Tipo de Operación 'ADJ' (Ajuste de Inventario) (¡CRÍTICO!)
            if main_wh_id:
                # Buscar ubicación de ajuste
                cursor.execute("SELECT id FROM locations WHERE company_id = %s AND category = 'AJUSTE'", (new_company_id,))
                adj_loc_row = cursor.fetchone()
                
                if adj_loc_row:
                    adj_loc_id = adj_loc_row['id']
                    cursor.execute("""
                        INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
                        VALUES (%s, %s, 'ADJ', %s, %s, %s) 
                        ON CONFLICT (company_id, name) DO NOTHING
                    """, (new_company_id, "Ajustes de Inventario", main_wh_id, adj_loc_id, adj_loc_id))
                    print(f" -> Tipo de operación ADJ creado para cia {new_company_id}")

            # 8. Crear Socios por defecto (Varios)
            cursor.execute("SELECT id FROM partner_categories WHERE name = 'Proveedor Cliente' AND company_id = %s", (new_company_id,))
            cat_cl_id = cursor.fetchone()['id']
            cursor.execute("SELECT id FROM partner_categories WHERE name = 'Proveedor Externo' AND company_id = %s", (new_company_id,))
            cat_ex_id = cursor.fetchone()['id']
            
            cursor.execute("INSERT INTO partners (company_id, name, category_id) VALUES (%s, 'Cliente Varios', %s) ON CONFLICT (company_id, name) DO NOTHING", (new_company_id, cat_cl_id))
            cursor.execute("INSERT INTO partners (company_id, name, category_id) VALUES (%s, 'Proveedor Varios', %s) ON CONFLICT (company_id, name) DO NOTHING", (new_company_id, cat_ex_id))

            conn.commit()
            return new_company

    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR DB] Falló crear compañía: {e}")
        if "companies_name_key" in str(e):
            raise ValueError(f"La compañía '{name}' ya existe.")
        raise e 
    finally:
        if conn: db_pool.putconn(conn)

def update_company(company_id: int, name: str, country_code: str):
    """
    Actualiza el nombre y país de una compañía.
    """
    print(f" -> [DB] Actualizando compañía ID {company_id}: {name}, {country_code}")
    
    # --- ¡AQUÍ ESTÁ LA CLAVE! ---
    # Asegúrate de que la query tenga 'country_code = %s'
    query = "UPDATE companies SET name = %s, country_code = %s WHERE id = %s RETURNING *"
    # ----------------------------
    
    try:
        # Y asegúrate de pasar los 3 argumentos en orden
        updated_company = execute_commit_query(query, (name, country_code, company_id), fetchone=True)
        return updated_company
    except Exception as e:
        if "companies_name_key" in str(e):
            raise ValueError(f"El nombre '{name}' ya existe (duplicado).")
        raise e

def delete_company(company_id: int):
    """
    Elimina una compañía y sus datos de configuración asociados.
    Bloquea la eliminación si hay datos operativos (productos, movimientos).
    """
    print(f" -> [DB] Intentando eliminar compañía ID: {company_id}")
    
    global db_pool
    if not db_pool: init_db_pool()
    conn = None
    
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            
            # 1. SEGURIDAD: Verificar si hay datos operativos críticos
            # No queremos borrar una empresa que ya tiene historial de movimientos o productos reales.
            cursor.execute("SELECT COUNT(*) FROM products WHERE company_id = %s", (company_id,))
            if cursor.fetchone()[0] > 0:
                raise ValueError("No se puede eliminar: La compañía tiene PRODUCTOS registrados.")

            cursor.execute("SELECT COUNT(*) FROM pickings WHERE company_id = %s", (company_id,))
            if cursor.fetchone()[0] > 0:
                raise ValueError("No se puede eliminar: La compañía tiene OPERACIONES (Albaranes) registradas.")

            cursor.execute("SELECT COUNT(*) FROM warehouses WHERE company_id = %s", (company_id,))
            if cursor.fetchone()[0] > 0:
                raise ValueError("No se puede eliminar: La compañía tiene ALMACENES registrados.")

            # 2. LIMPIEZA: Borrar datos de configuración (Hijos)
            # Debemos hacerlo en orden para respetar las FKs entre ellos.
            
            print("   -> Eliminando socios (Partners)...")
            cursor.execute("DELETE FROM partners WHERE company_id = %s", (company_id,))
            
            print("   -> Eliminando categorías de producto...")
            cursor.execute("DELETE FROM product_categories WHERE company_id = %s", (company_id,))
            
            print("   -> Eliminando categorías de almacén...")
            cursor.execute("DELETE FROM warehouse_categories WHERE company_id = %s", (company_id,))
            
            print("   -> Eliminando categorías de socio...")
            cursor.execute("DELETE FROM partner_categories WHERE company_id = %s", (company_id,))

            # 3. FINAL: Borrar la compañía (Padre)
            print("   -> Eliminando registro de compañía...")
            cursor.execute("DELETE FROM companies WHERE id = %s", (company_id,))
            
            if cursor.rowcount == 0:
                raise ValueError("La compañía no existe o ya fue eliminada.")

            conn.commit()
            print(f" -> Compañía ID {company_id} eliminada correctamente.")
            return True, "Compañía eliminada."

    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR DB] Falló delete_company: {e}")
        # Convertimos errores de FK en mensajes legibles si se nos pasó algo
        if "ForeignKeyViolation" in str(e):
            raise ValueError("No se puede eliminar: Existen datos relacionados que impiden el borrado.")
        raise e
    finally:
        if conn: db_pool.putconn(conn)
        
def search_storable_products_by_term(company_id: int, search_term: str):
    """
    Busca productos por nombre o SKU usando ILIKE y LIMIT.
    """
    # El '%%' es para la sintaxis de 'LIKE' en SQL
    like_term = f"%{search_term}%" 

    sql_query = """
        SELECT 
            pr.id, pr.name, pr.sku, pr.tracking, pr.ownership, 
            pr.uom_id, pr.standard_price, u.name as uom_name,
            pr.company_id  -- <--- ¡AÑADE ESTA LÍNEA!
        FROM products pr
        LEFT JOIN uom u ON pr.uom_id = u.id
        WHERE 
            pr.company_id = %(company_id)s
            AND pr.type = 'storable'
            AND (pr.name ILIKE %(term)s OR pr.sku ILIKE %(term)s)
        ORDER BY 
            pr.name
        LIMIT 20; 
    """

    params = {"company_id": company_id, "term": like_term}

    results_rows = execute_query(sql_query, params, fetchall=True)
    if not results_rows:
        return []
    return [dict(row) for row in results_rows]  

def process_sku_import_list(company_id: int, raw_text: str):
    """
    Parsea una lista de SKUs y cantidades, y los valida contra la BD.
    Usa UNA sola consulta SQL para máxima eficiencia.
    """
    parsed_lines = {} # Usamos un dict para agrupar SKUs duplicados
    errors = []
    
    lines = raw_text.strip().split('\n')
    
    for i, line in enumerate(lines):
        if not line.strip(): continue
        
        sku = ""
        qty_str = "1"
        
        if '*' in line:
            parts = line.split('*')
            if len(parts) == 2:
                sku = parts[0].strip().lower() # Normalizar a minúsculas
                qty_str = parts[1].strip().replace(',', '.')
            else:
                errors.append(f"Línea {i+1}: Formato inválido (demasiados '*').")
                continue
        else:
            sku = line.strip().lower() # Normalizar a minúsculas
        
        if not sku:
            errors.append(f"Línea {i+1}: SKU vacío.")
            continue
            
        try:
            quantity = float(qty_str)
            if quantity <= 0:
                errors.append(f"Línea {i+1}: Cantidad debe ser positiva.")
                continue
        except (ValueError, TypeError):
            errors.append(f"Línea {i+1}: Cantidad '{qty_str}' no es un número.")
            continue
            
        # Agrupar cantidades por SKU
        parsed_lines[sku] = parsed_lines.get(sku, 0) + quantity

    # --- Validación contra la Base de Datos ---
    
    if not parsed_lines:
        return [], errors # No hay nada que buscar

    # Obtenemos la lista de SKUs únicos a buscar
    skus_to_find = list(parsed_lines.keys())
    
    # ¡Consulta SQL eficiente!
    # Usamos "LOWER(pr.sku) = ANY(%(skus)s)" para usar un array de PostgreSQL
    sql_query = """
        SELECT 
            pr.id, pr.name, pr.sku, pr.tracking, pr.ownership, 
            pr.uom_id, pr.standard_price, u.name as uom_name
        FROM products pr
        LEFT JOIN uom u ON pr.uom_id = u.id
        WHERE 
            pr.company_id = %(company_id)s
            AND pr.type = 'storable'
            AND LOWER(pr.sku) = ANY(%(skus)s); -- ¡Busca en un array!
    """
    params = {"company_id": company_id, "skus": skus_to_find}
    db_results = execute_query(sql_query, params, fetchall=True)
    
# Asegúrate de que db_results sea una lista vacía si es None
    if db_results is None:
        db_results = []

    found_products_map = {row['sku'].lower(): row for row in db_results}
    
    # --- Construir la respuesta ---
    final_found_list = []
    
    for sku_lower, total_qty in parsed_lines.items():
        product_data = found_products_map.get(sku_lower)
        
        if product_data:
            # Producto encontrado. Añadir la cantidad
            # Esto es lo que Flet recibirá en 'found'
            final_found_list.append({
                "product": dict(product_data),  # <--- Esta es la corrección
                "quantity": total_qty
            })
        else:
            # Producto no encontrado
            errors.append(f"SKU '{sku_lower}' no encontrado, inactivo o no almacenable.")

    return final_found_list, errors