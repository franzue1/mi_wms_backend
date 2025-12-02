import psycopg2
import psycopg2.extras
import traceback
from ..core import get_db_connection, return_db_connection, execute_query, execute_commit_query
# Importamos lógica de creación desde el schema para no duplicar código
from ..utils import create_warehouse_with_data, _create_warehouse_with_cursor

# --- CATEGORÍAS DE ALMACÉN ---

def get_warehouse_categories(company_id: int):
    """Obtiene categorías de almacén FILTRADAS POR COMPAÑÍA."""
    return execute_query(
        "SELECT id, name FROM warehouse_categories WHERE company_id = %s ORDER BY name",
        (company_id,),
        fetchall=True
    )

def get_warehouse_category_details(cat_id):
    return execute_query("SELECT * FROM warehouse_categories WHERE id = %s", (cat_id,), fetchone=True)

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

def get_warehouse_category_id_by_name(name):
    """Busca el ID de una categoría de almacén por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM warehouse_categories WHERE name = %s", (name,), fetchone=True)
    return result['id'] if result else None

# --- ALMACENES ---

def get_warehouses(company_id):
    """
    Función de compatibilidad. Obtiene todos los almacenes activos.
    """
    return get_warehouses_filtered_sorted(
        company_id,
        filters={'status': 'activo'},
        sort_by='name',
        ascending=True
    )

def get_warehouse_details_by_id(warehouse_id: int):
    """
    Obtiene los detalles completos de un solo almacén por su ID.
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

def create_warehouse(company_id, name, code, category_id, social_reason=None, ruc=None, email=None, phone=None, address=None):
    """
    Crea un nuevo almacén desde el formulario de la UI.
    """
    print(f" -> [DB] Creando almacén: {name} ({code}) para Cía {company_id}")
    
    # ELIMINADO: global db_pool, init_db_pool
    
    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor() as cursor:
            # Validar si ya existe el código en esta compañía
            cursor.execute("SELECT id FROM warehouses WHERE code = %s AND company_id = %s", (code, company_id))
            if cursor.fetchone():
                raise ValueError(f"El código de almacén '{code}' ya existe en esta compañía.")

            # Insertar Almacén
            cursor.execute(
                """INSERT INTO warehouses (company_id, name, code, category_id, social_reason, ruc, email, phone, address, status) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'activo') 
                   RETURNING id""",
                (company_id, name, code, category_id, social_reason, ruc, email, phone, address)
            )
            new_wh_row = cursor.fetchone()
            if not new_wh_row: raise Exception("Error al insertar almacén.")
            new_wh_id = new_wh_row[0]
            
            # Llamar al auxiliar (importado de schema)
            create_warehouse_with_data(
                cursor, 
                name, 
                code, 
                company_id, 
                category_id, 
                for_existing=True, 
                warehouse_id=new_wh_id
            )
            conn.commit()
            return new_wh_id

    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR DB] create_warehouse: {e}")
        if "warehouses_code_company_key" in str(e) or "unique constraint" in str(e):
            raise ValueError(f"El código '{code}' ya existe.")
        raise e
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def update_warehouse(wh_id, name, code, category_id, social_reason, ruc, email, phone, address, status):
    """
    Actualiza un almacén y sus ubicaciones en cascada.
    """
    print(f"[DB-UPDATE-WH] Intentando actualizar Warehouse ID: {wh_id} con nuevo código: {code}")
    new_code_upper = code.strip().upper() if code else None
    if not new_code_upper:
        raise ValueError("El código de almacén no puede estar vacío.")

    # ELIMINADO: global db_pool...

    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
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
        if conn: return_db_connection(conn) # <-- USAR HELPER

def inactivate_warehouse(warehouse_id):
    """
    Archiva (desactiva) un almacén.
    """
    print(f"[DB-INACTIVATE-WH] Intentando archivar Warehouse ID: {warehouse_id}")
    
    # ELIMINADO: global db_pool...

    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # --- VERIFICACIÓN DE SEGURIDAD ---
            cursor.execute(
                """SELECT SUM(sq.quantity) as total_stock
                   FROM stock_quants sq
                   JOIN locations l ON sq.location_id = l.id
                   WHERE l.warehouse_id = %s""",
                (warehouse_id,)
            )
            stock_result = cursor.fetchone()
            
            if stock_result and stock_result['total_stock'] and abs(stock_result['total_stock']) > 0.001:
                stock_total = stock_result['total_stock']
                print(f" -> Bloqueado: Tiene stock ({stock_total}).")
                return False, f"No se puede archivar: El almacén aún tiene stock ({stock_total} unidades)."

            # --- FASE 2: ARCHIVAR ---
            print(" -> Almacén limpio (sin stock). Procediendo a archivar...")
            cursor.execute(
                "UPDATE warehouses SET status = 'inactivo' WHERE id = %s AND status = 'activo'",
                (warehouse_id,)
            )
            rows_affected = cursor.rowcount
            
            conn.commit()

            if rows_affected > 0:
                print(" -> Almacén archivado con éxito.")
                return True, "Almacén archivado correctamente."
            else:
                print(" -> El almacén ya estaba inactivo o no se encontró.")
                return False, "El almacén no se pudo archivar (quizás ya estaba inactivo)."

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error CRÍTICO en inactivate_warehouse: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al archivar: {e}"
        
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def get_warehouse_id_by_name(name):
    """Busca el ID de un almacén por su nombre exacto."""
    if not name: return None
    result = execute_query("SELECT id FROM warehouses WHERE name = %s", (name,), fetchone=True)
    return result['id'] if result else None

def get_warehouse_id_for_location(location_id):
    """Obtiene el warehouse_id de una ubicación interna específica."""
    if not location_id: return None
    result = execute_query(
        "SELECT warehouse_id FROM locations WHERE id = %s AND type = 'internal'",
        (location_id,), fetchone=True
    )
    return result['warehouse_id'] if result else None

def validate_warehouse_names(names_to_check):
    """
    Recibe una lista de nombres de almacenes y devuelve una lista de aquellos que NO existen.
    """
    if not names_to_check: return []
    placeholders = ', '.join('%s' for name in names_to_check)
    query = f"SELECT name FROM warehouses WHERE name IN ({placeholders})"
    results = execute_query(query, tuple(names_to_check), fetchall=True)
    existing_names = {row['name'] for row in results}
    non_existent_names = [name for name in names_to_check if name not in existing_names]
    return non_existent_names

def get_warehouse_code(warehouse_id):
    """Obtiene el código ('code') de un almacén por su ID."""
    if not warehouse_id: return None
    result = execute_query("SELECT code FROM warehouses WHERE id = %s", (warehouse_id,), fetchone=True)
    return result['code'] if result else None

def get_warehouses_simple(company_id):
    """Devuelve una lista simple de almacenes (ID, Nombre, Código) para dropdowns."""
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
    """
    if not category_names: return []
    placeholders = ', '.join('%s' for _ in category_names)
    
    query = f"""
        SELECT w.id, w.name
        FROM warehouses w
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE w.company_id = %s 
          AND w.status = 'activo' 
          AND wc.name IN ({placeholders})
        ORDER BY w.name
    """
    params = tuple([company_id] + category_names)
    results = execute_query(query, params, fetchall=True)
    return results if results else []

def get_warehouses_by_category(company_id, category_name):
    """
    Obtiene las UBICACIONES de stock de los almacenes que pertenecen a una categoría.
    """
    query = """
        SELECT l.id, w.name 
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE l.type = 'internal' AND w.company_id = %s AND wc.name = %s
        ORDER BY w.name
    """
    return execute_query(query, (company_id, category_name), fetchall=True)

def upsert_warehouse_from_import(company_id, code, name, status, social_reason, ruc, email, phone, address, category_id):
    """
    Inserta o actualiza un almacén desde la importación.
    """
    # ELIMINADO: global db_pool...

    conn = None
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
    params = (company_id, code, name, status, social_reason, ruc, email, phone, address, category_id)

    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            was_inserted = cursor.fetchone()[0]
            
            if was_inserted:
                print(f" -> Almacén nuevo '{code}'. Creando datos asociados...")
                cursor.execute("SELECT id FROM warehouses WHERE code = %s AND company_id = %s", (code, company_id))
                new_wh_id_row = cursor.fetchone()
                
                if not new_wh_id_row:
                    raise Exception(f"No se pudo re-encontrar el almacén '{code}' justo después de crearlo.")
                
                new_wh_id = new_wh_id_row[0]
                create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=True, warehouse_id=new_wh_id)
                print(f" -> Datos asociados creados para el almacén ID {new_wh_id}.")

            conn.commit()
            return "created" if was_inserted else "updated"

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error procesando fila para CÓDIGO {code} (ROLLBACK ejecutado): {e}")
        traceback.print_exc()
        raise e 
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def get_warehouses_filtered_sorted(company_id, filters={}, sort_by='name', ascending=True, limit=None, offset=None):
    """Obtiene almacenes filtrados y ordenados."""
    base_query = """
    SELECT
        w.id, w.company_id, w.name, w.code,
        w.social_reason, w.ruc, w.email, w.phone, w.address,
        w.status,
        w.category_id,
        wc.name as category_name
    FROM warehouses w
    LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
    WHERE w.company_id = %s
    """
    params = [company_id]
    where_clauses = []

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
             where_clauses.append(f"{sql_column} ILIKE %s")
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
    
    return execute_query(base_query, tuple(params), fetchall=True)

def get_warehouses_count(company_id, filters={}):
    """Cuenta almacenes."""
    base_query = """
    SELECT COUNT(w.id) as total_count
    FROM warehouses w
    LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
    WHERE w.company_id = %s
    """
    params = [company_id]
    where_clauses = []
    
    # Misma lógica de filtrado...
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
            where_clauses.append(f"{sql_column} LIKE %s")
            params.append(f"%{value}%")
        elif key in ['status', 'category_name']:
            where_clauses.append(f"{sql_column} = %s")
            params.append(value)
            
    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)
        
    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

# --- UBICACIONES ---

def get_all_locations(): return execute_query("SELECT id, path FROM locations ORDER BY path", fetchall=True)

def get_location_path(location_id):
    loc = execute_query("SELECT path FROM locations WHERE id = %s", (location_id,), fetchone=True)
    return loc['path'] if loc else ""

def get_location_by_path(company_id, path):
    return execute_query("SELECT * FROM locations WHERE path = %s AND company_id = %s", (path, company_id), fetchone=True)

def get_internal_locations(company_id):
    query = """
        SELECT l.id, l.path FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.type = 'internal' AND w.company_id = %s
        ORDER BY l.path
    """
    return execute_query(query, (company_id,), fetchall=True)

def get_locations_by_warehouse(warehouse_id):
    if not warehouse_id: return []
    try: wh_id_int = int(warehouse_id)
    except (ValueError, TypeError): return []
    
    query = """
        SELECT l.id, l.path, l.warehouse_id 
        FROM locations l
        WHERE l.warehouse_id = %s AND l.type = 'internal'
        ORDER BY l.path
    """
    return execute_query(query, (wh_id_int,), fetchall=True)

def get_location_id_by_warehouse_name(name, company_id):
    if not name: return None
    query = """
        SELECT l.id, wc.name as category_name
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        LEFT JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE w.name = %s AND w.company_id = %s AND l.type = 'internal' AND l.name = 'Stock'
    """
    result = execute_query(query, (name, company_id), fetchone=True)
    return result

def get_locations_detailed(company_id):
    query = """
        SELECT
            l.id, l.name, l.path, l.type, l.category,
            l.warehouse_id as location_wh_id,
            w.id as warehouse_actual_id,
            w.name as warehouse_name,
            w.company_id as warehouse_company_id,
            w.status as warehouse_status
        FROM locations l
        LEFT JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.company_id = %s
        ORDER BY l.path
    """
    return execute_query(query, (company_id,), fetchall=True)

def create_location(company_id, name, path, type, category, warehouse_id):
    """
    Crea una nueva ubicación.
    """
    if type != 'internal' and warehouse_id is not None: warehouse_id = None
    elif type == 'internal' and warehouse_id is None:
        raise ValueError("Se requiere un Almacén Asociado para ubicaciones de tipo 'Interna'.")

    # ELIMINADO: global db_pool...

    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor() as cursor:
            # Validación de Path duplicado
            cursor.execute("SELECT id FROM locations WHERE path = %s AND company_id = %s", (path, company_id))
            if cursor.fetchone():
                raise ValueError(f"El Path '{path}' ya existe.")
            
            # Insertar
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
            
    except Exception as e:
        if conn: conn.rollback()
        if "locations_path_key" in str(e): 
            raise ValueError(f"El Path '{path}' ya existe.")
        raise ValueError(f"No se pudo crear la ubicación. Verifique los datos.") from e
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def update_location(location_id, company_id, name, path, type, category, warehouse_id):
    """
    Actualiza una ubicación existente.
    """
    print(f"[DB-UPDATE-LOC] Intentando actualizar Location ID: {location_id}")
    if type != 'internal' and warehouse_id is not None: warehouse_id = None
    elif type == 'internal' and warehouse_id is None: raise ValueError("Se requiere un Almacén Asociado.")

    # ELIMINADO: global db_pool...

    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor() as cursor:
            # Datos Actuales
            cursor.execute("SELECT type, warehouse_id FROM locations WHERE id = %s AND company_id = %s", (location_id, company_id))
            current_loc = cursor.fetchone()
            if not current_loc: raise ValueError(f"No se encontró la ubicación con ID {location_id}.")
            
            current_type = current_loc[0]
            current_warehouse_id = current_loc[1]

            # VALIDACIÓN ANTI-HUÉRFANOS
            is_changing_from_internal = (current_type == 'internal' and type != 'internal')
            is_changing_internal_wh = (current_type == 'internal' and type == 'internal' and current_warehouse_id != warehouse_id)

            if (is_changing_from_internal or is_changing_internal_wh) and current_warehouse_id is not None:
                cursor.execute(
                    "SELECT COUNT(*) FROM locations WHERE warehouse_id = %s AND type = 'internal' AND id != %s",
                    (current_warehouse_id, location_id)
                )
                other_internal_count = cursor.fetchone()[0]
                if other_internal_count == 0:
                    raise ValueError(f"No se puede modificar: es la última ubicación interna del almacén original (ID: {current_warehouse_id}).")

            # Validación Path único
            cursor.execute("SELECT id FROM locations WHERE path = %s AND company_id = %s AND id != %s", (path, company_id, location_id))
            existing = cursor.fetchone()
            if existing: raise ValueError(f"El Path '{path}' ya está en uso por otra ubicación.")

            # Ejecutar UPDATE
            cursor.execute(
                """UPDATE locations SET
                   name = %s, path = %s, type = %s, category = %s, warehouse_id = %s
                   WHERE id = %s AND company_id = %s""",
                (name, path, type, category, warehouse_id, location_id, company_id)
            )
            conn.commit()
            return True

    except ValueError as err:
        if conn: conn.rollback()
        print(f"[DB-ERROR] Error al actualizar ubicación: {err}")
        raise err
    except Exception as ex:
        if conn: conn.rollback()
        print(f"Error CRÍTICO en update_location: {ex}")
        traceback.print_exc()
        raise RuntimeError(f"Error inesperado al actualizar ubicación: {ex}")
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def delete_location(location_id):
    """
    Elimina una ubicación si no está en uso.
    """
    print(f"[DB-DELETE-LOC] Intentando eliminar Location ID: {location_id}")
    
    # ELIMINADO: global db_pool...

    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. Check stock
            cursor.execute("SELECT SUM(quantity) as total_stock FROM stock_quants WHERE location_id = %s", (location_id,))
            stock_result = cursor.fetchone()
            if stock_result and stock_result['total_stock'] and abs(stock_result['total_stock']) > 0.001:
                return False, f"No se puede eliminar: La ubicación tiene stock ({stock_result['total_stock']} unidades)."

            # 2. Check default in Picking Types
            cursor.execute("SELECT COUNT(*) as count FROM picking_types WHERE default_location_src_id = %s OR default_location_dest_id = %s", (location_id, location_id))
            if cursor.fetchone()['count'] > 0:
                return False, "No se puede eliminar: Es ubicación por defecto en tipos de operación."

            # 3. Check moves history
            cursor.execute("SELECT COUNT(*) as count FROM stock_moves WHERE location_src_id = %s OR location_dest_id = %s", (location_id, location_id))
            if cursor.fetchone()['count'] > 0:
                return False, "No se puede eliminar: La ubicación tiene historial de movimientos."

            # DELETE
            cursor.execute("DELETE FROM locations WHERE id = %s", (location_id,))
            rows_affected = cursor.rowcount
            
            conn.commit()

            if rows_affected > 0:
                return True, "Ubicación eliminada correctamente."
            else:
                return False, "La ubicación no se encontró."

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error CRÍTICO en delete_location: {e}")
        return False, f"Error inesperado al intentar eliminar: {e}"
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def get_location_name_details(location_id):
    if not location_id: return None
    query = """
        SELECT l.name as location_name, w.name as warehouse_name, l.warehouse_id, l.type as location_type
        FROM locations l LEFT JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.id = %s
    """
    result = execute_query(query, (location_id,), fetchone=True)
    return dict(result) if result else None

def get_location_details_by_names(company_id, warehouse_name, location_name):
    if not company_id or not warehouse_name or not location_name: return None
    query = """
        SELECT l.id, l.warehouse_id
        FROM locations l
        JOIN warehouses w ON l.warehouse_id = w.id
        WHERE l.company_id = %s AND w.name = %s AND l.name = %s AND l.type = 'internal'
    """
    result = execute_query(query, (company_id, warehouse_name, location_name), fetchone=True)
    return dict(result) if result else None

def get_location_details_by_id(location_id: int):
    query = """
    SELECT l.id, l.company_id, l.name, l.path, l.type, l.category, l.warehouse_id, w.name as warehouse_name
    FROM locations l LEFT JOIN warehouses w ON l.warehouse_id = w.id
    WHERE l.id = %s
    """
    return execute_query(query, (location_id,), fetchone=True)

def get_locations_filtered_sorted(company_id, filters={}, sort_by='path', ascending=True, limit=None, offset=None):
    base_query = """
    SELECT 
        l.id, l.company_id, l.name, l.path, l.type, l.category,
        l.warehouse_id, w.name as warehouse_name, w.status as warehouse_status
    FROM locations l
    LEFT JOIN warehouses w ON l.warehouse_id = w.id
    WHERE l.company_id = %s
    """
    params = [company_id]
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
            where_clauses.append(f"{sql_column} ILIKE %s"); params.append(f"%{value}%")
        else:
            where_clauses.append(f"{sql_column} = %s"); params.append(value)

    if where_clauses: base_query += " AND " + " AND ".join(where_clauses)
    
    sort_map = {'path': 'l.path', 'type': 'l.type', 'warehouse_name': 'w.name', 'id': 'l.id'}
    order_by_col = sort_map.get(sort_by, "l.id")
    direction = "ASC" if ascending else "DESC"
    base_query += f" ORDER BY {order_by_col} {direction}"

    if limit is not None and offset is not None:
        base_query += " LIMIT %s OFFSET %s"; params.extend([limit, offset])

    return execute_query(base_query, tuple(params), fetchall=True)

def get_locations_count(company_id, filters={}):
    base_query = """
    SELECT COUNT(l.id) as total_count
    FROM locations l
    LEFT JOIN warehouses w ON l.warehouse_id = w.id
    WHERE l.company_id = %s
    """
    params = [company_id]
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
        if key == 'path' or key == 'warehouse_name': where_clauses.append(f"{sql_column} LIKE %s"); params.append(f"%{value}%")
        else: where_clauses.append(f"{sql_column} = %s"); params.append(value)

    if where_clauses: base_query += " AND " + " AND ".join(where_clauses)
    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0