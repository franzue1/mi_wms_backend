# backend/app/database/repositories/product_repo.py

import traceback
import psycopg2.extras
from ..core import get_db_connection, return_db_connection, execute_query, execute_commit_query

# --- PRODUCTOS ---

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

def get_product_details_by_sku(sku, company_id):
    """
    Busca los detalles de un producto por su SKU.
    [MEJORA] Búsqueda insensible a mayúsculas (ILIKE + TRIM).
    """
    if not sku: return None
    clean_sku = sku.strip()
    
    query = """
        SELECT p.id, p.name, p.tracking, u.name as uom_name, p.standard_price
        FROM products p
        LEFT JOIN uom u ON p.uom_id = u.id
        WHERE TRIM(p.sku) ILIKE TRIM(%s) AND p.company_id = %s
    """
    return execute_query(query, (clean_sku, company_id), fetchone=True)

def create_product(name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price):
    """
    Crea un nuevo producto.
    NOTA: Los datos deben venir ya normalizados desde el Service Layer.
    """
    query = """
        INSERT INTO products (name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    params = (name, sku, category_id, tracking, uom_id, company_id, ownership, standard_price)

    try:
        result = execute_commit_query(query, params, fetchone=True)
        if result:
            return result[0]
        else:
            raise Exception("No se devolvió ID al crear producto.")

    except Exception as e:
        if "products_sku_key" in str(e):
            raise ValueError(f"El SKU '{sku}' ya existe.")
        else:
            print(f"Error DB [create_product]: {e}")
            traceback.print_exc()
            raise e

def update_product(product_id, name, sku, category_id, tracking, uom_id, ownership, standard_price):
    """
    Actualiza un producto.
    NOTA: Los datos deben venir ya normalizados desde el Service Layer.
    """
    query = """
        UPDATE products
        SET name = %s, sku = %s, category_id = %s, tracking = %s,
            uom_id = %s, ownership = %s, standard_price = %s
        WHERE id = %s
    """
    params = (name, sku, category_id, tracking, uom_id, ownership, standard_price, product_id)

    try:
        execute_commit_query(query, params)

    except Exception as e:
        if "products_sku_key" in str(e):
            raise ValueError(f"El SKU '{sku}' ya existe para otro producto.")
        else:
            raise e

def delete_product(product_id):
    """
    Elimina un producto y sus datos asociados (quants, lots)
    SÓLO SI no tiene movimientos de stock.
    [CORREGIDO] Ya no usa 'global db_pool'. Usa get_db_connection() directo.
    """
    
    # [ELIMINADO] global db_pool y su validación manual
    
    conn = None 
    try:
        # 2. Obtenemos UNA conexión del pool para toda la transacción
        conn = get_db_connection() # <-- Esto ya maneja la inicialización si hace falta
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
            # cursor.execute("DELETE FROM stock_moves WHERE product_id = %s", (product_id,)) # (Ya sabemos que son 0, innecesario)
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
            return_db_connection(conn)

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

def upsert_product_from_import(company_id, sku, name, category_id, uom_id, tracking, ownership, price):
    """
    Inserta o actualiza un producto (UPSERT).
    NOTA: Los datos deben venir ya normalizados desde el Service Layer.
    """
    conn = None

    query = """
        INSERT INTO products (
            company_id, sku, name, category_id, uom_id, tracking, ownership, standard_price
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, sku)
        DO UPDATE SET
            name = EXCLUDED.name,
            category_id = EXCLUDED.category_id,
            uom_id = EXCLUDED.uom_id,
            tracking = EXCLUDED.tracking,
            ownership = EXCLUDED.ownership,
            standard_price = EXCLUDED.standard_price
        RETURNING (xmax = 0) AS inserted
    """
    params = (company_id, sku, name, category_id, uom_id, tracking, ownership, price)

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            was_inserted = result[0] if result else False

            conn.commit()
            return "created" if was_inserted else "updated"

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error procesando fila para SKU {sku}: {e}")
        raise e

    finally:
        if conn:
            return_db_connection(conn)

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

def find_products_by_skus(company_id: int, skus: list):
    """
    Busca productos almacenables por lista de SKUs (lowercase).
    SQL PURO - La lógica de parsing está en el Service Layer.

    Args:
        company_id: ID de la compañía
        skus: Lista de SKUs en lowercase

    Returns:
        Lista de productos encontrados
    """
    if not skus:
        return []

    sql_query = """
        SELECT
            pr.id, pr.name, pr.sku, pr.tracking, pr.ownership,
            pr.uom_id, pr.standard_price, u.name as uom_name
        FROM products pr
        LEFT JOIN uom u ON pr.uom_id = u.id
        WHERE
            pr.company_id = %(company_id)s
            AND pr.type = 'storable'
            AND LOWER(pr.sku) = ANY(%(skus)s)
    """
    params = {"company_id": company_id, "skus": skus}
    results = execute_query(sql_query, params, fetchall=True)

    if results is None:
        return []

    return [dict(row) for row in results]

# --- UNIDADES DE MEDIDA (UOM) - REFACTORIZADO PARA MULTI-COMPAÑÍA ---

def get_uoms(company_id: int):
    """
    [CORREGIDO] Obtiene UdM FILTRADAS por compañía.
    """
    return execute_query(
        "SELECT id, name FROM uom WHERE company_id = %s ORDER BY name", 
        (company_id,), 
        fetchall=True
    )

def create_uom(name: str, company_id: int):
    """
    [CORREGIDO] Crea una UdM asociada a una compañía.
    """
    query = "INSERT INTO uom (name, company_id) VALUES (%s, %s) RETURNING id"
    params = (name, company_id)
    
    try:
        result = execute_commit_query(query, params, fetchone=True)
        if result:
            return result[0]
        else:
            raise Exception("No se pudo crear la UOM.")

    except Exception as e: 
        if "uom_company_id_name_key" in str(e) or "unique constraint" in str(e): 
            raise ValueError(f"La unidad '{name}' ya existe en esta compañía.")
        else:
            raise e

def update_uom(uom_id: int, name: str, company_id: int):
    """
    [CORREGIDO] Actualiza UdM verificando la compañía.
    """
    query = "UPDATE uom SET name = %s WHERE id = %s AND company_id = %s"
    params = (name, uom_id, company_id)
    
    try:
        execute_commit_query(query, params)
    except Exception as e: 
        if "unique constraint" in str(e): 
            raise ValueError(f"La unidad '{name}' ya existe.")
        else:
            raise e

def delete_uom(uom_id: int, company_id: int):
    """
    [CORREGIDO] Elimina UdM verificando la compañía.
    """
    query = "DELETE FROM uom WHERE id = %s AND company_id = %s"
    params = (uom_id, company_id)
    
    try:
        execute_commit_query(query, params)
        return True, "Unidad de medida eliminada."
    except Exception as e:
        if "violates foreign key constraint" in str(e):
            return False, "No se puede eliminar: Está asignada a productos."
        print(f"[DB-ERROR] delete_uom: {e}")
        return False, f"Error al eliminar: {e}"
    
def get_uom_id_by_name(name, company_id):
    """
    [CORREGIDO] Busca ID por nombre Y compañía.
    """
    if not name or not name.strip(): return None
    result = execute_query(
        "SELECT id FROM uom WHERE name = %s AND company_id = %s", 
        (name, company_id), 
        fetchone=True
    )
    return result['id'] if result else None

# --- CATEGORÍAS (Ya estaban bien, solo las incluimos para completar) ---

def get_product_categories(company_id: int):
    return execute_query(
        "SELECT id, name FROM product_categories WHERE company_id = %s ORDER BY name", 
        (company_id,), fetchall=True
    )

def create_product_category(name: str, company_id: int):
    try:
        return execute_commit_query(
            "INSERT INTO product_categories (name, company_id) VALUES (%s, %s) RETURNING id, name",
            (name, company_id), fetchone=True
        )
    except Exception as e:
        if "unique constraint" in str(e):
            raise ValueError(f"La categoría '{name}' ya existe.")
        raise e

def update_product_category(category_id: int, name: str, company_id: int):
    try:
        updated = execute_commit_query(
            "UPDATE product_categories SET name = %s WHERE id = %s AND company_id = %s RETURNING id, name",
            (name, category_id, company_id), fetchone=True
        )
        if not updated: raise ValueError("No encontrada o sin permisos.")
        return updated
    except Exception as e:
        if "unique constraint" in str(e):
            raise ValueError(f"El nombre '{name}' ya existe.")
        raise e
 
def delete_product_category(category_id: int, company_id: int):
    try:
        execute_commit_query(
            "DELETE FROM product_categories WHERE id = %s AND company_id = %s",
            (category_id, company_id)
        )
        return True, "Categoría eliminada."
    except Exception as e:
        if "foreign key constraint" in str(e):
            return False, "Error: Esta categoría tiene productos asociados."
        return False, f"Error: {e}"

def get_category_id_by_name(name, company_id): # Agregado company_id por consistencia
    if not name: return None
    res = execute_query("SELECT id FROM product_categories WHERE name = %s AND company_id = %s", (name, company_id), fetchone=True)
    return res['id'] if res else None