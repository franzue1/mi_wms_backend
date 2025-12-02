import traceback
from ..core import (
    get_db_connection, 
    return_db_connection, 
    execute_query, 
    execute_commit_query
)

# --- CATEGORÍAS DE PARTNER ---

def get_partner_categories(company_id: int):
    """Obtiene categorías de partner FILTRADAS POR COMPAÑÍA."""
    return execute_query(
        "SELECT id, name FROM partner_categories WHERE company_id = %s ORDER BY name",
        (company_id,),
        fetchall=True
    )

def create_partner_category(name: str, company_id: int):
    """Crea una categoría de partner PARA UNA COMPAÑÍA."""
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
    """Actualiza una categoría de partner, verificando la compañía."""
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
    """Elimina una categoría de partner, verificando la compañía."""
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

def get_partner_category_id_by_name(name):
    """Busca el ID de una categoría de partner por su nombre exacto."""
    if not name or not name.strip():
        return None
    result = execute_query("SELECT id FROM partner_categories WHERE name = %s", (name,), fetchone=True)
    return result['id'] if result else None

# --- PARTNERS (SOCIOS/CLIENTES/PROVEEDORES) ---

def get_partners(company_id, category_name=None):
    """Obtiene todos los partners de una compañía."""
    query = """
        SELECT
            p.id, p.name, p.social_reason, p.ruc, p.email, p.phone, p.address,
            pc.name as category_name,
            p.category_id
        FROM partners p
        LEFT JOIN partner_categories pc ON p.category_id = pc.id
        WHERE p.company_id = %s
    """
    params = (company_id,)
    if category_name:
        query += " AND pc.name = %s"
        params += (category_name,)
    query += " ORDER BY p.name"
    return execute_query(query, params, fetchall=True)

def get_partner_details(partner_id):
    """Obtiene todos los campos de un partner."""
    return execute_query("SELECT * FROM partners WHERE id = %s", (partner_id,), fetchone=True)

def get_partner_details_by_id(partner_id: int):
    """
    Obtiene los detalles completos de un solo socio (partner) por su ID,
    incluyendo el nombre de la categoría.
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

def get_partner_name(partner_id):
    """Obtiene el nombre de un partner por su ID."""
    if not partner_id:
        return None
    result = execute_query("SELECT name FROM partners WHERE id = %s", (partner_id,), fetchone=True)
    return result['name'] if result else None

def get_partner_id_by_name(name, company_id):
    """Busca el ID y el NOMBRE DE CATEGORÍA de un partner por su nombre exacto."""
    if not name: return None
    query = """
        SELECT p.id, pc.name as category_name
        FROM partners p
        LEFT JOIN partner_categories pc ON p.category_id = pc.id
        WHERE p.name = %s AND p.company_id = %s
    """
    result = execute_query(query, (name, company_id), fetchone=True)
    return result

def create_partner(name, category_id, company_id, social_reason, ruc, email, phone, address):
    """
    Crea un nuevo partner usando el helper de commit.
    """
    query = """
        INSERT INTO partners
        (name, category_id, company_id, social_reason, ruc, email, phone, address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    params = (name, category_id, company_id, social_reason, ruc, email, phone, address)
    
    try:
        result = execute_commit_query(query, params, fetchone=True)
        if result:
            return result[0]
        else:
            raise Exception("No se pudo crear el partner o no se retornó el ID.")

    except Exception as e: 
        if "partners_company_id_name_key" in str(e):
            print(f"[DB-WARN] Intento de crear Partner duplicado: {name} para Company ID: {company_id}")
            raise ValueError(f"Ya existe un proveedor/cliente con el nombre '{name}'.")
        else:
            raise e

def update_partner(partner_id, name, category_id, social_reason, ruc, email, phone, address):
    """
    Actualiza los detalles de un partner existente.
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
        execute_commit_query(query, params)
    except Exception as e: 
        if "partners_company_id_name_key" in str(e):
            raise ValueError(f"Ya existe otro proveedor/cliente con el nombre '{name}'.")
        else:
            raise e

def delete_partner(partner_id):
    """
    Elimina un partner si no está siendo usado en operaciones.
    [REFACTORIZADO] Usa transacción manual con helpers.
    """
    # ELIMINADO: global db_pool...

    conn = None
    try:
        conn = get_db_connection() # <-- USAR HELPER
        
        with conn.cursor() as cursor: # Cursor estándar está bien aquí
            # --- VERIFICACIÓN DE SEGURIDAD ---
            cursor.execute("SELECT COUNT(*) FROM pickings WHERE partner_id = %s", (partner_id,))
            picking_count = cursor.fetchone()[0]
            
            if picking_count > 0:
                return False, f"No se puede eliminar: está asociado a {picking_count} operación(es)."

            # --- Si no se usa, proceder a eliminar ---
            cursor.execute("DELETE FROM partners WHERE id = %s", (partner_id,))
            
            conn.commit()
            return True, "Proveedor/Cliente eliminado correctamente."

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error en delete_partner: {e}")
        traceback.print_exc()
        return False, f"Error inesperado al eliminar: {e}"
        
    finally:
        if conn: return_db_connection(conn) # <-- USAR HELPER

def upsert_partner_from_import(company_id, name, category_id, ruc, social_reason, address, email, phone):
    """
    Inserta o actualiza un partner desde la importación.
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
        result = execute_commit_query(query, params, fetchone=True)
        if result:
            was_inserted = result[0]
            return "created" if was_inserted else "updated"
        else:
            print(f"ADVERTENCIA: UPSERT para Partner '{name}' no retornó un estado.")
            return "error"

    except Exception as e:
        print(f"Error procesando fila para Partner '{name}': {e}")
        return "error"

def get_partners_filtered_sorted(company_id, filters={}, sort_by='name', ascending=True, limit=None, offset=None):
    """ Obtiene proveedores/clientes filtrados, ordenados y paginados. """
    base_query = """
    SELECT p.id, p.company_id, p.name, p.social_reason, p.ruc, p.email, p.phone, p.address,
           pc.name as category_name, p.category_id
    FROM partners p
    LEFT JOIN partner_categories pc ON p.category_id = pc.id
    WHERE p.company_id = %s
    """
    params = [company_id]
    where_clauses = []

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
            where_clauses.append(f"{sql_column} ILIKE %s")
            params.append(f"%{value}%")
        else: # Para el dropdown de categoría
            where_clauses.append(f"{sql_column} = %s")
            params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

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
    WHERE p.company_id = %s
    """
    params = [company_id]
    where_clauses = []
    
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
            where_clauses.append(f"{sql_column} LIKE %s"); params.append(f"%{value}%")
        else:
            where_clauses.append(f"{sql_column} = %s"); params.append(value)

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    result = execute_query(base_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0