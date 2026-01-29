#backend/app/database/repositories/security_repo.py

import hashlib
import traceback
import psycopg2.extras
from ..core import get_db_connection, return_db_connection, execute_query, execute_commit_query
from ..utils import _create_warehouse_with_cursor

def hash_password(password):
    """Genera un hash SHA-256 para la contraseña."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def check_password(hashed_password, plain_password):
    """Verifica si la contraseña coincide con el hash."""
    return hashed_password == hash_password(plain_password)

def validate_user_and_get_permissions(username, plain_password):
    """
    Valida al usuario y devuelve sus detalles (INCLUYENDO EL NOMBRE DEL ROL).
    """
    try:
        query = """
            SELECT u.*, r.name as role_name 
            FROM users u
            LEFT JOIN roles r ON u.role_id = r.id
            WHERE u.username = %s
        """
        user = execute_query(query, (username,), fetchone=True)
        # --------------------------------------------------------------------
        
        if not user:
            print(f"[AUTH] Fallo: Usuario '{username}' no encontrado.")
            return None, None 
        
        if not user['is_active']:
            print(f"[AUTH] Fallo: Usuario '{username}' está inactivo.")
            return None, None 

        # Verificar contraseña
        if not check_password(user['hashed_password'], plain_password):
            print(f"[AUTH] Fallo: Contraseña incorrecta para '{username}'.")
            return None, None 
        
        # ¡Éxito!
        print(f"[AUTH] Éxito: Usuario '{username}' (Rol: {user['role_name']}) validado.")
        user_data = dict(user) # Ahora user_data tiene 'role_name'
        role_id = user_data['role_id']
        
        permissions = execute_query(
            """SELECT p.key
               FROM role_permissions rp
               JOIN permissions p ON rp.permission_id = p.id
               WHERE rp.role_id = %s""",
            (role_id,),
            fetchall=True
        )
        
        permissions_set = {perm['key'] for perm in permissions}
        
        return user_data, permissions_set

    except Exception as e:
        print(f"[ERROR] en validate_user_and_get_permissions: {e}")
        traceback.print_exc()
        return None, None

def create_user(username, plain_password, full_name, role_id, company_ids=None, warehouse_ids=None):
    """
    Crea un usuario y asigna sus compañías permitidas.
    """
    if not username or not plain_password or not full_name or not role_id:
        raise ValueError("Todos los campos son obligatorios.")

    conn = None
    try:
        conn = get_db_connection()
        conn.cursor_factory = psycopg2.extras.DictCursor

        with conn.cursor() as cursor:
            # 1. Hashear contraseña
            hashed_pass = hash_password(plain_password)

            # 2. Insertar Usuario
            # [CAMBIO] Insertamos must_change_password = TRUE implícitamente o explícitamente
            query_user = """
                INSERT INTO users (username, hashed_password, full_name, role_id, is_active, must_change_password)
                VALUES (%s, %s, %s, %s, 1, TRUE) 
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

            # 4. [NUEVO] Insertar Relación con Almacenes
            if warehouse_ids and isinstance(warehouse_ids, list) and len(warehouse_ids) > 0:
                values = [(new_user_id, int(w_id)) for w_id in warehouse_ids]
                query_wh = "INSERT INTO user_warehouses (user_id, warehouse_id) VALUES (%s, %s)"
                cursor.executemany(query_wh, values)
                print(f" -> Asignados {len(values)} almacenes al usuario {username}.")

            # 5. Confirmar todo
            conn.commit()
            return new_user_id

    except Exception as e:
        if conn: conn.rollback() # Revertir si falla algo
        if "users_username_key" in str(e): 
            raise ValueError(f"El nombre de usuario '{username}' ya existe.")
        raise e
    finally:
        if conn: return_db_connection(conn)

def update_user(user_id, full_name, role_id, is_active, new_password=None, company_ids=None, warehouse_ids=None):
    """
    Actualiza datos del usuario y sus compañías.
    Si 'company_ids' es None, no se tocan las compañías.
    Si 'company_ids' es [], se le quitan todas las compañías.
    """

    conn = None

    try:
        conn = get_db_connection()
        conn.cursor_factory = psycopg2.extras.DictCursor

        with conn.cursor() as cursor:
            # 1. Actualizar datos básicos
            if new_password:
                hashed_pass = hash_password(new_password)
                # [CAMBIO] Si se cambia la password aquí (reset administrativo), activamos must_change_password = TRUE
                query = """
                    UPDATE users 
                    SET full_name = %s, role_id = %s, is_active = %s, hashed_password = %s, must_change_password = TRUE 
                    WHERE id = %s
                """
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
                cursor.execute("DELETE FROM user_companies WHERE user_id = %s", (user_id,))
                if company_ids:
                    values = [(user_id, int(c_id)) for c_id in company_ids]
                    query_rel = "INSERT INTO user_companies (user_id, company_id) VALUES (%s, %s)"
                    cursor.executemany(query_rel, values)

            # 3. [NUEVO] Actualizar Almacenes
            if warehouse_ids is not None:
                cursor.execute("DELETE FROM user_warehouses WHERE user_id = %s", (user_id,))
                if warehouse_ids:
                    values = [(user_id, int(w_id)) for w_id in warehouse_ids]
                    query_wh = "INSERT INTO user_warehouses (user_id, warehouse_id) VALUES (%s, %s)"
                    cursor.executemany(query_wh, values)

            conn.commit()
            return True

    except Exception as e:
        if conn: conn.rollback()
        raise ValueError(f"Error al actualizar usuario: {e}")
    finally:
        if conn: return_db_connection(conn)


def get_users_for_admin():
    """
    Obtiene todos los usuarios con el nombre de su rol Y 
    la lista de IDs de compañías a las que tienen acceso.
    """
    conn = None
    try:
        conn = get_db_connection() # <-- Correcto: Usa el helper
        
        # RECOMENDACIÓN: Usa DictCursor explícitamente aquí para poder hacer dict(row)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. Obtener usuarios básicos
            query_users = """
                SELECT u.id, u.username, u.full_name, u.is_active,u.must_change_password, r.name as role_name, u.role_id
                FROM users u
                LEFT JOIN roles r ON u.role_id = r.id
                ORDER BY u.username
            """
            cursor.execute(query_users)
            users_rows = cursor.fetchall()
            
            final_users = []
            for u_row in users_rows:
                user_dict = dict(u_row)
                
                # Traer Compañías
                cursor.execute("SELECT company_id FROM user_companies WHERE user_id = %s", (user_dict['id'],))
                user_dict['company_ids'] = [row['company_id'] for row in cursor.fetchall()]
                
                # [NUEVO] Traer Almacenes
                cursor.execute("SELECT warehouse_id FROM user_warehouses WHERE user_id = %s", (user_dict['id'],))
                user_dict['warehouse_ids'] = [row['warehouse_id'] for row in cursor.fetchall()]
                
                final_users.append(user_dict)
            
            return final_users

    except Exception as e:
        print(f"[ERROR DB] get_users_for_admin: {e}")
        raise e
    finally:
        # --- ¡CORRECCIÓN CRÍTICA AQUÍ! ---
        if conn:
            return_db_connection(conn) # <-- Usa el helper, NO db_pool.putconn

def get_user_by_username(username: str):
    """
    Obtiene los datos básicos de un usuario por su nombre de usuario.
    Usado para buscar el ID del usuario logueado.
    """
    query = "SELECT id, username, role_id, full_name FROM users WHERE username = %s"
    # Usamos fetchone=True para obtener un solo diccionario
    return execute_query(query, (username,), fetchone=True)

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

def create_company(name: str, country_code: str = "PE", creator_user_id: int = None):
    """
    Crea una nueva compañía e inicializa su infraestructura base.
    [ACTUALIZADO] Incluye la categoría 'CUADRILLA INTERNA'.
    """
    print(f" -> [DB] Iniciando creación de compañía: {name} ({country_code}) por Usuario ID: {creator_user_id}")

    conn = None
    try:
        conn = get_db_connection()
        conn.cursor_factory = psycopg2.extras.DictCursor

        with conn.cursor() as cursor:
            # 1. Crear Compañía
            cursor.execute(
                "INSERT INTO companies (name, country_code) VALUES (%s, %s) RETURNING *", 
                (name, country_code)
            )
            new_company = cursor.fetchone()
            new_company_id = new_company['id']

            # --- VINCULAR AL CREADOR ---
            if creator_user_id:
                cursor.execute(
                    "INSERT INTO user_companies (user_id, company_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (creator_user_id, new_company_id)
                )
                print(f" -> [DB] Compañía {new_company_id} asignada al creador {creator_user_id}.")
            # -----------------------------------

            # 2. Categorías de Almacén [ACTUALIZADO]
            # Ahora incluimos 'CUADRILLA INTERNA' para que el módulo de empleados funcione desde el día 1.
            wh_categories = [
                (new_company_id, "ALMACEN PRINCIPAL"), 
                (new_company_id, "CONTRATISTA"),
                (new_company_id, "CUADRILLA INTERNA") # <--- ¡NUEVO!
            ]
            cursor.executemany("INSERT INTO warehouse_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", wh_categories)
            
            # 3. Categorías de Socio
            partner_categories = [(new_company_id, "Proveedor Externo"), (new_company_id, "Proveedor Cliente")]
            cursor.executemany("INSERT INTO partner_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", partner_categories)
            
            # 4. Categoría de Producto
            cursor.execute("INSERT INTO product_categories (company_id, name) VALUES (%s, %s) ON CONFLICT (company_id, name) DO NOTHING", (new_company_id, 'General'))

            # 5. Ubicaciones Virtuales
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

            # 6. Almacén Principal (Lógica existente correcta)
            cursor.execute("SELECT id FROM warehouse_categories WHERE company_id = %s AND name = 'ALMACEN PRINCIPAL'", (new_company_id,))
            main_wh_cat = cursor.fetchone()
            
            main_wh_id = None
            if main_wh_cat:
                wh_code = f"PRI-{new_company_id}" 
                # Asumo que _create_warehouse_with_cursor está importado desde utils
                _create_warehouse_with_cursor(
                    cursor, "Almacén Principal", wh_code, main_wh_cat['id'], new_company_id, 
                    "", "", "", "", "", "activo"
                )
                cursor.execute("SELECT id FROM warehouses WHERE company_id = %s AND code = %s", (new_company_id, wh_code))
                wh_row = cursor.fetchone()
                if wh_row: main_wh_id = wh_row['id']

            # 7. Tipo de Operación ADJ
            if main_wh_id:
                cursor.execute("SELECT id FROM locations WHERE company_id = %s AND category = 'AJUSTE'", (new_company_id,))
                adj_loc_row = cursor.fetchone()
                
                if adj_loc_row:
                    adj_loc_id = adj_loc_row['id']
                    cursor.execute("""
                        INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
                        VALUES (%s, %s, 'ADJ', %s, %s, %s) 
                        ON CONFLICT (company_id, name) DO NOTHING
                    """, (new_company_id, "Ajustes de Inventario", main_wh_id, adj_loc_id, adj_loc_id))

            # 8. Socios por defecto
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
        if conn: return_db_connection(conn)

def update_company(company_id: int, name: str, country_code: str):
    """
    Actualiza el nombre y país de una compañía.
    """
    print(f" -> [DB] Actualizando compañía ID {company_id}: {name}, {country_code}")
    
    # Asegúrate de que la query tenga 'country_code = %s'
    query = "UPDATE companies SET name = %s, country_code = %s WHERE id = %s RETURNING *"
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

    conn = None
    
    try:
        conn = get_db_connection()
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
        if conn: return_db_connection(conn)
 
def get_companies():
    """Obtiene todas las compañías."""
    # Opción A: Seleccionar todo (Recomendado)
    query = "SELECT * FROM companies ORDER BY id"
    
    # Opción B: Seleccionar explícitamente (Si prefieres)
    # query = "SELECT id, name, country_code FROM companies ORDER BY id"
    
    return execute_query(query, fetchall=True)

def change_own_password(user_id: int, new_password: str):
    """
    Permite al usuario cambiar su propia contraseña y DESACTIVA el flag de cambio obligatorio.
    """
    conn = None
    try:
        conn = get_db_connection()
        hashed_pass = hash_password(new_password)
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE users 
                SET hashed_password = %s, must_change_password = FALSE 
                WHERE id = %s
            """, (hashed_pass, user_id))
            conn.commit()
            return True
    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        if conn: return_db_connection(conn)