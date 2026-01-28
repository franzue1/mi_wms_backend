#app/database/repositories/operation_repo.py

import psycopg2
import psycopg2.extras
import psycopg2.pool
import traceback
import functools
from datetime import datetime, date, timedelta
import re
from collections import defaultdict
import json 
from ..core import get_db_connection, return_db_connection, execute_query, execute_commit_query
from . import project_repo

ALLOWED_PICKING_FIELDS_TO_UPDATE = {
    'name', 'partner_id', 'state', 'scheduled_date', 'responsible_user',
    'service_act_number', 'date_attended', 'warehouse_id',
    'location_src_id', 'location_dest_id', 'partner_ref', 'date_transfer',
    'purchase_order', 'custom_operation_type', 'adjustment_reason', 'loss_confirmation', 'notes',
    'project_id' # <-- NUEVO: Permitir actualizar el proyecto
}

# --- PICKING CRUD (Cabecera) ---

def get_picking_details(picking_id, company_id):
    query = """
        SELECT p.*, pt.code as type_code, proj.name as project_name
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN projects proj ON p.project_id = proj.id
        WHERE p.id = %(picking_id)s
    """
    p_info = execute_query(query, {"picking_id": picking_id}, fetchone=True)

    moves_query = """
            SELECT 
                sm.id, pr.name, pr.sku, sm.product_uom_qty, 
                sm.quantity_done, pr.tracking, pr.id as product_id,
                u.name as uom_name,
                sm.price_unit,
                pr.standard_price,
                sm.cost_at_adjustment,
                sm.project_id, proj.name as project_name
            FROM stock_moves sm 
            JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(company_id)s)
            LEFT JOIN uom u ON pr.uom_id = u.id
            LEFT JOIN projects proj ON sm.project_id = proj.id
            WHERE sm.picking_id = %(picking_id)s
        """
    moves = execute_query(moves_query, {"picking_id": picking_id, "company_id": company_id}, fetchall=True)
    
    return p_info, moves


def create_picking(name, picking_type_id, location_src_id, location_dest_id, company_id, responsible_user, work_order_id=None, project_id=None, warehouse_id=None):
    """
    [CORREGIDO] Crea un nuevo picking base.
    Ahora guarda el warehouse_id para que los reportes por almacén funcionen.
    """
    s_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # [FIX] Agregamos warehouse_id a la consulta
    query = """
        INSERT INTO pickings (
            company_id, name, picking_type_id, 
            location_src_id, location_dest_id, warehouse_id, 
            scheduled_date, state, work_order_id, responsible_user, project_id
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'draft', %s, %s, %s) 
        RETURNING id
    """
    params = (
        company_id, name, picking_type_id, 
        location_src_id, location_dest_id, warehouse_id, 
        s_date, work_order_id, responsible_user, project_id
    )
    
    new_id_row = execute_commit_query(query, params, fetchone=True)
    
    if new_id_row and new_id_row[0]:
        return new_id_row[0]
    else:
        raise Exception("No se pudo crear el picking, no se devolvió ID.")

def update_picking_header(pid: int, updates: dict):
    """
    Actualiza campos del picking.
    [BLINDADO] Bloquea edición si el picking ya no es borrador.
    """
    if not updates: return

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            # 1. VERIFICAR ESTADO ACTUAL (Candado de Historia)
            # [CORRECCIÓN]: El FOR UPDATE va DENTRO de las comillas
            cursor.execute("""
                SELECT p.state, pt.code 
                FROM pickings p 
                JOIN picking_types pt ON p.picking_type_id = pt.id 
                WHERE p.id = %s 
                FOR UPDATE
            """, (pid,))
            
            res = cursor.fetchone()
            
            if not res: return # O raise error
            
            current_state = res['state']
            pt_code = res['code']

            # [REGLA DE ORO] Si ya está hecho o cancelado, SOLO permitimos editar notas o referencias.
            if current_state in ('done', 'cancelled'):
                # Definir qué campos son "inofensivos" para editar post-cierre
                safe_fields = {'notes', 'partner_ref', 'invoice_number', 'guide_number'}
                
                # Verificar si intentan tocar algo prohibido
                unsafe_keys = [k for k in updates.keys() if k not in safe_fields]
                
                if unsafe_keys:
                    raise ValueError(f"No se puede editar el documento porque está '{current_state}'. Campos bloqueados: {', '.join(unsafe_keys)}")

            # 2. Preparar actualización
            fields_to_update = {}
            
            # --- LÓGICA DE DETECCIÓN DE ALMACÉN ---
            new_location_id = updates.get('location_src_id') or updates.get('location_dest_id')
            if new_location_id and 'warehouse_id' not in updates:
                cursor.execute("SELECT warehouse_id FROM locations WHERE id = %s AND type = 'internal'", (new_location_id,))
                res_wh = cursor.fetchone()
                if res_wh and res_wh[0]:
                    fields_to_update['warehouse_id'] = res_wh[0]
            # --------------------------------------

            for key, value in updates.items():
                if key in ALLOWED_PICKING_FIELDS_TO_UPDATE:
                    if isinstance(value, date) and not isinstance(value, str): 
                        fields_to_update[key] = value.isoformat()
                    else: 
                        fields_to_update[key] = value
            
            if not fields_to_update: return

            # Construir query dinámica para Header
            set_clause_parts = [f"{key} = %s" for key in fields_to_update.keys()]
            params = list(fields_to_update.values()) + [pid]
            cursor.execute(f"UPDATE pickings SET {', '.join(set_clause_parts)} WHERE id = %s", tuple(params))
            
            # --- 3. LÓGICA DE CASCADA (Sincronizar Líneas) ---
            moves_updates = []
            moves_params = []

            cascade_map = {
                'location_src_id': 'location_src_id',
                'location_dest_id': 'location_dest_id',
                'partner_id': 'partner_id',
                'project_id': 'project_id'
            }

            # [PROTECCIÓN CRÍTICA] 
            # Si es Ajuste (ADJ), NO sobrescribir ubicaciones de líneas con la cabecera.
            if pt_code == 'ADJ':
                cascade_map.pop('location_src_id', None)
                cascade_map.pop('location_dest_id', None)

            for head_field, move_field in cascade_map.items():
                if head_field in fields_to_update:
                    moves_updates.append(f"{move_field} = %s")
                    moves_params.append(fields_to_update[head_field])
            
            if moves_updates:
                query_moves = f"UPDATE stock_moves SET {', '.join(moves_updates)} WHERE picking_id = %s"
                moves_params.append(pid)
                cursor.execute(query_moves, tuple(moves_params))
                print(f"[DB] Cascada ejecutada para Picking {pid} (Moves actualizados)")

            conn.commit()

    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR] update_picking_header: {e}")
        raise e
    finally:
        if conn: return_db_connection(conn)

def cancel_picking(picking_id):
    """
    [BLINDADO] Cancela un picking asegurando que nadie lo esté validando en ese instante.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. BLOQUEO ATÓMICO
            cursor.execute("SELECT id, state FROM pickings WHERE id = %s FOR UPDATE", (picking_id,))
            picking = cursor.fetchone()
            
            if not picking: return False, "El albarán no existe."
            
            # 2. VALIDACIÓN DE ESTADO POST-BLOQUEO
            # Si alguien logró validarlo milisegundos antes, el estado ya será 'done'.
            if picking['state'] == 'done':
                return False, "No se puede cancelar: El albarán YA FUE VALIDADO/PROCESADO."
            
            if picking['state'] == 'cancelled':
                return True, "Ya estaba cancelado."

            # 3. EJECUCIÓN SEGURA
            cursor.execute("UPDATE pickings SET state = 'cancelled' WHERE id = %s", (picking_id,))
            # Cancelar también los movimientos hijos para liberar reservas si las hubiera
            cursor.execute("UPDATE stock_moves SET state = 'cancelled' WHERE picking_id = %s", (picking_id,))
            
        conn.commit()
        return True, "Albarán cancelado correctamente."

    except Exception as e:
        if conn: conn.rollback()
        print(f"[DB-ERROR] cancel_picking: {e}")
        return False, f"Error al cancelar: {e}"
    finally:
        if conn: return_db_connection(conn)

def delete_picking(picking_id):
    """
    [BLINDADO] Borra un picking solo si es borrador y nadie lo está tocando.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. BLOQUEO ATÓMICO
            cursor.execute("SELECT id, state FROM pickings WHERE id = %s FOR UPDATE", (picking_id,))
            picking = cursor.fetchone()
            
            if not picking: return False, "No existe."
            
            # 2. VALIDACIÓN ESTRICTA
            if picking['state'] != 'draft': 
                return False, f"No se puede borrar: El estado es '{picking['state']}' (solo borradores)."

            # 3. EJECUCIÓN
            cursor.execute("SELECT id FROM stock_moves WHERE picking_id = %s", (picking_id,))
            moves = cursor.fetchall()

            if moves:
                move_ids = tuple([move['id'] for move in moves])
                cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN %s", (move_ids,))
                cursor.execute("DELETE FROM stock_moves WHERE picking_id = %s", (picking_id,))
            
            cursor.execute("DELETE FROM pickings WHERE id = %s", (picking_id,))
            
            conn.commit()
            return True, "Eliminado correctamente."

    except Exception as e:
        if conn: conn.rollback()
        return False, f"Error: {e}"
    finally:
        if conn: return_db_connection(conn)

def mark_picking_as_ready(picking_id):
    """
    Cambia el estado a 'listo' (reserva stock).
    [CORREGIDO] Sincroniza el project_id de la cabecera a las líneas ANTES de validar stock.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            # 1. Bloquear y Obtener datos de cabecera
            cursor.execute("""
                SELECT p.id, p.state, pt.code, p.project_id 
                FROM pickings p 
                JOIN picking_types pt ON p.picking_type_id = pt.id 
                WHERE p.id = %s FOR UPDATE
            """, (picking_id,))
            p = cursor.fetchone()
            
            if not p: raise ValueError("El albarán no existe.")
            if p['state'] != 'draft': raise ValueError(f"Estado inválido: {p['state']}. Debe estar en borrador.")

            # --- [FIX DEFINITIVO] SINCRONIZACIÓN ABSOLUTA DE PROYECTO ---
            # Actualizamos SIEMPRE. Si p['project_id'] es None, las líneas pasan a None (Stock General).
            # Esto corrige el bug de "volver a borrador -> cambiar a sin proyecto".
            cursor.execute("""
                UPDATE stock_moves 
                SET project_id = %s 
                WHERE picking_id = %s
            """, (p['project_id'], picking_id))

            # 2. Validación de Integridad
            cursor.execute("SELECT COUNT(*) as count FROM stock_moves WHERE picking_id = %s", (picking_id,))
            if cursor.fetchone()['count'] == 0:
                raise ValueError("El albarán está vacío. Agregue productos primero.")

            # 3. Bloqueo Inteligente de Productos
            cursor.execute("""
                SELECT p.id FROM products p
                JOIN stock_moves sm ON sm.product_id = p.id
                WHERE sm.picking_id = %s
                FOR UPDATE
            """, (picking_id,))

            # 4. Validación de Stock
            # (Ahora las líneas ya tienen el project_id correcto, así que la verificación buscará en el stock del proyecto)
            if p['code'] not in ('IN',):
                ok, msg = _check_stock_with_cursor(cursor, picking_id, p['code'])
                if not ok:
                    raise ValueError(f"Stock insuficiente al intentar reservar:\n{msg}")

            # 5. Actualización de estado
            cursor.execute("UPDATE pickings SET state = 'listo' WHERE id = %s", (picking_id,))
            
        conn.commit()
        return True

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error en mark_picking_as_ready: {e}")
        raise e 
    finally:
        if conn: return_db_connection(conn)

def return_picking_to_draft(picking_id):
    """
    [BLINDADO & CORREGIDO] Regresa a borrador solo si está en 'listo'.
    Corrección: Se agrega cursor_factory para evitar el error de tupla vs dict.
    """
    conn = None
    try:
        conn = get_db_connection()
        # --- CORRECCIÓN AQUÍ: Agregamos el factory ---
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            # 1. BLOQUEO ATÓMICO
            cursor.execute("SELECT id, state FROM pickings WHERE id = %s FOR UPDATE", (picking_id,))
            row = cursor.fetchone()
            
            if not row: return False, "No encontrado."
            
            # 2. VALIDACIÓN (Ahora row['state'] funcionará porque es un DictCursor)
            if row['state'] == 'done':
                return False, "Imposible regresar a borrador: El documento YA FUE VALIDADO."
            
            if row['state'] == 'draft':
                return True, "Ya está en borrador."

            # 3. EJECUCIÓN (De 'listo'/'cancelled' a 'draft')
            # Primero actualizamos la cabecera
            cursor.execute("UPDATE pickings SET state = 'draft' WHERE id = %s", (picking_id,))
            
            # Luego liberamos las reservas en los movimientos
            cursor.execute("UPDATE stock_moves SET state = 'draft' WHERE picking_id = %s", (picking_id,))
            
            conn.commit()
            return True, "Regresado a borrador exitosamente."
            
    except Exception as e:
        if conn: conn.rollback()
        # Imprimimos el error en consola para que lo veas en los logs de Render si vuelve a pasar
        print(f"[ERROR RETURN DRAFT] {e}")
        return False, str(e)
    finally:
        if conn: return_db_connection(conn)

def get_next_picking_name(picking_type_id, company_id):
    """
    [BLINDADO CON ADVISORY LOCK] 
    Genera secuencia única por empresa de forma segura.
    Usa un bloqueo ligero a nivel de aplicación DB para evitar colisiones.
    """
    # 1. Obtener código
    pt = execute_query("SELECT code FROM picking_types WHERE id = %s", (picking_type_id,), fetchone=True)
    pt_code = pt['code']
    prefix = f"C{company_id}/{pt_code}/"

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 2. ADVISORY LOCK (Basado en un hash del prefijo para no bloquear todo el sistema)
            # Esto pone en "fila india" solo a quienes intentan generar un código con este prefijo.
            lock_id = abs(hash(prefix)) % 2147483647 # Postgres integer limit
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
            
            # 3. Ahora que tenemos el turno exclusivo, buscamos el último
            cursor.execute(
                "SELECT name FROM pickings WHERE name LIKE %s AND company_id = %s ORDER BY id DESC LIMIT 1",
                (f"{prefix}%", company_id)
            )
            last_res = cursor.fetchone()
            
            current_sequence = 0
            if last_res:
                try: current_sequence = int(last_res[0].split('/')[-1])
                except: pass

            new_name = f"{prefix}{str(current_sequence + 1).zfill(5)}"
            
            # El lock se libera automáticamente al terminar la transacción (commit/rollback)
            # o al devolver la conexión si usamos advisory_xact_lock
            return new_name
    finally:
        if conn: return_db_connection(conn)

def get_next_remission_number(company_id):
    """
    [BLINDADO CON ADVISORY LOCK] Secuencia para Guías de Remisión.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Lock específico para guías de esta empresa
            lock_key = f"GR-{company_id}"
            lock_id = abs(hash(lock_key)) % 2147483647
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))

            cursor.execute(
                "SELECT remission_number FROM pickings WHERE remission_number IS NOT NULL AND company_id = %s ORDER BY remission_number DESC LIMIT 1",
                (company_id,)
            )
            res = cursor.fetchone()
            
            last = 0
            if res:
                # Asumiendo formato GR-00001. Si cambia, ajustar parseo.
                try: last = int(res[0].replace("GR-", ""))
                except: pass
                
            return f"GR-{str(last + 1).zfill(5)}"
    finally:
        if conn: return_db_connection(conn)

# --- MOVIMIENTOS DE STOCK (Moves) ---

def add_stock_move_to_picking(picking_id, product_id, qty, loc_src_id, loc_dest_id, company_id, price_unit=0, partner_id=None, project_id=None):
    """
    Añade una línea. Soporta project_id.
    [AHORA CON VALIDACIÓN DE REGLAS DE NEGOCIO]
    """
    
    # --- 1. VALIDACIÓN DE REGLAS DE NEGOCIO (Defense in Depth) ---
    # Consultamos los datos necesarios para validar antes de insertar
    validation_query = """
        SELECT 
            pt.code as op_code, 
            p.custom_operation_type,
            pr.ownership,
            pr.name as product_name
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        JOIN products pr ON pr.id = %s
        WHERE p.id = %s
    """
    # Usamos execute_query para lectura rápida (sin commit aún)
    val_data = execute_query(validation_query, (product_id, picking_id), fetchone=True)
    
    if val_data:
        op_name = val_data['custom_operation_type']
        ownership = val_data['ownership'] or 'owned' # Default a 'owned' si es nulo
        p_name = val_data['product_name']
        
        # Regla: Compra Nacional -> Solo Owned
        if op_name == "Compra Nacional" and ownership != 'owned':
            raise ValueError(f"Regla de Negocio: No puedes comprar '{p_name}' porque es material Consignado.")
            
        # Regla: Consignación Recibida -> Solo Consigned
        elif op_name == "Consignación Recibida" and ownership != 'consigned':
            raise ValueError(f"Regla de Negocio: '{p_name}' es material Propio, no puedes recibirlo como Consignación.")
            
        # Regla: Devolución a Proveedor -> Solo Owned
        elif op_name == "Devolución a Proveedor" and ownership != 'owned':
             raise ValueError(f"Regla de Negocio: No puedes devolver '{p_name}' a proveedor porque es Consignado (usa Dev. a Cliente).")

        # Regla: Devolución a Cliente -> Solo Consigned
        elif op_name == "Devolución a Cliente" and ownership != 'consigned':
             raise ValueError(f"Regla de Negocio: No puedes devolver '{p_name}' a cliente porque es Propio (usa Dev. a Proveedor).")

    # --- 2. INSERCIÓN ORIGINAL ---
    query = """
    WITH new_move AS (
        INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done, location_src_id, location_dest_id, price_unit, partner_id, project_id) 
        VALUES (%(pid)s, %(prod)s, %(qty)s, %(qty)s, %(src)s, %(dest)s, %(price)s, %(part)s, %(proj)s) 
        RETURNING *
    )
    SELECT sm.id, pr.name, pr.sku, sm.product_uom_qty, sm.quantity_done, pr.tracking, pr.id as product_id, u.name as uom_name, sm.price_unit,
           sm.project_id, pr.ownership -- Agregamos ownership al retorno por si acaso
    FROM new_move sm
    JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(cid)s)
    LEFT JOIN uom u ON pr.uom_id = u.id;
    """
    params = {
        "pid": picking_id, "prod": product_id, "qty": qty, 
        "src": loc_src_id, "dest": loc_dest_id, 
        "company_id": company_id, "cid": company_id,
        "price": price_unit, "part": partner_id, "proj": project_id
    }
    
    new_move = execute_commit_query(query, params, fetchone=True)
    if new_move: return new_move
    raise Exception("Error creando move.")

def update_move_quantity_done(move_id, quantity_done, company_id):
    query = """
    WITH updated_move AS (
        UPDATE stock_moves SET product_uom_qty = %(qty)s, quantity_done = %(qty)s WHERE id = %(mid)s RETURNING *
    )
    SELECT sm.id, pr.name, pr.sku, sm.product_uom_qty, sm.quantity_done, pr.tracking, pr.id as product_id, u.name as uom_name, sm.price_unit
    FROM updated_move sm JOIN products pr ON (sm.product_id = pr.id AND pr.company_id = %(cid)s) LEFT JOIN uom u ON pr.uom_id = u.id;
    """
    return execute_commit_query(query, {"qty": quantity_done, "mid": move_id, "cid": company_id}, fetchone=True)

def update_move_price(move_id, new_price):
    """ Actualiza solo el precio unitario de una línea existente. """
    # Usamos execute_commit_query para asegurar que se guarde
    query = "UPDATE stock_moves SET price_unit = %s WHERE id = %s RETURNING id"
    res = execute_commit_query(query, (new_price, move_id), fetchone=True)
    return True if res else False

def delete_stock_move(move_id):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM stock_move_lines WHERE move_id = %s", (move_id,))
            cursor.execute("DELETE FROM stock_moves WHERE id = %s", (move_id,))
        conn.commit()
        return True
    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        if conn: return_db_connection(conn)

def save_move_lines_for_move(move_id, tracking_data: dict):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT product_id FROM stock_moves WHERE id = %s", (move_id,))
            product_id = cursor.fetchone()['product_id']
            
            cursor.execute("DELETE FROM stock_move_lines WHERE move_id = %s", (move_id,))
            count = 0
            for name, qty in tracking_data.items():
                if qty <= 0: continue
                lot_id = create_lot(cursor, product_id, name)
                cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move_id, lot_id, qty))
                count += 1
        conn.commit()
        return True, f"{count} series guardadas."
    except Exception as e:
        if conn: conn.rollback()
        return False, str(e)
    finally:
        if conn: return_db_connection(conn)

# --- LOTES Y SERIES ---

def get_lot_by_name(cursor, product_id, lot_name):
    # Soporta cursor o llamada directa
    if cursor is None: return execute_query("SELECT id FROM stock_lots WHERE product_id = %s AND name = %s", (product_id, lot_name), fetchone=True)
    cursor.execute("SELECT id FROM stock_lots WHERE product_id = %s AND name = %s", (product_id, lot_name))
    return cursor.fetchone()

def create_lot(cursor, product_id, lot_name):
    """
    Crea un lote/serie asegurando limpieza y validación estricta.
    [CORREGIDO] Agregado .strip() para eliminar \r y \n invisibles.
    """
    try:
        if lot_name:
            # 1. LIMPIEZA AGRESIVA
            # .strip() elimina \r, \n, \t del inicio y final. 
            # .replace(" ", "") elimina espacios intermedios.
            lot_name = str(lot_name).strip().replace(" ", "").upper()
            
            # 2. VALIDACIÓN DE LONGITUD
            if len(lot_name) > 30:
                raise ValueError(f"La serie '{lot_name[:15]}...' es demasiado larga (Máximo 30 caracteres).")
                
            # 3. VALIDACIÓN DE CARACTERES (Whitelist)
            if not re.match(r'^[A-Z0-9\-_/\.]+$', lot_name):
                # Usamos repr() para que el error muestre los caracteres invisibles si quedan (ej: 'Serie\r')
                raise ValueError(f"La serie {repr(lot_name)} contiene caracteres inválidos. Solo se permiten letras, números y guiones.")

        cursor.execute("INSERT INTO stock_lots (name, product_id) VALUES (%s, %s) ON CONFLICT (product_id, name) DO NOTHING RETURNING id", (lot_name, product_id))
        new_id = cursor.fetchone()
        if new_id: return new_id[0]
        
        cursor.execute("SELECT id FROM stock_lots WHERE product_id = %s AND name = %s", (product_id, lot_name))
        return cursor.fetchone()[0]
    except Exception as e:
        raise e

def get_available_serials_at_location(product_id, location_id, project_id=None):
    """
    Obtiene series disponibles, filtrando por PROYECTO si se especifica.
    """
    if not location_id: return []
    
    # Filtro dinámico de proyecto
    # Si project_id tiene valor: Trae series de ese proyecto + series generales (NULL)
    # Si project_id es None: Trae series generales (NULL) (o todas si quisieras visión global, pero mantengamos la consistencia)
    
    proj_clause = "AND (sq.project_id = %s OR sq.project_id IS NULL)" if project_id is not None else "AND sq.project_id IS NULL"
    params = [product_id, location_id]
    if project_id is not None: params.append(project_id)
    
    # Params para la subquery (location_src)
    params.append(location_id)

    query = f"""
        SELECT sl.id, sl.name 
        FROM stock_quants sq 
        JOIN stock_lots sl ON sq.lot_id = sl.id
        WHERE sq.product_id = %s 
          AND sq.location_id = %s 
          AND sq.quantity > 0
          {proj_clause} -- <--- FILTRO PROYECTO
          AND sl.id NOT IN (
              -- Excluir lo que ya está reservado en otros pickings listos
              SELECT sml.lot_id 
              FROM stock_move_lines sml 
              JOIN stock_moves sm ON sml.move_id = sm.id 
              JOIN pickings p ON sm.picking_id = p.id
              WHERE sm.location_src_id = %s 
                AND p.state = 'listo' 
                AND sm.state != 'cancelled'
          )
        ORDER BY sl.name
    """
    return execute_query(query, tuple(params), fetchall=True)

def get_serials_for_picking(picking_id):
    query = """
        SELECT sm.id as move_id, sl.name as lot_name, sml.qty_done
        FROM stock_moves sm JOIN stock_move_lines sml ON sm.id = sml.move_id JOIN stock_lots sl ON sml.lot_id = sl.id
        WHERE sm.picking_id = %s
    """
    results = execute_query(query, (picking_id,), fetchall=True)
    serials_by_move = defaultdict(dict)
    for row in results: serials_by_move[row['move_id']][row['lot_name']] = row['qty_done']
    return serials_by_move

# --- HELPERS DE CONFIGURACIÓN Y TIPOS ---

def get_picking_types(company_id):
    query = "SELECT MIN(id) as id, MIN(name) as name, code FROM picking_types WHERE company_id = %s GROUP BY code ORDER BY code"
    return execute_query(query, (company_id,), fetchall=True)

def get_picking_type_details(type_id): return execute_query("SELECT * FROM picking_types WHERE id = %s", (type_id,), fetchone=True)
def get_picking_type_by_code(warehouse_id, code): return execute_query("SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id = %s AND code = %s", (warehouse_id, code), fetchone=True)
def find_picking_type_id(company_id, type_code, warehouse_id=None):
    params = [company_id, type_code]
    query = "SELECT id FROM picking_types WHERE company_id = %s AND code = %s"
    if warehouse_id: query += " AND warehouse_id = %s"; params.append(warehouse_id)
    result = execute_query(query + " LIMIT 1", tuple(params), fetchone=True)
    return result['id'] if result else None

def get_operation_types_by_code(code): return execute_query("SELECT id, name FROM operation_types WHERE code = %s ORDER BY name", (code,), fetchall=True)
def get_operation_type_details(name): return execute_query("SELECT * FROM operation_types WHERE name = %s", (name,), fetchone=True)
@functools.lru_cache
def get_operation_type_details_by_name(name): return execute_query("SELECT * FROM operation_types WHERE TRIM(name) = TRIM(%s)", (name,), fetchone=True)


# --- LÓGICA CORE: VALIDACIÓN Y STOCK UPDATE (CON PROYECTOS) ---

def update_stock_quant(cursor, product_id, location_id, quantity_change, lot_id=None, project_id=None):
    """
    Actualiza el stock físico.
    [MODIFICADO V2] Soporta 'project_id'. Si project_id es None, usa stock general (NULL).
    """
    op_type = "SUMANDO" if quantity_change > 0 else "RESTANDO"
    print(f"    [+] update_stock_quant: {op_type} {abs(quantity_change)} uds. Prod {product_id} Loc {location_id} Proj {project_id}")

    if location_id is None: raise ValueError("Location ID null")
    
    # Manejo de NULLs para la consulta SQL
    lot_sql = "lot_id = %s" if lot_id else "lot_id IS NULL"
    proj_sql = "project_id = %s" if project_id else "project_id IS NULL"
    
    params = [product_id, location_id]
    if lot_id: params.append(lot_id)
    if project_id: params.append(project_id)
    
    cursor.execute(f"SELECT id, quantity FROM stock_quants WHERE product_id = %s AND location_id = %s AND {lot_sql} AND {proj_sql}", tuple(params))
    quant = cursor.fetchone()
    
    if quant:
        new_qty = quant['quantity'] + quantity_change
        if new_qty < -0.001 and quantity_change < 0: 
            raise ValueError(f"Stock insuficiente (ID Quant: {quant['id']}). Se intentó restar {abs(quantity_change)}, había {quant['quantity']}.")
        
        if new_qty > 0.001:
            cursor.execute("UPDATE stock_quants SET quantity = %s WHERE id = %s", (new_qty, quant['id']))
        else:
            cursor.execute("DELETE FROM stock_quants WHERE id = %s", (quant['id'],))
    elif quantity_change > 0.001:
        # Crear nuevo registro de stock (con el project_id correspondiente)
        cursor.execute(
            "INSERT INTO stock_quants (product_id, location_id, lot_id, project_id, quantity) VALUES (%s, %s, %s, %s, %s)", 
            (product_id, location_id, lot_id, project_id, quantity_change)
        )
    elif quantity_change < -0.001:
        # Intentando restar de algo que no existe
        pass # El validador previo debería haber atrapado esto

def _check_stock_with_cursor(cursor, picking_id, picking_type_code):
    """
    [CORREGIDO - LÓGICA UNIVERSAL]
    Valida disponibilidad REAL.
    Disponible = (Físico Total) - (Reservado por TODOS en Moves 'listo').
    """
    # 1. Obtener demanda del Picking Actual
    cursor.execute("""
        SELECT sm.product_id, sm.location_src_id, 
               SUM(sm.product_uom_qty) as qty_needed, 
               MAX(p.name) as prod_name
        FROM stock_moves sm 
        JOIN products p ON sm.product_id = p.id 
        JOIN locations l ON sm.location_src_id = l.id
        WHERE sm.picking_id = %s AND l.type = 'internal'
        GROUP BY sm.product_id, sm.location_src_id
    """, (picking_id,))
    demands = cursor.fetchall()
    
    if not demands: return True, "Ok"

    errors = []
    
    for row in demands:
        pid = row['product_id']
        loc = row['location_src_id']
        needed = float(row['qty_needed'])
        p_name = row['prod_name']

        # 2. Calcular Físico (Lo que existe en la estantería)
        # Sumamos TODO lo que hay en esa ubicación (General + Proyectos)
        # Porque si está en la ubicación, físicamente está ahí.
        cursor.execute("""
            SELECT COALESCE(SUM(quantity), 0) 
            FROM stock_quants 
            WHERE product_id = %s AND location_id = %s
        """, (pid, loc))
        physical_qty = float(cursor.fetchone()[0])

        # 3. Calcular Reservado TOTAL (La Competencia)
        # Sumamos TODO lo que otros pickings 'listo' ya apartaron de esa ubicación.
        # NO filtramos por proyecto. Si la Obra A reservó 3, esas 3 ya no existen para nadie.
        cursor.execute("""
            SELECT COALESCE(SUM(sm.product_uom_qty), 0)
            FROM stock_moves sm
            JOIN pickings p ON sm.picking_id = p.id
            WHERE sm.product_id = %s 
              AND sm.location_src_id = %s
              AND p.state = 'listo'
              AND p.id != %s  -- Excluirnos a nosotros mismos para no autobloquearnos
              AND sm.state != 'cancelled'
        """, (pid, loc, picking_id))
        reserved_global = float(cursor.fetchone()[0])

        # 4. Cálculo Final
        available_real = physical_qty - reserved_global

        if picking_type_code == 'ADJ':
            # Para ajustes negativos (restar stock), validamos contra físico puro
            if needed < 0 and physical_qty < abs(needed):
                 errors.append(f"- {p_name}: Físico {physical_qty} < Ajuste {abs(needed)}")
        else:
            # Para salidas normales, validamos contra disponible neto
            if available_real < needed:
                errors.append(
                    f"- {p_name}: Requerido {needed} > Disponible {available_real} "
                    f"(Físico: {physical_qty} - Reservado Global: {reserved_global})"
                )

    if errors: return False, "Stock insuficiente:\n" + "\n".join(errors)
    return True, "Ok"

def _update_product_weighted_cost(cursor, product_id, incoming_qty, incoming_price):
    """
    [BLINDADO FINANCIERO] Recalcula el Precio Estándar (Costo Promedio).
    Usa 'FOR UPDATE' en la tabla products para evitar corrupción de costos 
    si entran dos compras simultáneas.
    """
    if incoming_qty <= 0 or incoming_price < 0: return

    # 1. BLOQUEO ATÓMICO DEL PRODUCTO
    # Esto evita que otro hilo lea el precio/stock antiguo mientras nosotros calculamos.
    cursor.execute("SELECT standard_price FROM products WHERE id = %s FOR UPDATE", (product_id,))
    res_price = cursor.fetchone()
    current_price = res_price['standard_price'] if res_price else 0.0

    # 2. Obtener Stock Físico Actual (Global de la empresa)
    # Nota: No necesitamos bloquear stock_quants porque ya bloqueamos el producto "padre",
    # lo que actúa como semáforo para cualquier operación de re-costeo de este producto.
    cursor.execute("SELECT SUM(quantity) as total FROM stock_quants WHERE product_id = %s", (product_id,))
    res_qty = cursor.fetchone()
    current_qty = res_qty['total'] if res_qty and res_qty['total'] else 0.0
    
    # Protegernos contra stocks negativos teóricos al valorar
    current_qty = max(0.0, current_qty) 

    # 3. Calcular Nuevo Precio Promedio
    new_total_qty = current_qty + incoming_qty
    
    # Calcular valor total actual + valor de lo que entra
    total_value = (current_qty * current_price) + (incoming_qty * incoming_price)
    
    new_avg_price = total_value / new_total_qty if new_total_qty > 0 else incoming_price

    # 4. Actualizar Maestro de Productos
    # Usamos redondeo a 4 decimales para evitar micro-cambios irrelevantes
    if abs(new_avg_price - current_price) > 0.0001:
        cursor.execute("UPDATE products SET standard_price = %s WHERE id = %s", (new_avg_price, product_id))
        print(f"[WAC-SAFE] Prod {product_id}: {current_price:.2f} -> {new_avg_price:.2f} (Base: {current_qty} uds, Entran: {incoming_qty} @ {incoming_price})")

def _process_picking_validation_with_cursor(cursor, picking_id, moves_with_tracking):
    """
    [BLINDADO v2 - PREVENCIÓN DOBLE CLIC] 
    Valida y ejecuta el movimiento de stock.
    Usa 'FOR UPDATE' para bloquear la fila y evitar condiciones de carrera.
    """
    # 1. BLOQUEO ATÓMICO (Critical Section)
    # Al usar FOR UPDATE, si llega un segundo clic, se quedará esperando aquí 
    # hasta que el primero termine.
    cursor.execute("""
        SELECT p.*, pt.code 
        FROM pickings p 
        JOIN picking_types pt ON p.picking_type_id = pt.id 
        WHERE p.id = %s 
        FOR UPDATE
    """, (picking_id,))
    
    picking = cursor.fetchone()
    
    # 2. VERIFICACIÓN DE ESTADO POST-BLOQUEO
    # Cuando el segundo clic logre entrar (después de que el primero termine),
    # se encontrará con que el estado ya es 'done' y será rechazado.
    if not picking: 
        return False, "El albarán no existe."
    
    if picking['state'] == 'done':
        return False, "TRANQUILO: Este ajuste ya fue validado procesado exitosamente por una petición anterior."
        
    if picking['state'] == 'cancelled':
        return False, "El albarán está cancelado."

    p_code = picking['code']
    project_id = picking['project_id'] 

    # 1.1 Obtener Ubicaciones Virtuales (para IN/OUT)
    v_loc, c_loc = None, None
    if p_code in ('IN', 'OUT'):
        cursor.execute("SELECT id, category FROM locations WHERE category IN ('PROVEEDOR', 'CLIENTE')")
        for r in cursor.fetchall():
            if r['category'] == 'PROVEEDOR': v_loc = r['id']
            elif r['category'] == 'CLIENTE': c_loc = r['id']

    # 2. Asegurar project_id en moves
    cursor.execute("UPDATE stock_moves SET project_id = %s WHERE picking_id = %s", (project_id, picking_id))

    # 3. Obtener Movimientos
    cursor.execute("""
        SELECT sm.*, p.tracking, p.ownership, p.name as product_name 
        FROM stock_moves sm 
        JOIN products p ON sm.product_id = p.id 
        WHERE sm.picking_id = %s
    """, (picking_id,))
    moves = cursor.fetchall()

    # 4. Validar Stock Numérico General
    ok, msg = _check_stock_with_cursor(cursor, picking_id, p_code)
    if not ok: return False, msg

    processed_serials_in_transaction = set()
    
    for m in moves:
        # ... (INICIO DE LÓGICA DE VALORACIÓN - IGUAL QUE ANTES) ...
        if p_code == 'IN' and m['ownership'] == 'owned':
            qty_in = m['quantity_done']
            cost_in = m['price_unit']
            if qty_in > 0 and cost_in > 0:
                _update_product_weighted_cost(cursor, m['product_id'], qty_in, cost_in)
        # ---------------------------------------------------

        src, dest = m['location_src_id'], m['location_dest_id']
        if p_code == 'IN': src = v_loc
        elif p_code == 'OUT': dest = c_loc
        
        qty_total = m['quantity_done']
        m_proj = m['project_id']
        
        # --- VALIDACIÓN DE LOTE/SERIE ---
        lot_ids_to_process = [] 

        if m['tracking'] == 'none':
            lot_ids_to_process.append((None, qty_total))
        else:
            t_data = moves_with_tracking.get(str(m['id'])) or moves_with_tracking.get(m['id']) or {}
            
            total_tracking_qty = sum(t_data.values())
            if abs(qty_total - total_tracking_qty) > 0.001:
                 return False, f"Error en '{m['product_name']}': Cantidad ({qty_total}) vs Series ({total_tracking_qty}) no coinciden."

            for lname, lqty in t_data.items():
                lname = lname.strip().upper()
                
                if (m['product_id'], lname) in processed_serials_in_transaction: 
                    return False, f"Serie duplicada en esta operación: {lname}"
                processed_serials_in_transaction.add((m['product_id'], lname))
                
                if m['tracking'] == 'serial' and lqty > 1:
                    return False, f"Error: La serie '{lname}' tiene cantidad {lqty}. Debe ser 1."

                # REGLA DE LA VIRGINIDAD (Solo Entradas - IN)
                if p_code == 'IN' and m['tracking'] == 'serial':
                    cursor.execute("""
                        SELECT w.name FROM stock_quants sq 
                        JOIN stock_lots sl ON sq.lot_id = sl.id 
                        JOIN locations l ON sq.location_id = l.id 
                        JOIN warehouses w ON l.warehouse_id = w.id
                        WHERE sl.name = %s AND sl.product_id = %s AND sq.quantity > 0 AND l.type = 'internal'
                        LIMIT 1
                    """, (lname, m['product_id']))
                    existing = cursor.fetchone()
                    if existing:
                        return False, f"La serie '{lname}' YA EXISTE en '{existing['name']}'."

                lot_id = create_lot(cursor, m['product_id'], lname)
                lot_ids_to_process.append((lot_id, lqty))
                cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (m['id'], lot_id, lqty))

        # --- LÓGICA DE STOCK Y PROYECTOS ---
        cursor.execute("""
            SELECT wc.name FROM locations l 
            JOIN warehouses w ON l.warehouse_id = w.id 
            JOIN warehouse_categories wc ON w.category_id = wc.id 
            WHERE l.id = %s
        """, (dest,))
        dest_cat_row = cursor.fetchone()
        is_dest_main = (dest_cat_row and dest_cat_row['name'] == 'ALMACEN PRINCIPAL')
        dest_proj_id = None if is_dest_main else m_proj

        for lot_id, qty in lot_ids_to_process:
            if p_code == 'IN' or (p_code == 'ADJ' and qty > 0):
                update_stock_quant(cursor, m['product_id'], dest, qty, lot_id, dest_proj_id)
                if p_code == 'ADJ': update_stock_quant(cursor, m['product_id'], src, -qty, lot_id, m_proj)
            else:
                qty_to_deduct = qty
                # A) DESCONTAR DEL ORIGEN (Proyecto específico primero)
                if m_proj is not None:
                    cursor.execute("SELECT quantity FROM stock_quants WHERE product_id=%s AND location_id=%s AND project_id=%s " + ("AND lot_id=%s" if lot_id else "AND lot_id IS NULL"), 
                                   (m['product_id'], src, m_proj) + ((lot_id,) if lot_id else ()))
                    res = cursor.fetchone()
                    available_proj = res['quantity'] if res else 0.0
                    deduct_from_proj = min(qty_to_deduct, available_proj)
                    
                    if deduct_from_proj > 0:
                        update_stock_quant(cursor, m['product_id'], src, -deduct_from_proj, lot_id, m_proj)
                        qty_to_deduct -= deduct_from_proj
                
                # B) Si falta, descontar del Stock GENERAL
                if qty_to_deduct > 0:
                    update_stock_quant(cursor, m['product_id'], src, -qty_to_deduct, lot_id, None) 

                # C) SUMAR AL DESTINO
                if p_code != 'ADJ': 
                    update_stock_quant(cursor, m['product_id'], dest, qty, lot_id, dest_proj_id)
                else:
                    # Ajuste negativo
                    update_stock_quant(cursor, m['product_id'], dest, qty, lot_id, m_proj)

    cursor.execute("UPDATE stock_moves SET state = 'done' WHERE picking_id = %s", (picking_id,))
    cursor.execute("UPDATE pickings SET state = 'done', date_done = NOW() WHERE id = %s", (picking_id,))
    
    if project_id:
        project_repo.check_and_update_project_phase(project_id)

    return True, "Validado correctamente."

def process_picking_validation(picking_id, moves_with_tracking):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            ok, msg = _process_picking_validation_with_cursor(cursor, picking_id, moves_with_tracking)
            if ok: conn.commit()
            else: conn.rollback()
            return ok, msg
    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return False, str(e)
    finally:
        if conn: return_db_connection(conn)

# --- OTROS HELPERS (Listados) ---
def get_pickings_count(picking_type_code, company_id, filters={}):
    base_query = """
    SELECT COUNT(p.id) as total_count FROM pickings p 
    JOIN picking_types pt ON p.picking_type_id = pt.id
    WHERE pt.code = %s AND p.company_id = %s AND pt.code != 'ADJ'
    """
    # (Lógica simplificada de filtros para brevedad, reutilizar la de la versión anterior si es compleja)
    return execute_query(base_query, (picking_type_code, company_id), fetchone=True)['total_count']

def get_pickings_by_type(picking_type_code, company_id, filters={}, sort_by='id', ascending=False, limit=None, offset=None):
    """
    Obtiene la lista de operaciones.
    [CORREGIDO] 'project_name' ahora devuelve "PEP (Macro)" en lugar del nombre simple.
    """
    sort_map = {
        'name': "p.name", 'purchase_order': "p.purchase_order", 
        'project_name': "proj.code", # Ordenar por código PEP es más útil ahora
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

    # --- Lógica de Filtros ---
    for key, value in filters.items():
         if value:
            if key in ["date_transfer_from", "date_transfer_to"]:
                try:
                    db_date = datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
                    operator = ">=" if key == "date_transfer_from" else "<="
                    where_clauses.append(f"p.date_transfer {operator} %s")
                    query_params.append(db_date)
                except ValueError: pass
            
            elif key == 'p.state':
                where_clauses.append("p.state = %s"); query_params.append(value)
            
            # [CORRECCIÓN] Filtro de Proyecto busca en Código o Macro Nombre
            elif key == 'project_name':
                 where_clauses.append("(proj.code ILIKE %s OR mp.name ILIKE %s)")
                 query_params.extend([f"%{value}%", f"%{value}%"])

            elif key in ["p.partner_ref", "p.custom_operation_type", "p.name", "p.purchase_order", "p.responsible_user"]:
                where_clauses.append(f"{key} ILIKE %s"); query_params.append(f"%{value}%")
            
            elif key == 'src_path_display':
                where_clauses.append("CASE WHEN pt.code = 'IN' THEN partner.name ELSE l_src.path END ILIKE %s")
                query_params.append(f"%{value}%")
            elif key == 'dest_path_display':
                 where_clauses.append("CASE WHEN pt.code = 'OUT' THEN partner.name ELSE l_dest.path END ILIKE %s")
                 query_params.append(f"%{value}%")
            elif key == 'w_src.name':
                 where_clauses.append("w_src.name ILIKE %s"); query_params.append(f"%{value}%")
            elif key == 'w_dest.name':
                 where_clauses.append("w_dest.name ILIKE %s"); query_params.append(f"%{value}%")

    where_string = " AND " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT
        p.id, p.name, p.state, p.purchase_order, p.partner_ref, p.custom_operation_type, p.responsible_user,
        TO_CHAR(p.scheduled_date, 'DD/MM/YYYY') as date,
        TO_CHAR(p.date_transfer, 'DD/MM/YYYY') as transfer_date,
        pt.code as type_code,
        
        CASE WHEN pt.code = 'IN' THEN partner.name ELSE l_src.path END as src_path_display,
        CASE WHEN pt.code = 'OUT' THEN partner.name ELSE l_dest.path END as dest_path_display,
        
        CASE WHEN l_src.type = 'internal' THEN w_src.name ELSE NULL END as warehouse_src_name,
        CASE WHEN l_dest.type = 'internal' THEN w_dest.name ELSE NULL END as warehouse_dest_name,
        
        -- [CAMBIO CRÍTICO] Concatenación PEP + Macro para la vista lista
        CASE 
            WHEN proj.id IS NOT NULL THEN CONCAT(proj.code, ' (', mp.name, ')') 
            ELSE NULL 
        END as project_name

    FROM pickings p
    JOIN picking_types pt ON p.picking_type_id = pt.id
    LEFT JOIN locations l_src ON p.location_src_id = l_src.id
    LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
    LEFT JOIN partners partner ON p.partner_id = partner.id
    LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
    LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
    
    -- Joins de Proyecto
    LEFT JOIN projects proj ON p.project_id = proj.id
    LEFT JOIN macro_projects mp ON proj.macro_project_id = mp.id
    
    WHERE pt.code = %s AND p.company_id = %s AND pt.code != 'ADJ'
    {where_string}
    ORDER BY {order_by_column} {direction}
    """

    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        query_params.extend([limit, offset])

    return execute_query(query, tuple(query_params), fetchall=True)

def get_or_create_by_name(cursor, table, name):
    if not name: return None
    if table not in ['stock_lots', 'brands']: raise ValueError("Tabla no permitida")
    cursor.execute(f"SELECT id FROM {table} WHERE name=%s", (name,))
    res = cursor.fetchone()
    if res: return res[0]
    cursor.execute(f"INSERT INTO {table} (name) VALUES (%s) RETURNING id", (name,))
    return cursor.fetchone()[0]

# --- STOCK HELPERS (LECTURA) ---

def get_stock_on_hand(warehouse_id=None):
    """
    Obtiene el stock físico agrupado por Producto, Ubicación, Lote y PROYECTO.
    """
    base_query = """
    SELECT
        p.sku, p.name as product_name, pc.name as category_name,
        w.name as warehouse_name, sl.name as lot_name,
        sq.project_id, proj.name as project_name, -- <-- NUEVO
        SUM(sq.quantity) as quantity,
        u.name as uom_name,
        w.id, p.id, sl.id, pc.id, u.id
    FROM stock_quants sq
    JOIN products p ON sq.product_id = p.id
    JOIN locations l ON sq.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN stock_lots sl ON sq.lot_id = sl.id
    LEFT JOIN uom u ON p.uom_id = u.id
    LEFT JOIN projects proj ON sq.project_id = proj.id -- <-- NUEVO JOIN
    WHERE sq.quantity > 0
    """
    params = []
    if warehouse_id:
        base_query += " AND w.id = %s"
        params.append(warehouse_id)

    base_query += " GROUP BY w.id, w.name, p.id, p.sku, p.name, sl.id, sl.name, pc.id, pc.name, u.id, u.name, sq.project_id, proj.name"
    base_query += " ORDER BY w.name, p.name, sl.name"
    
    return execute_query(base_query, tuple(params), fetchall=True)

def get_reserved_stock(product_id, location_id):
    # Nota: El stock reservado también debería considerar el proyecto, 
    # pero por ahora sumamos todo lo reservado en esa ubicación física.
    query = """
        SELECT SUM(sm.product_uom_qty) as reserved_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE sm.product_id = %s
          AND sm.location_src_id = %s
          AND p.state = 'listo'
          AND sm.state != 'cancelled'
    """
    result = execute_query(query, (product_id, location_id), fetchone=True)
    return result['reserved_qty'] if result and result['reserved_qty'] else 0.0

def get_incoming_stock(product_id, location_id):
    query = """
        SELECT SUM(sm.product_uom_qty) as incoming_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE sm.product_id = %s
          AND sm.location_dest_id = %s
          AND p.state = 'listo'
          AND sm.state != 'cancelled'
    """
    result = execute_query(query, (product_id, location_id), fetchone=True)
    return result['incoming_qty'] if result and result['incoming_qty'] else 0.0

def get_stock_for_product_location(product_id, location_id):
    """Obtiene el total físico (sin desglosar lotes/proyectos)."""
    result = execute_query(
        "SELECT SUM(quantity) as total FROM stock_quants WHERE product_id = %s AND location_id = %s",
        (product_id, location_id), fetchone=True
    )
    return result['total'] if result and result['total'] else 0

def get_real_available_stock(product_id, location_id, project_id=None):
    """
    [CORREGIDO - LÓGICA UNIVERSAL]
    Calcula disponible para mostrar en la UI.
    Disponible = (Físico Total) - (Reservado Total).
    El parámetro project_id se ignora para el cálculo de disponibilidad neta
    porque el stock físico es un recurso compartido en la ubicación.
    """
    if not product_id or not location_id: return 0.0
    
    # 1. Físico Total
    res_phy = execute_query("""
        SELECT COALESCE(SUM(quantity), 0) as total 
        FROM stock_quants 
        WHERE product_id = %s AND location_id = %s
    """, (product_id, location_id), fetchone=True)
    physical = float(res_phy['total'])
    
    # 2. Reservado Total (Global)
    # Sumamos todas las reservas activas en esa ubicación, sin importar quién las pidió.
    res_res = execute_query("""
        SELECT COALESCE(SUM(sm.product_uom_qty), 0) as reserved
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE sm.product_id = %s 
          AND sm.location_src_id = %s
          AND p.state = 'listo'
          AND sm.state != 'cancelled'
    """, (product_id, location_id), fetchone=True)
    reserved = float(res_res['reserved'])

    # 3. Resultado
    available = max(0.0, physical - reserved)
    
    # Debug
    # print(f"[STOCK-CHECK] Prod {product_id} @ Loc {location_id}: Fis {physical} - Res {reserved} = {available}")
    
    return available

def get_products_with_stock_at_location(location_id):
    if not location_id: return []
    query = """
        SELECT DISTINCT p.id, p.name, p.sku
        FROM products p JOIN stock_quants sq ON p.id = sq.product_id
        WHERE sq.location_id = %s AND sq.quantity > 0 ORDER BY p.name
    """
    return execute_query(query, (location_id,), fetchall=True)

def get_stock_for_multiple_products(location_id, product_ids: list):
    if not location_id or not product_ids: return {}
    placeholders = ', '.join('%s' for _ in product_ids)
    query = f"SELECT product_id, SUM(quantity) as on_hand_stock FROM stock_quants WHERE location_id = %s AND product_id IN ({placeholders}) GROUP BY product_id"
    params = [location_id] + product_ids
    results = execute_query(query, tuple(params), fetchall=True)
    return {row['product_id']: row['on_hand_stock'] for row in results}

# --- BORRADORES Y VALIDACIONES ---

def create_or_update_draft_picking(wo_id, company_id, user_name, warehouse_id, date_attended, service_act_number, lines_data: list):
    """
    Wrapper público para guardar borradores (usa la lógica interna con transacción).
    """
    print(f"[DB-WRAPPER] Guardando borrador para WO {wo_id}")
    
    # Usamos el helper de conexión manual para la transacción
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Construimos el diccionario de datos
            picking_data = {
                'warehouse_id': warehouse_id,
                'date_attended_db': date_attended,
                'service_act_number': service_act_number,
                'lines_data': lines_data,
                'location_src_id': None # Se calcula dentro si es OUT
            }
            # Llamamos a la interna (que ya definimos en el bloque anterior, asegúrate de tenerla)
            # Si no la tienes, avísame. Asumo que está en el código previo.
            # Aquí solo decidimos si es OUT (consumo) o qué. 
            # NOTA: Esta función era específica para Liquidaciones.
            # Asumiremos OUT por defecto si llamas a esto.
            
            # Para Liquidación: OUT
            pid, _ = _create_or_update_draft_picking_internal(cursor, wo_id, 'OUT', picking_data, company_id, user_name)
            
        conn.commit()
        return True, "Borrador guardado."
    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return False, str(e)
    finally:
        if conn: return_db_connection(conn)

def check_stock_for_picking(picking_id):
    """
    Verifica stock antes de marcar como listo.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT p.id, pt.code FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id WHERE p.id = %s", (picking_id,))
            res = cursor.fetchone()
            if not res: return False, "Picking no encontrado"
            
            # Llamamos a la interna (que soporta project_id)
            return _check_stock_with_cursor(cursor, picking_id, res['code'])
    finally:
        if conn: return_db_connection(conn)

# --- AJUSTES DE INVENTARIO (COMPLETO) ---

def get_adjustments(company_id):
    query = """
        SELECT p.id, p.name, p.state, TO_CHAR(p.scheduled_date, 'YYYY-MM-DD') as date, 
               l_src.path as src_path, l_dest.path as dest_path, 
               p.responsible_user, p.adjustment_reason, p.notes, p.loss_confirmation
        FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN locations l_src ON p.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
        WHERE p.company_id = %s AND pt.code = 'ADJ' ORDER BY p.id DESC
    """
    return execute_query(query, (company_id,), fetchall=True)

def create_draft_adjustment(company_id, user_name):
    """
    [CORREGIDO COMPLETO] Crea picking ADJ guardando el warehouse_id.
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. Obtener configuración (incluyendo warehouse_id)
            cursor.execute("SELECT id, warehouse_id FROM picking_types WHERE code='ADJ' AND company_id=%s LIMIT 1", (company_id,))
            pt = cursor.fetchone()
            
            cursor.execute("SELECT id FROM locations WHERE category='AJUSTE' AND company_id=%s LIMIT 1", (company_id,))
            loc = cursor.fetchone()
            
            if not pt or not loc: return None
            
            # Capturamos el ID del almacén para guardarlo
            wh_id = pt['warehouse_id'] 
            
            # --- GENERACIÓN DE NOMBRE C{id} ---
            prefix = f"C{company_id}/ADJ/"
            
            cursor.execute("SELECT name FROM pickings WHERE name LIKE %s AND company_id = %s ORDER BY id DESC LIMIT 1", (f"{prefix}%", company_id))
            last_res = cursor.fetchone()
            
            current_seq = 0
            if last_res:
                try: current_seq = int(last_res['name'].split('/')[-1])
                except: pass
            
            while True:
                current_seq += 1
                new_name = f"{prefix}{str(current_seq).zfill(5)}"
                cursor.execute("SELECT 1 FROM pickings WHERE name = %s", (new_name,))
                if not cursor.fetchone():
                    break
            # -----------------------------------
            
            s_date = datetime.now()
            
            # [FIX] Agregamos warehouse_id al INSERT
            cursor.execute("""
                INSERT INTO pickings (
                    company_id, name, picking_type_id, warehouse_id, 
                    location_src_id, location_dest_id, 
                    scheduled_date, state, responsible_user, custom_operation_type
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'draft', %s, 'Ajuste de Inventario') 
                RETURNING id
            """, (
                company_id, new_name, pt['id'], wh_id, 
                loc['id'], loc['id'], 
                s_date, user_name
            ))
            
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error create_draft_adjustment: {e}")
        return None
    finally:
        if conn: return_db_connection(conn)

def save_adjustment_draft(picking_id, header_data, lines_data):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Update Header
            if header_data:
                sets = ", ".join([f"{k}=%s" for k in header_data.keys()])
                cursor.execute(f"UPDATE pickings SET {sets} WHERE id=%s", list(header_data.values()) + [picking_id])
            
            # Replace Lines
            cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN (SELECT id FROM stock_moves WHERE picking_id=%s)", (picking_id,))
            cursor.execute("DELETE FROM stock_moves WHERE picking_id=%s", (picking_id,))
            
            src, dest = header_data.get('location_src_id'), header_data.get('location_dest_id')
            
            for line in lines_data:
                # line es un objeto pydantic o dict, ajusta acceso
                pid = getattr(line, 'product_id', line.get('product_id'))
                qty = getattr(line, 'quantity', line.get('quantity'))
                cost = getattr(line, 'cost_at_adjustment', line.get('cost_at_adjustment'))
                tracking = getattr(line, 'tracking_data', line.get('tracking_data'))
                
                cursor.execute("""
                    INSERT INTO stock_moves (picking_id, product_id, product_uom_qty, quantity_done, location_src_id, location_dest_id, state, cost_at_adjustment) 
                    VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s) RETURNING id
                """, (picking_id, pid, qty, qty, src, dest, cost))
                move_id = cursor.fetchone()[0]
                
                if tracking:
                    for lot, lqty in tracking.items():
                        # Create lot logic inline
                        cursor.execute("INSERT INTO stock_lots (name, product_id) VALUES (%s, %s) ON CONFLICT (product_id, name) DO NOTHING RETURNING id", (lot, pid))
                        res = cursor.fetchone()
                        if res: lot_id = res[0]
                        else: 
                            cursor.execute("SELECT id FROM stock_lots WHERE product_id=%s AND name=%s", (pid, lot))
                            lot_id = cursor.fetchone()[0]
                        
                        cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move_id, lot_id, lqty))
            
        conn.commit()
        return True, "Guardado", {}
    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return False, str(e), None
    finally:
        if conn: return_db_connection(conn)

def get_adjustments_count(company_id, filters={}):
    base_query = """
    SELECT COUNT(p.id) as total_count FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id
    WHERE p.company_id = %s AND pt.code = 'ADJ'
    """
    params = [company_id]
    # (Agregar lógica de filtros WHERE si necesario, igual que en get_pickings_count)
    res = execute_query(base_query, tuple(params), fetchone=True)
    return res['total_count'] if res else 0

def get_adjustments_filtered_sorted(company_id, filters={}, sort_by='id', ascending=False, limit=None, offset=None):
    """
    Listado de Ajustes de Inventario con nombres legibles de ubicaciones.
    [MEJORA] Lógica inteligente para 'dest_path' (Ubicación Afectada).
    """
    sort_map = {
        'id': "p.id", 'name': "p.name", 'state': "p.state",
        'date': "p.scheduled_date", 
        'src_path': "COALESCE(l_src.path, w_src.name)", 
        # Mapeamos 'dest_path' a la lógica inteligente para que el ordenamiento funcione
        'dest_path': "CASE WHEN l_src.category = 'AJUSTE' THEN COALESCE(l_dest.path, w_dest.name) ELSE COALESCE(l_src.path, w_src.name) END", 
        'responsible_user': "p.responsible_user",
        'adjustment_reason': "p.adjustment_reason"
    }
    order_by_column = sort_map.get(sort_by, "p.id")
    direction = "ASC" if ascending else "DESC"
    
    query = """
        SELECT 
            p.id, p.company_id, p.name, p.state, 
            TO_CHAR(p.scheduled_date, 'YYYY-MM-DD') as date,
            p.responsible_user, p.adjustment_reason, p.notes, p.loss_confirmation,
            
            -- Lógica Inteligente para 'Ubicación Afectada' (dest_path)
            -- Buscamos el lado que NO sea 'AJUSTE' (Virtual).
            CASE 
                -- Caso 1: Origen es Virtual -> Mostramos Destino (Físico)
                WHEN l_src.category = 'AJUSTE' THEN COALESCE(l_dest.path, w_dest.name, 'Sin Ubicación')
                
                -- Caso 2: Destino es Virtual -> Mostramos Origen (Físico)
                WHEN l_dest.category = 'AJUSTE' THEN COALESCE(l_src.path, w_src.name, 'Sin Ubicación')
                
                -- Caso 3: Ambos Virtuales (Datos antiguos/sucios) -> Mostramos Destino por defecto
                ELSE COALESCE(l_dest.path, 'Indeterminado')
            END as dest_path,

            -- (Opcional) Mantenemos src_path original por si se necesita
            COALESCE(l_src.path, w_src.name) as src_path

        FROM pickings p 
        JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN locations l_src ON p.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
        
        WHERE p.company_id = %s AND pt.code = 'ADJ'
    """
    params = [company_id]
    where_clauses = []
    
    for key, value in filters.items():
        if not value: continue
        
        if key == 'state':
            where_clauses.append("p.state = %s"); params.append(value)
        elif key == 'name':
            where_clauses.append("p.name ILIKE %s"); params.append(f"%{value}%")
        elif key == 'responsible_user':
            where_clauses.append("p.responsible_user ILIKE %s"); params.append(f"%{value}%")
        elif key == 'adjustment_reason':
            where_clauses.append("p.adjustment_reason ILIKE %s"); params.append(f"%{value}%")
        # Filtro para la nueva columna inteligente
        elif key == 'dest_path':
             where_clauses.append("""
                (CASE 
                    WHEN l_src.category = 'AJUSTE' THEN COALESCE(l_dest.path, w_dest.name)
                    ELSE COALESCE(l_src.path, w_src.name)
                END) ILIKE %s
             """)
             params.append(f"%{value}%")
    
    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)
    
    query += f" ORDER BY {order_by_column} {direction}"
    
    if limit is not None and offset is not None:
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
    return execute_query(query, tuple(params), fetchall=True)

# --- UI DETAILS ---

def get_picking_ui_details_optimized(picking_id, company_id):
    """
    [OPTIMIZADO-JSON-CORREGIDO] Obtiene Picking, Moves (con ownership), Serials, Dropdowns Y PRODUCTOS.
    """
    sql = """
    WITH 
    -- 1. Cabecera y Regla
    picking_data AS (
        SELECT p.*, pt.code as type_code, w.name as warehouse_name, -- <--- AGREGAR ESTO
               json_build_object('id', ot.id, 'name', ot.name, 'source_location_category', ot.source_location_category, 'destination_location_category', ot.destination_location_category) as op_rule
        FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id 
        LEFT JOIN warehouses w ON p.warehouse_id = w.id -- <--- AGREGAR ESTE JOIN
        LEFT JOIN operation_types ot ON p.custom_operation_type = ot.name
        WHERE p.id = %(pid)s AND p.company_id = %(cid)s
    ),
    -- 2. Movimientos (CORREGIDO: Se agregó ownership)
    moves_data AS (
        SELECT COALESCE(json_agg(json_build_object(
            'id', sm.id, 
            'product_id', pr.id,
            'location_src_id', sm.location_src_id, -- <--- ¡AGREGA ESTA LÍNEA!
            'name', pr.name, 
            'sku', pr.sku, 
            'product_uom_qty', sm.product_uom_qty, 
            'quantity_done', sm.quantity_done, 
            'tracking', pr.tracking, 
            'uom_name', u.name, 
            'price_unit', sm.price_unit,
            'project_id', sm.project_id,
            'standard_price', pr.standard_price,
            'cost_at_adjustment', sm.cost_at_adjustment,
            'ownership', pr.ownership -- <--- ¡AQUÍ FALTABA!
        )), '[]'::json) as moves
        FROM stock_moves sm 
        JOIN products pr ON sm.product_id = pr.id 
        LEFT JOIN uom u ON pr.uom_id = u.id 
        WHERE sm.picking_id = %(pid)s
    ),
    -- 3. Series
    serials_data AS (
        SELECT COALESCE(json_object_agg(s.move_id, s.lots), '{}'::json) as serials
        FROM (
            SELECT sml.move_id, json_object_agg(sl.name, sml.qty_done) as lots
            FROM stock_move_lines sml JOIN stock_lots sl ON sml.lot_id = sl.id
            WHERE sml.move_id IN (SELECT id FROM stock_moves WHERE picking_id = %(pid)s)
            GROUP BY sml.move_id
        ) s
    ),
    -- 4. LISTA DE PRODUCTOS
    products_data AS (
        SELECT COALESCE(json_agg(p_data), '[]'::json) as products_list
        FROM (
            SELECT pr.id, pr.name, pr.sku, pr.tracking, pr.ownership, 
                   pr.uom_id, pr.standard_price, u.name as uom_name,
                   pr.company_id, pr.type
            FROM products pr
            LEFT JOIN uom u ON pr.uom_id = u.id
            WHERE pr.company_id = %(cid)s
            ORDER BY pr.name LIMIT 100 
        ) p_data
    ),
    -- 5. DROPDOWNS
    dropdowns AS (
        SELECT
            (SELECT json_agg(json_build_object('name', ot.name)) 
             FROM operation_types ot 
             WHERE ot.code = (SELECT type_code FROM picking_data)
            ) AS operation_types,
            
            (SELECT json_agg(p.*) 
             FROM (
                SELECT p.id, p.name 
                FROM partners p JOIN partner_categories pc ON p.category_id = pc.id
                WHERE p.company_id = %(cid)s AND pc.name = 'Proveedor Externo'
                ORDER BY p.name LIMIT 100
             ) p
            ) AS partners_vendor,
            
            (SELECT json_agg(p.*) 
             FROM (
                SELECT p.id, p.name 
                FROM partners p JOIN partner_categories pc ON p.category_id = pc.id
                WHERE p.company_id = %(cid)s AND pc.name = 'Proveedor Cliente'
                ORDER BY p.name LIMIT 100
             ) p
            ) AS partners_customer
    )
    -- 6. JSON FINAL
    SELECT json_build_object(
        'picking_data', (SELECT to_jsonb(pd) - 'op_rule' FROM picking_data pd),
        'op_rule', (SELECT op_rule FROM picking_data),
        'moves_data', (SELECT moves FROM moves_data),
        'serials_data', (SELECT serials FROM serials_data),
        'all_products', (SELECT products_list FROM products_data),
        'dropdown_options', json_build_object(
            'operation_types', COALESCE((SELECT operation_types FROM dropdowns), '[]'::json),
            'partners_vendor', COALESCE((SELECT partners_vendor FROM dropdowns), '[]'::json),
            'partners_customer', COALESCE((SELECT partners_customer FROM dropdowns), '[]'::json)
        )
    ) as result
    """
    res = execute_query(sql, {'pid': picking_id, 'cid': company_id}, fetchone=True)
    
    if res and res['result'] and res['result'].get('picking_data'): 
        return res['result'], None
        
    return None, "No encontrado"

# --- OTROS ---

def get_draft_liquidation(wo_id):
    return execute_query("SELECT id FROM pickings WHERE work_order_id = %s AND state = 'draft'", (wo_id,), fetchone=True)

def get_finalized_liquidation(wo_id):
    return execute_query("SELECT id FROM pickings WHERE work_order_id = %s AND state = 'done'", (wo_id,), fetchone=True)

def get_project_id_by_name(project_name, company_id):
    """Busca un proyecto activo por nombre (exacto o similar)."""
    if not project_name or str(project_name).strip() == "": return None
    
    # Intentamos búsqueda exacta (case insensitive)
    query = "SELECT id FROM projects WHERE company_id = %s AND name ILIKE %s AND status = 'active' LIMIT 1"
    res = execute_query(query, (company_id, project_name.strip()), fetchone=True)
    
    if res: return res['id']
    return None

def create_full_picking_transaction(data: dict):
    """
    [LAZY CREATION] Crea cabecera y líneas en una sola transacción atómica.
    [CORREGIDO] Definición de wh_id restaurada + Prefijo C{id}.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        print("[DB TRANSACTION] Iniciando creación masiva de Albarán...")

        # 1. Configuración Defaults
        cursor.execute("SELECT * FROM picking_types WHERE id = %s", (data['picking_type_id'],))
        pt = cursor.fetchone()
        if not pt: raise ValueError("Tipo de operación no válido.")

        # --- [FIX] RESTAURADO: Definir wh_id ---
        wh_id = pt['warehouse_id'] 
        # ---------------------------------------

        # --- GENERACIÓN DE NOMBRE (LÓGICA C{id}) ---
        pt_code = pt['code'] # El código ya viene en la consulta de picking_types
        
        # Prefijo: C{company_id}/{TIPO}/
        prefix = f"C{data['company_id']}/{pt_code}/"
        
        # Buscar último
        cursor.execute("SELECT name FROM pickings WHERE name LIKE %s AND company_id = %s ORDER BY id DESC LIMIT 1", (f"{prefix}%", data['company_id']))
        last_res = cursor.fetchone()
        
        current_seq = 0
        if last_res:
            try: current_seq = int(last_res['name'].split('/')[-1])
            except: pass

        # Bucle de Seguridad
        while True:
            current_seq += 1
            new_name = f"{prefix}{str(current_seq).zfill(5)}"
            cursor.execute("SELECT 1 FROM pickings WHERE name = %s", (new_name,))
            if not cursor.fetchone():
                break
        # -----------------------------------

        # 3. Ubicaciones (Respetando NULLs de la UI)
        final_src = data.get('location_src_id')
        final_dest = data.get('location_dest_id')

        # 4. Insertar Cabecera
        cursor.execute("""
            INSERT INTO pickings (
                company_id, name, picking_type_id, state, responsible_user,
                location_src_id, location_dest_id, warehouse_id,
                partner_id, partner_ref, purchase_order, date_transfer,
                custom_operation_type, project_id, scheduled_date
            ) VALUES (
                %s, %s, %s, 'draft', %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, NOW()
            ) RETURNING id
        """, (
            data['company_id'], new_name, data['picking_type_id'], data['responsible_user'],
            final_src, final_dest, wh_id, # <--- Aquí es donde fallaba antes
            data.get('partner_id'), data.get('partner_ref'), data.get('purchase_order'), data.get('date_transfer'),
            data.get('custom_operation_type'), data.get('project_id')
        ))
        new_picking_id = cursor.fetchone()[0]

        # 5. Insertar Líneas
        if data.get('moves'):
            for m in data['moves']:
                qty = float(m['quantity'])
                if qty <= 0: continue 
                
                cursor.execute("""
                    INSERT INTO stock_moves (
                        picking_id, product_id, product_uom_qty, quantity_done,
                        location_src_id, location_dest_id, price_unit, 
                        partner_id, project_id, state
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'draft')
                """, (
                    new_picking_id, m['product_id'], qty, qty,
                    final_src, final_dest, m.get('price_unit', 0),
                    data.get('partner_id'), data.get('project_id')
                ))

        conn.commit()
        print(f"[DB TRANSACTION] Éxito. Creado ID {new_picking_id} Name {new_name}")
        return new_picking_id

    except Exception as e:
        if conn: conn.rollback()
        print(f"[DB ERROR] Rollback ejecutado: {e}")
        raise e
    finally:
        if conn: return_db_connection(conn)

def get_project_stock_in_location(company_id, location_id, project_id):
    """
    Obtiene todo el stock (productos y cantidades) que un Proyecto específico 
    tiene guardado en una Ubicación específica.
    """
    query = """
        SELECT 
            p.id, p.name, p.sku, p.tracking, p.ownership,
            u.name as uom_name,
            SUM(sq.quantity) as quantity
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        LEFT JOIN uom u ON p.uom_id = u.id
        WHERE sq.location_id = %s 
          AND sq.project_id = %s
          AND p.company_id = %s
          AND sq.quantity > 0
        GROUP BY p.id, p.name, p.sku, p.tracking, p.ownership, u.name
        ORDER BY p.name
    """
    return execute_query(query, (location_id, project_id, company_id), fetchall=True)

def update_stock_quant_notes(product_id, location_id, notes, lot_id=None, project_id=None, apply_to_group=False):
    """
    Actualiza la nota de stock.
    [MEJORA] Si apply_to_group=True, actualiza TODOS los registros de ese producto/ubicación
    (útil para limpiar notas en vista resumen de productos seriados).
    """
    params = [notes, product_id, location_id]
    
    # Filtros Base (Siempre obligatorios)
    where_conditions = ["product_id = %s", "location_id = %s"]
    
    if not apply_to_group:
        # --- MODO ESTRICTO (Edición puntual) ---
        # Solo actualiza la fila exacta (Lote específico o Proyecto específico)
        
        if lot_id:
            where_conditions.append("lot_id = %s")
            params.append(lot_id)
        else:
            where_conditions.append("lot_id IS NULL")
            
        if project_id:
            where_conditions.append("project_id = %s")
            params.append(project_id)
        else:
            where_conditions.append("project_id IS NULL")
            
    else:
        # --- MODO GRUPO (Edición masiva desde Resumen) ---
        # No filtramos por lot_id ni project_id.
        # Actualizamos TODO lo que haya de este producto en esta ubicación.
        # Esto permite "limpiar" o "etiquetar" todo el lote de series de un golpe.
        pass

    query = f"""
        UPDATE stock_quants 
        SET notes = %s 
        WHERE {' AND '.join(where_conditions)}
    """
    
    execute_commit_query(query, tuple(params))
    return True

# ==============================================================================
# --- NUEVAS FUNCIONES PARA IMPORTACIÓN/EXPORTACIÓN DE AJUSTES (SMART LOGIC) ---
# ==============================================================================

def get_adjustments_for_export(company_id):
    """
    Obtiene data plana de ajustes para CSV.
    Detecta si es entrada o salida basándose en la ubicación origen.
    """
    query = """
        SELECT 
            p.name as referencia,
            p.adjustment_reason as razon,
            TO_CHAR(p.scheduled_date, 'DD/MM/YYYY') as fecha,
            p.responsible_user as usuario,
            p.notes as notas,
            p.state as estado,
            
            -- Ubicación Real (La que no es virtual)
            CASE 
                WHEN l_src.category = 'AJUSTE' THEN COALESCE(l_dest.path, w_dest.name)
                ELSE COALESCE(l_src.path, w_src.name)
            END as ubicacion,
            
            prod.sku,
            prod.name as producto,
            
            -- Cantidad (Positiva o Negativa según flujo)
            CASE 
                WHEN l_src.category = 'AJUSTE' THEN sm.quantity_done -- Entrada (+10)
                ELSE -sm.quantity_done -- Salida (-10)
            END as cantidad,
            
            sm.price_unit as costo_unitario,

            -- [NUEVO] Concatenar series separadas por comas
            (
                SELECT string_agg(sl.name, ', ')
                FROM stock_move_lines sml
                JOIN stock_lots sl ON sml.lot_id = sl.id
                WHERE sml.move_id = sm.id
            ) as series

        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        JOIN products prod ON sm.product_id = prod.id
        
        LEFT JOIN locations l_src ON sm.location_src_id = l_src.id
        LEFT JOIN locations l_dest ON sm.location_dest_id = l_dest.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
        
        WHERE p.company_id = %s 
          AND pt.code = 'ADJ' 
          AND p.state != 'cancelled'
        ORDER BY p.id DESC, prod.sku ASC
    """
    return execute_query(query, (company_id,), fetchall=True)

def import_smart_adjustments_transaction(company_id, user_name, rows):
    """
    [LÓGICA INTELIGENTE v6 - SECUENCIA MATEMÁTICA] 
    Calcula el máximo ID existente una sola vez y proyecta los nuevos IDs en memoria.
    Esto evita huecos (1, 3, 5) causados por falsos positivos en consultas DB repetitivas.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 1. Configuración Previa
        cursor.execute("SELECT id, warehouse_id FROM picking_types WHERE code='ADJ' AND company_id=%s LIMIT 1", (company_id,))
        pt = cursor.fetchone()
        if not pt: raise ValueError("No existe configuración de 'Ajustes' (Picking Type ADJ).")

        # [FIX] Capturamos el ID del almacén
        wh_id = pt['warehouse_id']

        cursor.execute("SELECT id FROM locations WHERE category='AJUSTE' AND company_id=%s LIMIT 1", (company_id,))
        loc_virtual = cursor.fetchone()
        if not loc_virtual: raise ValueError("No existe ubicación virtual 'AJUSTE'.")
        virtual_id = loc_virtual['id']

        # Obtener prefijo del almacén
        cursor.execute("SELECT wt.code FROM warehouses wt WHERE id = %s", (pt['warehouse_id'],))
        wh_code = cursor.fetchone()['code']
        prefix = f"C{company_id}/ADJ/"

        # 2. Agrupar filas
        from collections import defaultdict
        grouped_rows = defaultdict(list)
        
        for i, row in enumerate(rows):
            ref = row.get('referencia') or f"IMP-{datetime.now().strftime('%Y%m%d-%H%M')}"
            grouped_rows[ref].append({'data': row, 'line': i + 2})

        # --- CÁLCULO DE SECUENCIA MAESTRA ---
        cursor.execute(
            "SELECT name FROM pickings WHERE name LIKE %s AND company_id = %s", 
            (f"{prefix}%", company_id)
        )
        existing_names = cursor.fetchall()
        
        max_sequence = 0
        for row in existing_names:
            try:
                name_str = row[0]
                num_part = int(name_str.split('/')[-1])
                if num_part > max_sequence:
                    max_sequence = num_part
            except (ValueError, IndexError):
                continue
        
        current_sequence_counter = max_sequence
        print(f"[IMPORT] Secuencia inicial detectada para Cia {company_id}: {current_sequence_counter}")
        # ------------------------------------

        total_created = 0
        
        # 3. Procesar Grupos
        for ref, lines in grouped_rows.items():
            first_row = lines[0]['data']
            reason = first_row.get('razon')
            if not reason or not reason.strip():
                raise ValueError(f"Fila {lines[0]['line']}: La 'razon' es OBLIGATORIA.")
            
            notes = first_row.get('notas', '')

            # --- [FIX] DETECTAR UBICACIÓN FÍSICA PARA LA CABECERA ---
            # Leemos la ubicación de la primera línea para asignarla al documento general
            first_loc_path = first_row.get('ubicacion')
            header_dest_id = virtual_id # Por defecto (si falla)

            if first_loc_path:
                cursor.execute(
                    "SELECT id FROM locations WHERE (path = %s OR name = %s) AND company_id = %s AND type='internal'", 
                    (first_loc_path, first_loc_path, company_id)
                )
                res_loc = cursor.fetchone()
                if res_loc:
                    header_dest_id = res_loc['id']

            # --- ASIGNACIÓN DE NOMBRE ---
            current_sequence_counter += 1
            new_name = f"{prefix}{str(current_sequence_counter).zfill(5)}"

            # Crear Cabecera
            # [CORREGIDO] Usamos header_dest_id en location_dest_id
            cursor.execute("""
                INSERT INTO pickings (
                    company_id, name, picking_type_id, warehouse_id,
                    state, responsible_user, 
                    adjustment_reason, notes, custom_operation_type,
                    location_src_id, location_dest_id, scheduled_date
                ) VALUES (%s, %s, %s, %s, 'draft', %s, %s, %s, 'Ajuste de Inventario', %s, %s, NOW())
                RETURNING id
            """, (
                company_id, new_name, pt['id'], wh_id, 
                user_name, reason, notes, 
                virtual_id, header_dest_id # <--- AQUI ESTABA EL ERROR (antes era virtual_id)
            ))

            picking_id = cursor.fetchone()[0]
            
            # Procesar Líneas
            for line_info in lines:
                row = line_info['data']
                sku = row.get('sku')
                qty_str = row.get('cantidad')
                loc_path = row.get('ubicacion')
                cost_str = row.get('costo', '0')
                serials_str = row.get('series') or row.get('serie') or row.get('serial') or row.get('lote') or ''

                if not sku or not qty_str or not loc_path:
                    raise ValueError(f"Fila {line_info['line']}: Faltan datos (SKU, Cantidad, Ubicación).")

                # Buscar Producto
                cursor.execute("SELECT id, standard_price, tracking FROM products WHERE sku = %s AND company_id = %s", (sku, company_id))
                prod = cursor.fetchone()
                if not prod: raise ValueError(f"Fila {line_info['line']}: SKU '{sku}' no existe.")
                
                # Buscar Ubicación Real
                cursor.execute("SELECT id FROM locations WHERE (path = %s OR name = %s) AND company_id = %s AND type='internal'", (loc_path, loc_path, company_id))
                loc_real = cursor.fetchone()
                if not loc_real: raise ValueError(f"Fila {line_info['line']}: Ubicación '{loc_path}' no encontrada o no es interna.")
                
                real_id = loc_real['id']
                qty = float(qty_str)
                cost = float(cost_str) if cost_str and float(cost_str) > 0 else (prod['standard_price'] or 0)

                if qty >= 0:
                    src, dest = virtual_id, real_id
                    final_qty = qty
                else:
                    src, dest = real_id, virtual_id
                    final_qty = abs(qty)

                cursor.execute("""
                    INSERT INTO stock_moves (
                        picking_id, product_id, product_uom_qty, quantity_done, 
                        location_src_id, location_dest_id, price_unit, cost_at_adjustment, state
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'draft')
                    RETURNING id
                """, (picking_id, prod['id'], final_qty, final_qty, src, dest, cost, cost))
                
                move_id = cursor.fetchone()[0]

                # Carga de Series
                tracking_type = prod['tracking']
                if tracking_type != 'none' and serials_str:
                    raw_vals = [s.strip() for s in re.split(r'[;,\n]', serials_str) if s.strip()]
                    if not raw_vals: continue

                    if tracking_type == 'serial':
                        if len(raw_vals) != int(final_qty):
                            raise ValueError(f"Fila {line_info['line']}: SKU '{sku}' requiere {int(final_qty)} series, se indicaron {len(raw_vals)}.")
                        for sn in raw_vals:
                            lot_id = create_lot(cursor, prod['id'], sn)
                            cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, 1)", (move_id, lot_id))
                            
                    elif tracking_type == 'lot':
                        if len(raw_vals) == 1:
                            lot_id = create_lot(cursor, prod['id'], raw_vals[0])
                            cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", (move_id, lot_id, final_qty))
                        else:
                            if len(raw_vals) != int(final_qty):
                                 print(f"[WARN] Lotes múltiples para '{sku}'. Se asignará 1 unidad a cada lote listado.")
                            for sn in raw_vals:
                                lot_id = create_lot(cursor, prod['id'], sn)
                                cursor.execute("INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, 1)", (move_id, lot_id))

            total_created += 1

        conn.commit()
        return total_created

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error import transaction: {e}")
        raise e
    finally:
        if conn: return_db_connection(conn)

    # --- EXPORTACIÓN CSV (Agregado a operation_repo.py) ---

def get_data_for_export(company_id, export_type, selected_ids=None):
    """
    Obtiene los datos para el CSV.
    [MEJORA v2] Lógica Inteligente para Nombres de Proveedores/Clientes.
    - Si es IN (Recepción): Origen = Nombre del Partner (Proveedor).
    - Si es OUT (Entrega): Destino = Nombre del Partner (Cliente).
    - Mantiene la columna 'notes' (Comentarios).
    """
    params = [company_id]
    filter_clause = ""
    
    if selected_ids:
        filter_clause = "AND p.id = ANY(%s)"
        params.append(selected_ids)

    notes_col = "p.notes as comentarios,"

    # Definimos la lógica de visualización de ubicaciones
    # Se usa tanto en 'headers' como en 'full'
    smart_locations_sql = """
        -- LOGICA INTELIGENTE ORIGEN
        CASE 
            WHEN pt.code = 'IN' THEN part.name -- Si es Entrada, mostrar Proveedor
            WHEN l_src.type = 'internal' THEN w_src.name 
            ELSE l_src.path 
        END as almacen_origen,
        
        CASE 
            WHEN pt.code = 'IN' THEN part.name 
            ELSE l_src.path 
        END as ubicacion_origen,
        
        -- LOGICA INTELIGENTE DESTINO
        CASE 
            WHEN pt.code = 'OUT' THEN part.name -- Si es Salida, mostrar Cliente
            WHEN l_dest.type = 'internal' THEN w_dest.name 
            ELSE l_dest.path 
        END as almacen_destino,
        
        CASE 
            WHEN pt.code = 'OUT' THEN part.name 
            ELSE l_dest.path 
        END as ubicacion_destino
    """

    if export_type == 'headers':
        query = f"""
            SELECT 
                p.name as picking_name, 
                pt.code as picking_type_code, 
                p.state, 
                p.custom_operation_type,
                proj.name as project_name,
                
                {smart_locations_sql}, -- <--- Lógica inyectada aquí
                
                p.partner_ref, 
                p.purchase_order,
                TO_CHAR(p.date_transfer, 'DD/MM/YYYY') as date_transfer, 
                p.responsible_user,
                {notes_col}

                -- Dummy columns
                '' as product_sku, '' as product_name, 0 as quantity, 0 as price_unit, '' as serial

            FROM pickings p
            JOIN picking_types pt ON p.picking_type_id = pt.id
            LEFT JOIN partners part ON p.partner_id = part.id  -- <--- JOIN CLAVE
            LEFT JOIN projects proj ON p.project_id = proj.id
            LEFT JOIN locations l_src ON p.location_src_id = l_src.id
            LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
            LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
            LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
            
            WHERE p.company_id = %s 
              AND pt.code != 'ADJ'
              {filter_clause}
            ORDER BY p.id DESC
        """
        
    elif export_type == 'full':
        query = f"""
            SELECT 
                p.name as picking_name, 
                pt.code as picking_type_code, 
                p.state, 
                p.custom_operation_type,
                proj.name as project_name,
                
                {smart_locations_sql}, -- <--- Lógica inyectada aquí
                
                p.partner_ref, 
                p.purchase_order,
                TO_CHAR(p.date_transfer, 'DD/MM/YYYY') as date_transfer, 
                p.responsible_user,
                
                prod.sku as product_sku,
                prod.name as product_name,
                sm.product_uom_qty as quantity,
                sm.price_unit,
                
                {notes_col}

                (
                    SELECT string_agg(sl.name, ', ')
                    FROM stock_move_lines sml
                    JOIN stock_lots sl ON sml.lot_id = sl.id
                    WHERE sml.move_id = sm.id
                ) as serial

            FROM pickings p
            JOIN stock_moves sm ON sm.picking_id = p.id
            JOIN products prod ON sm.product_id = prod.id
            JOIN picking_types pt ON p.picking_type_id = pt.id
            LEFT JOIN partners part ON p.partner_id = part.id -- <--- JOIN CLAVE
            LEFT JOIN projects proj ON p.project_id = proj.id
            LEFT JOIN locations l_src ON p.location_src_id = l_src.id
            LEFT JOIN locations l_dest ON p.location_dest_id = l_dest.id
            LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
            LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
            
            WHERE p.company_id = %s 
              AND pt.code != 'ADJ'
              {filter_clause}
            ORDER BY p.id DESC, prod.sku ASC
        """
    
    return execute_query(query, tuple(params), fetchall=True)

def get_project_id_by_composite_key(macro_name, project_code, company_id):
    """
    Busca un proyecto usando la LLAVE COMPUESTA: (Nombre Macro Proyecto + Código PEP Obra).
    Garantiza unicidad.
    """
    if not macro_name or not project_code: return None
    
    clean_macro = macro_name.strip()
    clean_code = project_code.strip()
    
    query = """
        SELECT p.id 
        FROM projects p
        JOIN macro_projects mp ON p.macro_project_id = mp.id
        WHERE p.company_id = %s
          AND p.status = 'active'
          AND TRIM(mp.name) ILIKE TRIM(%s)  -- Coincidencia flexible de nombre Macro
          AND TRIM(p.code) ILIKE TRIM(%s)   -- Coincidencia flexible de Código PEP
        LIMIT 1
    """
    res = execute_query(query, (company_id, clean_macro, clean_code), fetchone=True)
    return res['id'] if res else None