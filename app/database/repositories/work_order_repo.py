#app/database/repositories/work_order_repo.py
import psycopg2
import psycopg2.extras
import traceback
from ..core import get_db_connection, return_db_connection, execute_query, execute_commit_query
from collections import defaultdict
from . import operation_repo

# --- CRUD BÁSICO (Lectura/Creación) ---

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
    return execute_query(query, (company_id,), fetchall=True)

def get_work_order_details(wo_id): 
    return execute_query("SELECT * FROM work_orders WHERE id = %s", (wo_id,), fetchone=True)

def create_work_order(company_id, ot_number, customer, address, service, job_type, project_id=None):
    print(f"[DB-DEBUG] Creando WO: OT={ot_number}, Proj={project_id}, Comp={company_id}")
    query = """
        INSERT INTO work_orders
            (company_id, ot_number, customer_name, address, service_type, job_type, project_id, phase)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'Sin Liquidar')
        RETURNING id
    """
    params = (company_id, ot_number, customer, address, service, job_type, project_id)
    try:
        result = execute_commit_query(query, params, fetchone=True)
        return result[0]
    except Exception as e: 
        if "work_orders_ot_number_key" in str(e): 
            raise ValueError(f"La Orden de Trabajo '{ot_number}' ya existe.")
        raise e

# --- LÓGICA DE NEGOCIO: LIQUIDACIÓN (Migrada de operation_repo) ---

def _create_or_update_draft_picking_internal(cursor, wo_id, code, data, company_id, user):
    """
    Crea/Actualiza los pickings asociados a la liquidación.
    [FIX APLICADO] Transfiere el project_id de la OT al Picking y sus Moves.
    """
    wh_id = data['warehouse_id']
    
    # 1. Obtener config tipo de operación
    cursor.execute(
        "SELECT id, default_location_src_id, default_location_dest_id FROM picking_types WHERE warehouse_id = %s AND code = %s", 
        (wh_id, code)
    )
    pt = cursor.fetchone()
    if not pt: 
        raise ValueError(f"Configuración faltante: No hay tipo de operación '{code}' para el almacén ID {wh_id}.")

    # --- [FIX] OBTENER EL PROJECT_ID DE LA WO ---
    cursor.execute("SELECT project_id FROM work_orders WHERE id = %s", (wo_id,))
    wo_row = cursor.fetchone()
    project_id = wo_row['project_id'] if wo_row else None
    # --------------------------------------------
    
    # 2. Buscar picking existente
    cursor.execute(
        """SELECT p.id FROM pickings p 
           JOIN picking_types pt ON p.picking_type_id = pt.id 
           WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = %s""", 
        (wo_id, code)
    )
    exist = cursor.fetchone()
    
    pid = None
    if exist:
        pid = exist['id']
        cursor.execute(
            """UPDATE pickings SET 
               warehouse_id=%s, location_src_id=%s, location_dest_id=%s, 
               attention_date=%s, service_act_number=%s, responsible_user=%s,
               project_id=%s -- [FIX] Actualizar proyecto
               WHERE id=%s""", 
            (wh_id, pt['default_location_src_id'], pt['default_location_dest_id'], 
             data['date_attended_db'], data['service_act_number'], user, 
             project_id, pid)
        )
    else:
        # Generar nombre
        cursor.execute(
            "SELECT wt.code as wh_code, pt.code as pt_code FROM picking_types pt JOIN warehouses wt ON pt.warehouse_id = wt.id WHERE pt.id = %s", 
            (pt['id'],)
        )
        pt_info = cursor.fetchone()
        prefix = f"{pt_info['wh_code']}/{pt_info['pt_code']}/"
        cursor.execute("SELECT COUNT(*) FROM pickings WHERE name LIKE %s", (f"{prefix}%",))
        count = cursor.fetchone()[0]
        pname = f"{prefix}{str(count + 1).zfill(5)}"
        
        cursor.execute(
            """INSERT INTO pickings (
                company_id, name, picking_type_id, warehouse_id, 
                location_src_id, location_dest_id, state, work_order_id, 
                custom_operation_type, service_act_number, attention_date, responsible_user,
                project_id -- [FIX] Columna nueva
               ) VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s, %s, %s, %s, %s) 
               RETURNING id""",
            (company_id, pname, pt['id'], wh_id, 
             pt['default_location_src_id'], pt['default_location_dest_id'], 
             wo_id, "Liquidación por OT", data['service_act_number'], data['date_attended_db'], user,
             project_id) # [FIX] Valor nuevo
        )
        pid = cursor.fetchone()[0]

    # 3. Reemplazar líneas (Borrar e Insertar)
    cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN (SELECT id FROM stock_moves WHERE picking_id = %s)", (pid,))
    cursor.execute("DELETE FROM stock_moves WHERE picking_id = %s", (pid,))
    
    moves_tracking = {}
    for line in data['lines_data']:
        # Validar que line sea dict o objeto
        p_id = line.get('product_id') if isinstance(line, dict) else line.product_id
        qty = line.get('quantity') if isinstance(line, dict) else line.quantity
        
        # --- [FIX PRECIO] OBTENER EL COSTO ACTUAL DEL PRODUCTO ---
        # Consultamos el precio estándar actual para valorizar la salida
        cursor.execute("SELECT standard_price FROM products WHERE id = %s", (p_id,))
        res_price = cursor.fetchone()
        cost_price = res_price['standard_price'] if res_price else 0.0
        # ---------------------------------------------------------

        cursor.execute(
            """INSERT INTO stock_moves (
                picking_id, product_id, product_uom_qty, quantity_done, 
                location_src_id, location_dest_id, state,
                project_id, 
                price_unit -- [FIX] Guardamos el precio
               ) VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s, %s) 
               RETURNING id""",
            (pid, p_id, qty, qty, pt['default_location_src_id'], pt['default_location_dest_id'],
             project_id, 
             cost_price) # [FIX] Pasamos el precio recuperado
        )
        mid = cursor.fetchone()[0]
        
        tracking = line.get('tracking_data') if isinstance(line, dict) else getattr(line, 'tracking_data', None)
        
        if tracking:
            moves_tracking[mid] = tracking
            for lot_name, lqty in tracking.items():
                # Usar create_lot interno 
                lot_id = operation_repo.create_lot(cursor, p_id, lot_name) 
                cursor.execute(
                    "INSERT INTO stock_move_lines (move_id, lot_id, qty_done) VALUES (%s, %s, %s)", 
                    (mid, lot_id, lqty)
                )
    
    return pid, moves_tracking

def save_liquidation_progress(wo_id, wo_updates: dict, consumo_data: dict, retiro_data: dict, company_id, user_name):
    """Guarda el estado de la liquidación (OT + Consumo + Retiro) atómicamente."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. Actualizar OT
            if wo_updates:
                set_clause = ", ".join([f"{k}=%s" for k in wo_updates.keys()])
                params = list(wo_updates.values()) + [wo_id]
                cursor.execute(f"UPDATE work_orders SET {set_clause} WHERE id=%s", tuple(params))
            
            # 2. Procesar Consumo (OUT)
            if consumo_data:
                _create_or_update_draft_picking_internal(cursor, wo_id, 'OUT', consumo_data, company_id, user_name)
            
            # 3. Procesar Retiro (RET)
            if retiro_data:
                _create_or_update_draft_picking_internal(cursor, wo_id, 'RET', retiro_data, company_id, user_name)
            
            # Borrar RET si está vacío
            if not retiro_data:
                 cursor.execute(
                     "SELECT p.id FROM pickings p JOIN picking_types pt ON p.picking_type_id = pt.id WHERE p.work_order_id = %s AND p.state = 'draft' AND pt.code = 'RET'", 
                     (wo_id,)
                 )
                 old_ret = cursor.fetchone()
                 if old_ret:
                     pid_del = old_ret['id']
                     cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN (SELECT id FROM stock_moves WHERE picking_id=%s)", (pid_del,))
                     cursor.execute("DELETE FROM stock_moves WHERE picking_id=%s", (pid_del,))
                     cursor.execute("DELETE FROM pickings WHERE id=%s", (pid_del,))

        conn.commit()
        return True, "Progreso guardado."
    except Exception as e:
        if conn: conn.rollback()
        traceback.print_exc()
        return False, f"Error al guardar: {e}"
    finally:
        if conn: return_db_connection(conn)

def process_full_liquidation(wo_id, consumptions, retiros, service_act_number, date_attended_db, current_ui_location_id, user_name, company_id):
    """
    [BLINDADO ATÓMICO] Finaliza la liquidación: Guarda, Valida Stocks y Cierra la OT.
    Previene duplicidad por race conditions usando bloqueo de fila.
    """
    print(f"[DB-LIQ-FULL] Finalizando WO {wo_id} (Blindado)...")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            # 1. BLOQUEO ATÓMICO (Critical Section)
            # Bloqueamos la OT para que nadie más pueda tocarla simultáneamente.
            cursor.execute("SELECT id, phase FROM work_orders WHERE id = %s FOR UPDATE", (wo_id,))
            wo_status = cursor.fetchone()

            if not wo_status:
                raise ValueError("La Orden de Trabajo no existe.")
            
            # 2. VERIFICACIÓN DE ESTADO POST-BLOQUEO
            # Si el segundo clic llega aquí, verá que ya está 'Liquidado' y fallará.
            if wo_status['phase'] == 'Liquidado':
                # Mensaje amigable para el frontend (no es un error grave, solo concurrencia)
                return True, "La OT ya había sido liquidada exitosamente por una petición anterior."

            # 3. LÓGICA DE NEGOCIO (Solo si pasamos el bloqueo)
            
            # 3.1 Obtener datos del Almacén origen
            cursor.execute("SELECT warehouse_id FROM locations WHERE id = %s", (current_ui_location_id,))
            loc_row = cursor.fetchone()
            if not loc_row: raise ValueError("Ubicación origen inválida")
            wh_id = loc_row['warehouse_id']

            pid_out, tracking_out = None, {}
            pid_ret, tracking_ret = None, {}

            # 3.2 Guardar Borradores (Pickings)
            if consumptions:
                c_data = {'warehouse_id': wh_id, 'date_attended_db': date_attended_db, 'service_act_number': service_act_number, 'lines_data': consumptions}
                pid_out, tracking_out = _create_or_update_draft_picking_internal(cursor, wo_id, 'OUT', c_data, company_id, user_name)
            
            if retiros:
                r_data = {'warehouse_id': wh_id, 'date_attended_db': date_attended_db, 'service_act_number': service_act_number, 'lines_data': retiros}
                pid_ret, tracking_ret = _create_or_update_draft_picking_internal(cursor, wo_id, 'RET', r_data, company_id, user_name)

            # 3.3 Validar Stocks y Confirmar (Usando la función BLINDADA de operation_repo)
            # Nota: _process_picking_validation_with_cursor TAMBIÉN tiene su propio FOR UPDATE sobre pickings,
            # lo cual está bien (bloqueo en cascada seguro).
            
            if pid_out:
                ok, msg = operation_repo._process_picking_validation_with_cursor(cursor, pid_out, tracking_out)
                if not ok: raise ValueError(f"Error validando Consumo: {msg}")
            
            if pid_ret:
                ok, msg = operation_repo._process_picking_validation_with_cursor(cursor, pid_ret, tracking_ret)
                if not ok: raise ValueError(f"Error validando Retiro: {msg}")

            # 3.4 Cerrar OT (Cambio de estado final)
            cursor.execute("UPDATE work_orders SET phase = 'Liquidado' WHERE id = %s", (wo_id,))

        conn.commit()
        return True, "Liquidación exitosa."

    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR LIQ] {e}")
        # Retornamos el error limpio para que el frontend lo muestre
        return False, str(e)
    finally:
        if conn: return_db_connection(conn)

def update_work_order_fields(wo_id, fields_to_update: dict):
    """
    Actualiza campos específicos de una Orden de Trabajo.
    [BLINDADO] Impide cambios si la OT ya está Liquidada.
    """
    allowed_fields = {
        "customer_name", "address", "warehouse_id", "date_attended",
        "service_type", "job_type", "phase"
    }

    update_dict = {
        k: v for k, v in fields_to_update.items()
        if k in allowed_fields and v is not None
    }

    if not update_dict:
        print(f"[DB-WARN] No se proporcionaron campos válidos para actualizar la OT {wo_id}.")
        return 0

    set_clause = ", ".join(f"{key} = %s" for key in update_dict.keys())
    params = list(update_dict.values()) + [wo_id]

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:

            # 1. CANDADO DE HISTORIA
            cursor.execute(
                "SELECT phase FROM work_orders WHERE id = %s FOR UPDATE",
                (wo_id,)
            )
            res = cursor.fetchone()

            if not res:
                raise ValueError(f"Orden de Trabajo {wo_id} no encontrada.")

            if res[0] == 'Liquidado':
                raise ValueError(
                    "Acción denegada: La Orden de Trabajo está LIQUIDADA y no se puede modificar."
                )

            # 2. UPDATE REAL
            query = f"UPDATE work_orders SET {set_clause} WHERE id = %s"
            cursor.execute(query, tuple(params))

            conn.commit()
            return cursor.rowcount

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] update_work_order_fields OT {wo_id}: {e}")
        raise

    finally:
        if conn:
            return_db_connection(conn)


def create_work_order_from_import(company_id, ot_number, customer, address, service, job_type):
    """
    Crea una OT desde la importación.
    """
    # 1. Validar que la OT no exista previamente
    existing_ot = execute_query(
        "SELECT id FROM work_orders WHERE ot_number = %s AND company_id = %s",
        (ot_number, company_id),
        fetchone=True
    )
    if existing_ot:
        print(f"[DB-IMPORT] OT Omitida (duplicada): {ot_number}")
        return "skipped"

    # 2. Llamar a la función simplificada para crear la OT
    try:
        new_id = create_work_order(
            company_id, ot_number, customer, address, service, job_type
        )
        if new_id:
             print(f"[DB-IMPORT] OT Creada: {ot_number}")
             return "created"
        else:
            print(f"[DB-IMPORT] Error creando OT (posible duplicado no detectado antes): {ot_number}")
            return "error"
    except Exception as e:
         print(f"[DB-IMPORT] Error inesperado creando OT {ot_number}: {e}")
         return "error"

def get_work_orders_for_export(company_id, filters={}):
    """Obtiene TODAS las OTs para exportar (sin paginación)."""
    return get_work_orders_filtered_sorted(company_id, filters, limit=None, offset=None)

def get_work_orders_filtered_sorted(company_id, filters={}, sort_by='id', ascending=False, limit=None, offset=None):
    """
    Obtiene las OTs con paginación, filtros y ordenamiento.
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
    LEFT JOIN projects proj ON wo.project_id = proj.id
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
        wo.project_id,
        proj.name as project_name,
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
    
    for key, value in filters.items():
        if not value: continue
        
        column_map = {
            'id': "wo.id",
            'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
            'job_type': "wo.job_type", 'customer_name': "wo.customer_name",
            'address': "wo.address", 'phase': "wo.phase",
            'warehouse_name': "warehouse_name", 'location_src_path': "location_src_path",
            'service_act_number': "service_act_number"
        }
        db_column = column_map.get(key)
        
        if db_column:
            if key == 'phase' or key == 'id':
                where_clauses.append(f"{db_column} = %s")
                params.append(value)
            else:
                where_clauses.append(f"{db_column} ILIKE %s")
                params.append(f"%{value}%")

    where_string = " WHERE " + " AND ".join(where_clauses)
    final_query = f"{select_clause} {base_query} {where_string} ORDER BY {order_by_column} {direction}"
    
    if limit is not None and offset is not None:
        final_query += " LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

    return execute_query(final_query, tuple(params), fetchall=True)

def get_work_orders_count(company_id, filters={}):
    base_query = """
    FROM work_orders wo
    LEFT JOIN pickings p_draft ON wo.id = p_draft.work_order_id AND p_draft.state = 'draft' AND p_draft.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_draft ON p_draft.warehouse_id = w_draft.id
    LEFT JOIN locations l_draft ON p_draft.location_src_id = l_draft.id
    LEFT JOIN pickings p_done ON wo.id = p_done.work_order_id AND p_done.state = 'done' AND p_done.picking_type_id IN (SELECT id FROM picking_types WHERE code = 'OUT')
    LEFT JOIN warehouses w_done ON p_done.warehouse_id = w_done.id
    LEFT JOIN locations l_done ON p_done.location_src_id = l_done.id
    """
    params = [company_id]
    where_clauses = ["wo.company_id = %s"]
    
    for key, value in filters.items():
        if not value: continue
        column_map = {
            'ot_number': "wo.ot_number", 'service_type': "wo.service_type",
            'job_type': "wo.job_type", 'customer_name': "wo.customer_name",
            'address': "wo.address", 'phase': "wo.phase",
            'warehouse_name': "warehouse_name", 'location_src_path': "location_src_path",
            'service_act_number': "service_act_number"
        }
        db_column = column_map.get(key)
        
        if db_column:
            if key == 'phase':
                where_clauses.append(f"{db_column} = %s"); params.append(value)
            else:
                where_clauses.append(f"{db_column} ILIKE %s"); params.append(f"%{value}%")
                
    where_string = " WHERE " + " AND ".join(where_clauses)
    count_query = f"SELECT COUNT(DISTINCT wo.id) as total_count {base_query} {where_string}"
    
    result = execute_query(count_query, tuple(params), fetchone=True)
    return result['total_count'] if result else 0

def get_liquidation_details_combo(wo_id, company_id):
    """
    Obtiene TODOS los datos necesarios para la vista de liquidación.
    [CORREGIDO] Agrega campos faltantes (code, company_id) para cumplir con Pydantic.
    """
    try:
        # 1. Datos de la Work Order (Igual)
        wo = execute_query(
            "SELECT * FROM work_orders WHERE id=%s AND company_id=%s", 
            (wo_id, company_id), fetchone=True
        )
        if not wo: return None, "Orden de Trabajo no encontrada."

        # 2. Buscar Pickings (Igual)
        pickings = execute_query("""
            SELECT p.*, pt.code as type_code 
            FROM pickings p
            JOIN picking_types pt ON p.picking_type_id = pt.id
            WHERE p.work_order_id = %s AND p.company_id = %s
        """, (wo_id, company_id), fetchall=True)

        p_consumo = next((p for p in pickings if p['type_code'] == 'OUT'), None)
        p_retiro = next((p for p in pickings if p['type_code'] == 'RET' or p['custom_operation_type'] == 'Materiales Retirados'), None)

        # Helper interno (Igual)
        def get_picking_full_data(picking_row):
            if not picking_row: return None, [], {}
            pid = picking_row['id']
            moves = execute_query("""
                SELECT sm.id, sm.product_id, sm.product_uom_qty, sm.quantity_done,
                       p.name, p.sku, p.tracking, p.ownership,
                       u.name as uom_name, sm.price_unit,
                       sm.project_id, proj.name as project_name
                FROM stock_moves sm
                JOIN products p ON sm.product_id = p.id
                LEFT JOIN uom u ON p.uom_id = u.id
                LEFT JOIN projects proj ON sm.project_id = proj.id
                WHERE sm.picking_id = %s
            """, (pid,), fetchall=True)
            
            serials_raw = execute_query("""
                SELECT sml.move_id, sl.name, sml.qty_done
                FROM stock_move_lines sml
                JOIN stock_lots sl ON sml.lot_id = sl.id
                JOIN stock_moves sm ON sml.move_id = sm.id
                WHERE sm.picking_id = %s
            """, (pid,), fetchall=True)
            
            serials_map = defaultdict(dict)
            for s in serials_raw:
                serials_map[s['move_id']][s['name']] = s['qty_done']
            return dict(picking_row), [dict(m) for m in moves], dict(serials_map)

        # 3. Obtener detalles (Igual)
        p_out_data, m_out, s_out = get_picking_full_data(p_consumo)
        p_ret_data, m_ret, s_ret = get_picking_full_data(p_retiro)

        # 4. Cargar Maestros (CORREGIDO)
        
        # [CORRECCIÓN 1] Agregar 'p.company_id' y 'cat.name'
        all_products = execute_query("""
            SELECT p.id, p.name, p.sku, p.tracking, p.ownership, p.standard_price, 
                   p.company_id, -- <--- FALTABA ESTE CAMPO
                   u.name as uom_name,
                   c.name as category_name
            FROM products p 
            LEFT JOIN uom u ON p.uom_id = u.id
            LEFT JOIN product_categories c ON p.category_id = c.id
            WHERE p.company_id = %s AND p.type = 'storable'
            ORDER BY p.name LIMIT 1000
        """, (company_id,), fetchall=True)

        # [CORRECCIÓN 2] Agregar 'code' al almacén
        warehouses = execute_query("""
            SELECT id, name, code, category_id 
            FROM warehouses 
            WHERE company_id=%s AND status='activo'
        """, (company_id,), fetchall=True)

        # 5. Cargar Ubicaciones (Igual)
        target_wh_id = None
        if p_out_data and p_out_data.get('warehouse_id'): target_wh_id = p_out_data['warehouse_id']
        elif p_ret_data and p_ret_data.get('warehouse_id'): target_wh_id = p_ret_data['warehouse_id']
        
        locations = []
        if target_wh_id:
            # [CORRECCIÓN 3] Agregar 'warehouse_name' y 'type'/'category' si el schema lo pide
            # El schema LocationResponse pide: name, path, type, category, warehouse_id, warehouse_name
            locations = execute_query("""
                SELECT l.id, l.name, l.path, l.type, l.category, l.warehouse_id, l.company_id, w.name as warehouse_name
                FROM locations l
                JOIN warehouses w ON l.warehouse_id = w.id
                WHERE l.warehouse_id=%s
            """, (target_wh_id,), fetchall=True)

        # 6. Ensamblar Respuesta
        result = {
            "wo_data": dict(wo),
            "picking_consumo": p_out_data,
            "moves_consumo": m_out,
            "serials_consumo": s_out,
            "picking_retiro": p_ret_data,
            "moves_retiro": m_ret,
            "serials_retiro": s_ret,
            "dropdowns": {
                "all_products": [dict(p) for p in all_products],
                "warehouses": [dict(w) for w in warehouses],
                "locations": [dict(l) for l in locations]
            }
        }
        return result, None

    except Exception as e:
        traceback.print_exc()
        return None, str(e)

# --- LÓGICA DE IMPORTACIÓN AVANZADA ---

def _get_project_id_by_name_internal(cursor, company_id, project_name):
    """
    Busca el ID de un proyecto por nombre (Case Insensitive).
    Usa el cursor de la transacción en curso.
    """
    if not project_name or not project_name.strip():
        return None
    
    clean_name = project_name.strip()
    cursor.execute(
        "SELECT id FROM projects WHERE company_id = %s AND name ILIKE %s LIMIT 1",
        (company_id, clean_name)
    )
    res = cursor.fetchone()
    if res:
        return res[0]
    else:
        # Opcional: Podrías levantar error si quieres ser estricto
        # raise ValueError(f"El proyecto '{clean_name}' no existe en el sistema.")
        print(f"[IMPORT WARN] Proyecto '{clean_name}' no encontrado. Se asignará como General.")
        return None

def upsert_work_order_from_import(company_id, data):
    """
    [NUEVO] Importación Inteligente (Upsert).
    1. Busca el Project ID basándose en el nombre (si viene).
    2. Si la OT existe, actualiza datos.
    3. Si no existe, la crea.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 1. Resolver Project ID
            project_name = data.get('project_name')
            project_id = _get_project_id_by_name_internal(cursor, company_id, project_name)

            ot_number = data['ot_number']
            
            # 2. Verificar existencia
            cursor.execute(
                "SELECT id FROM work_orders WHERE company_id = %s AND ot_number = %s",
                (company_id, ot_number)
            )
            existing = cursor.fetchone()

            if existing:
                # --- UPDATE (Solo actualizamos campos no críticos) ---
                wo_id = existing[0]
                cursor.execute("""
                    UPDATE work_orders SET 
                        customer_name = %s,
                        address = %s,
                        service_type = %s,
                        job_type = %s,
                        project_id = %s -- Actualizamos el proyecto si cambió en el Excel
                    WHERE id = %s
                """, (
                    data['customer_name'], 
                    data.get('address'), 
                    data.get('service_type'), 
                    data.get('job_type'),
                    project_id,
                    wo_id
                ))
                conn.commit()
                return "updated"
            else:
                # --- INSERT ---
                cursor.execute("""
                    INSERT INTO work_orders (
                        company_id, ot_number, customer_name, address, 
                        service_type, job_type, project_id, phase, date_registered
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'Sin Liquidar', NOW())
                """, (
                    company_id, 
                    ot_number, 
                    data['customer_name'], 
                    data.get('address'), 
                    data.get('service_type'), 
                    data.get('job_type'),
                    project_id
                ))
                conn.commit()
                return "created"

    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        if conn: return_db_connection(conn)

def delete_work_order(wo_id):
    """
    [BLINDADO] Elimina una Orden de Trabajo y sus borradores asociados.
    Reglas de Protección:
    1. No se puede borrar si ya está 'Liquidado'.
    2. No se puede borrar si tiene movimientos de stock validados ('done').
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            
            # 1. BLOQUEO ATÓMICO (Evita que alguien la liquide mientras intentamos borrarla)
            cursor.execute("SELECT id, phase, ot_number FROM work_orders WHERE id = %s FOR UPDATE", (wo_id,))
            wo = cursor.fetchone()

            if not wo:
                return False, "La Orden de Trabajo no existe."

            # 2. REGLA DE INMUTABILIDAD HISTÓRICA
            if wo['phase'] == 'Liquidado':
                return False, f"🚫 Acción Bloqueada: La OT '{wo['ot_number']}' está LIQUIDADA. No se puede eliminar la historia."

            # 3. REGLA DE INTEGRIDAD DE STOCK
            # Verificamos si existen Pickings YA VALIDADOS (que movieron stock real).
            # Si existen, prohibimos el borrado para no dejar 'huecos' en el Kardex.
            cursor.execute("""
                SELECT count(*) as total 
                FROM pickings 
                WHERE work_order_id = %s AND state = 'done'
            """, (wo_id,))
            
            if cursor.fetchone()['total'] > 0:
                return False, "🚫 Acción Bloqueada: Esta OT tiene movimientos de inventario ya procesados. Debe anularlos primero (si es posible) o crear una devolución."

            # 4. LIMPIEZA EN CASCADA (Solo Borradores/Cancelados)
            # Si llegamos aquí, es seguro borrar porque solo hay 'papeles sucios' (borradores) sin impacto real.
            print(f"[DB-DELETE] Limpiando dependencias de OT {wo_id}...")

            # A. Borrar Líneas de detalle (Series/Lotes) de los pickings asociados
            cursor.execute("""
                DELETE FROM stock_move_lines 
                WHERE move_id IN (
                    SELECT id FROM stock_moves 
                    WHERE picking_id IN (SELECT id FROM pickings WHERE work_order_id = %s)
                )
            """, (wo_id,))

            # B. Borrar Movimientos (Stock Moves)
            cursor.execute("""
                DELETE FROM stock_moves 
                WHERE picking_id IN (SELECT id FROM pickings WHERE work_order_id = %s)
            """, (wo_id,))

            # C. Borrar Pickings (Cabeceras)
            cursor.execute("DELETE FROM pickings WHERE work_order_id = %s", (wo_id,))

            # D. Finalmente, borrar la OT
            cursor.execute("DELETE FROM work_orders WHERE id = %s", (wo_id,))

            conn.commit()
            return True, "Orden de Trabajo eliminada correctamente (se limpiaron los borradores asociados)."

    except Exception as e:
        if conn: conn.rollback()
        print(f"[ERROR DELETE WO] {e}")
        return False, f"Error al eliminar: {str(e)}"
    finally:
        if conn: return_db_connection(conn)
