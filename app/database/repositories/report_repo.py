#app/database/repositories/report_repo.py

from datetime import datetime, date, timedelta
from collections import defaultdict
from ..core import get_db_connection, return_db_connection, execute_query

# --- DASHBOARD & KPIs ---

def get_dashboard_kpis(company_id):
    """
    Obtiene contadores de operaciones pendientes por tipo.
    [CORREGIDO] Solo cuenta estado 'listo' - operaciones que requieren
    atención inmediata para ser validadas (excluye borradores).
    """
    query = """
        SELECT
            pt.code,
            COUNT(p.id) as pending_count
        FROM pickings p
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.company_id = %s AND p.state = 'listo'
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

def get_inventory_value_kpis(company_id):
    """
    [VERSIÓN FINAL VALIDADA] Calcula el valor del inventario.
    Usa lógica de agrupación flexible en Python para garantizar que no se pierdan
    categorías por temas de sintaxis SQL.
    """
    # Consulta amplia: Trae todo lo que tenga stock > 0
    query = """
        SELECT 
            UPPER(wc.name) as category_name, 
            SUM(sq.quantity * p.standard_price) as val
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE l.type = 'internal' 
          AND p.company_id = %s 
          AND sq.quantity > 0
        GROUP BY wc.name
    """
    results = execute_query(query, (company_id,), fetchall=True)

    kpis = {"total": 0.0, "pri": 0.0, "tec": 0.0}
    
    if results:
        for row in results:
            cat = row['category_name']
            val = float(row['val'] or 0.0)
            
            # 1. Detectar Almacenes Principales
            # Buscamos palabras clave típicas
            if 'PRINCIPAL' in cat or 'CENTRAL' in cat or 'ALMACEN' in cat: 
                # Evitamos falsos positivos (ej. "Almacén Contratista")
                if 'CONTRAT' not in cat and 'TERCERO' not in cat:
                    kpis['pri'] += val
            
            # 2. Detectar Contratistas
            if 'CONTRAT' in cat or 'TERCERO' in cat:
                kpis['tec'] += val
    
    # Totalizar
    kpis['total'] = kpis['pri'] + kpis['tec']
    return kpis

# --- REPORTES DE INVENTARIO (Aging, Cobertura) ---

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

# --- KARDEX ---

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

# --- STOCK SUMMARY (Reporte simple) ---

def get_stock_summary_count(company_id, filters={}):
    """
    [CORREGIDO] Cuenta grupos de Productos + Ubicación.
    Antes faltaba agrupar por Ubicación (l.id), por eso daba menos filas.
    """
    base_query = """
    SELECT COUNT(*) as total FROM (
        SELECT 1
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        JOIN warehouses w ON l.warehouse_id = w.id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        LEFT JOIN uom u ON p.uom_id = u.id
        
        WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0.001
    """
    params = [company_id]
    where_clauses = []

    filter_map = {
        'warehouse_name': 'w.name', 'location_name': 'l.name', 'sku': 'p.sku', 
        'product_name': 'p.name', 'category_name': 'pc.name', 'uom_name': 'u.name',
        'warehouse_id': 'w.id', 'location_id': 'l.id',
        'notes': 'sq.notes'
    }
    
    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value is not None and value != "":
            if key in ['warehouse_id', 'location_id']:
                where_clauses.append(f"{db_column} = %s"); params.append(value)
            else:
                where_clauses.append(f"{db_column} ILIKE %s"); params.append(f"%{value}%")

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    # [CORRECCIÓN CRÍTICA] Agregamos l.id y l.name al Group By
    base_query += """
    GROUP BY 
        w.id, w.name, 
        l.id, l.name, -- <--- ¡ESTO FALTABA!
        p.id, p.sku, p.name, 
        pc.id, pc.name, 
        u.id, u.name
    ) as subquery
    """

    res = execute_query(base_query, tuple(params), fetchone=True)
    return res['total'] if res else 0

def get_stock_summary_filtered_sorted(company_id, warehouse_id=None, filters={}, sort_by='sku', ascending=True, limit=None, offset=None):
    """ 
    [CORREGIDO DEFINITIVO] Obtiene el stock resumen.
    
    CAMBIO CLAVE:
    Para 'IncomingStock' (En Tránsito) y 'ReservedStock' (Reservado), ahora usamos
    directamente 'sm.product_uom_qty' (la cantidad planificada en el movimiento).
    
    Esto evita el problema donde el cálculo por líneas (stock_move_lines) devolvía 0
    si las líneas aún no tenían 'qty_done' validado, garantizando que el número
    coincida con la realidad operativa.
    """
    base_query = """
    WITH 
    -- 1. Stock Físico (Quants existentes)
    PhysicalStock AS (
        SELECT 
            sq.product_id, sq.location_id, SUM(sq.quantity) as qty, MAX(sq.notes) as note
        FROM stock_quants sq
        JOIN locations l ON sq.location_id = l.id
        WHERE l.company_id = %s AND l.type = 'internal'
        GROUP BY sq.product_id, sq.location_id
    ),
    -- 2. Stock Reservado (Salidas Planificadas)
    ReservedStock AS (
        SELECT 
            sm.product_id, sm.location_src_id as location_id, 
            SUM(sm.product_uom_qty) as qty -- Usamos la demanda total del movimiento
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE p.state = 'listo' AND p.company_id = %s
        GROUP BY sm.product_id, sm.location_src_id
    ),
    -- 3. Stock En Tránsito (Entradas Planificadas)
    IncomingStock AS (
        SELECT 
            sm.product_id, sm.location_dest_id as location_id, 
            SUM(sm.product_uom_qty) as qty -- Usamos la demanda total del movimiento
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        WHERE p.state = 'listo' AND p.company_id = %s
        GROUP BY sm.product_id, sm.location_dest_id
    ),
    -- 4. Unimos todas las claves para no perder productos que solo tengan tránsito
    ActiveKeys AS (
        SELECT product_id, location_id FROM PhysicalStock
        UNION
        SELECT product_id, location_id FROM ReservedStock
        UNION
        SELECT product_id, location_id FROM IncomingStock
    )
    
    SELECT
        p.id as product_id, 
        w.id as warehouse_id, 
        l.id as location_id, 
        
        p.sku, 
        p.name as product_name, 
        pc.name as category_name,
        w.name as warehouse_name, 
        l.name as location_name,
        u.name as uom_name,

        COALESCE(phys.note, '') as notes,

        COALESCE(phys.qty, 0) as physical_quantity,
        COALESCE(res.qty, 0) as reserved_quantity,
        
        -- Aquí aseguramos que si Incoming es nulo, sea 0
        COALESCE(inc.qty, 0) as incoming_quantity,
        
        (COALESCE(phys.qty, 0) - COALESCE(res.qty, 0)) as available_quantity

    FROM ActiveKeys k
    JOIN products p ON k.product_id = p.id
    JOIN locations l ON k.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN uom u ON p.uom_id = u.id
    
    -- Joins a las tablas CTE calculadas arriba
    LEFT JOIN PhysicalStock phys ON k.product_id = phys.product_id AND k.location_id = phys.location_id
    LEFT JOIN ReservedStock res ON k.product_id = res.product_id AND k.location_id = res.location_id
    LEFT JOIN IncomingStock inc ON k.product_id = inc.product_id AND k.location_id = inc.location_id
    
    WHERE p.company_id = %s 
      -- Filtro para mostrar solo filas con actividad
      AND (COALESCE(phys.qty, 0) > 0.001 OR COALESCE(inc.qty, 0) > 0.001 OR COALESCE(res.qty, 0) > 0.001)
    """
    
    # Params: [Physical, Reserved, Incoming, MainQuery]
    params = [company_id, company_id, company_id, company_id]
    where_clauses = []

    filter_map = {
        'warehouse_name': 'w.name', 'location_name': 'l.name', 'sku': 'p.sku', 
        'product_name': 'p.name', 'category_name': 'pc.name', 'uom_name': 'u.name',
        'warehouse_id': 'w.id', 'location_id': 'l.id',
        'notes': 'phys.note' 
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

    sort_map = {
        'warehouse_name': 'w.name', 'location_name': 'l.name', 'sku': 'p.sku',
        'product_name': 'p.name', 'category_name': 'pc.name',
        'physical_quantity': 'physical_quantity', 
        'reserved_quantity': 'reserved_quantity', 
        'incoming_quantity': 'incoming_quantity',
        'available_quantity': 'available_quantity'
    }
    order_by_col_key = sort_by if sort_by else 'sku'
    order_by_col = sort_map.get(order_by_col_key, 'p.sku')
    direction = "ASC" if ascending else "DESC"
    
    if order_by_col in ['pc.name', 'u.name', 'l.name']:
         order_by_clause = f"COALESCE({order_by_col}, 'zzzz')"
    else:
         order_by_clause = order_by_col
         
    base_query += f" ORDER BY {order_by_clause} {direction}"
    
    if limit is not None:
        base_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
    
    return execute_query(base_query, tuple(params), fetchall=True)

def get_stock_on_hand_filtered_sorted(company_id, warehouse_id=None, filters={}, sort_by='sku', ascending=True, limit=None, offset=None):
    """ 
    Obtiene el stock detallado por lote/serie y PROYECTO (Versión V3).
    [CORREGIDO] Arreglado bug de ordenamiento por lote (lot_name_ordered).
    """
    base_query = """
    WITH ReservedStock AS (
        SELECT 
            sm.product_id, sm.location_src_id, sml.lot_id, sm.project_id,
            SUM(CASE WHEN sml.id IS NOT NULL THEN sml.qty_done ELSE sm.product_uom_qty END) as reserved_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        LEFT JOIN stock_move_lines sml ON sm.id = sml.move_id
        WHERE p.state = 'listo' AND p.company_id = %s
        GROUP BY sm.product_id, sm.location_src_id, sml.lot_id, sm.project_id
    ),
    IncomingStock AS (
        SELECT 
            sm.product_id, sm.location_dest_id, sml.lot_id, sm.project_id,
            SUM(CASE WHEN sml.id IS NOT NULL THEN sml.qty_done ELSE sm.product_uom_qty END) as incoming_qty
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        LEFT JOIN stock_move_lines sml ON sm.id = sml.move_id
        WHERE p.state = 'listo' AND p.company_id = %s
        GROUP BY sm.product_id, sm.location_dest_id, sml.lot_id, sm.project_id
    )
    SELECT
        p.id as product_id, w.id as warehouse_id, l.id as location_id, sl.id as lot_id, sq.project_id,
        p.sku, p.name as product_name, pc.name as category_name,
        w.name as warehouse_name, l.name as location_name, 
        sl.name as lot_name, proj.name as project_name,
        
        SUM(sq.quantity) as physical_quantity, 
        u.name as uom_name, MAX(sq.notes) as notes, 
        
        COALESCE(MAX(rs.reserved_qty), 0) as reserved_quantity,
        COALESCE(MAX(iss.incoming_qty), 0) as incoming_quantity,
        
        (SUM(sq.quantity) - COALESCE(MAX(rs.reserved_qty), 0)) as available_quantity,
        COALESCE(sl.name, '---') as lot_name_ordered
        
    FROM stock_quants sq
    JOIN products p ON sq.product_id = p.id
    JOIN locations l ON sq.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    LEFT JOIN product_categories pc ON p.category_id = pc.id
    LEFT JOIN stock_lots sl ON sq.lot_id = sl.id
    LEFT JOIN uom u ON p.uom_id = u.id
    LEFT JOIN projects proj ON sq.project_id = proj.id
    
    LEFT JOIN ReservedStock rs ON sq.product_id = rs.product_id AND sq.location_id = rs.location_src_id 
        AND ((sq.lot_id = rs.lot_id AND sq.lot_id IS NOT NULL) OR (sq.lot_id IS NULL AND rs.lot_id IS NULL))
        AND ((sq.project_id = rs.project_id AND sq.project_id IS NOT NULL) OR (sq.project_id IS NULL AND rs.project_id IS NULL))

    LEFT JOIN IncomingStock iss ON sq.product_id = iss.product_id AND sq.location_id = iss.location_dest_id 
        AND ((sq.lot_id = iss.lot_id AND sq.lot_id IS NOT NULL) OR (sq.lot_id IS NULL AND iss.lot_id IS NULL))
        AND ((sq.project_id = iss.project_id AND sq.project_id IS NOT NULL) OR (sq.project_id IS NULL AND iss.project_id IS NULL))

    WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0.001
    """
    
    params = [company_id, company_id, company_id]
    where_clauses = []

    filter_map = {
        'warehouse_name': 'w.name','location_name': 'l.name', 'sku': 'p.sku', 'product_name': 'p.name',
        'category_name': 'pc.name', 'lot_name': 'sl.name', 'uom_name': 'u.name',
        'warehouse_id': 'w.id', 'location_id': 'l.id', 
        'project_name': 'proj.name', 'location_name': 'l.name'
    }
    
    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value is not None and value != "":
            if key in ['warehouse_id', 'location_id']:
                where_clauses.append(f"{db_column} = %s"); params.append(value)
            elif key == 'lot_name' and value == '-':
                where_clauses.append("sl.id IS NULL")
            else:
                where_clauses.append(f"{db_column} ILIKE %s"); params.append(f"%{value}%")

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    base_query += """
    GROUP BY p.id, w.id, l.id, sl.id, sq.project_id, p.sku, p.name, pc.name, w.name, l.name, sl.name, proj.name, u.name, lot_name_ordered
    """

    sort_map = {
        'warehouse_name': 'w.name', 'location_name': 'l.name', 'sku': 'p.sku',
        'product_name': 'p.name', 'category_name': 'pc.name', 'lot_name': 'lot_name_ordered',
        'physical_quantity': 'physical_quantity', 'reserved_quantity': 'reserved_quantity', 'incoming_quantity': 'incoming_quantity',
        'available_quantity': 'available_quantity', 'uom_name': 'u.name', 'project_name': 'proj.name'
    }
    order_by_col_key = sort_by if sort_by else 'sku'
    order_by_col = sort_map.get(order_by_col_key, 'p.sku')
    direction = "ASC" if ascending else "DESC"
    
    # [CORRECCIÓN AQUI]
    # Quitamos 'lot_name_ordered' de esta lista porque ya es un alias "seguro" (nunca es nulo)
    # y PostgreSQL falla si metemos un alias dentro de una función en el ORDER BY.
    if order_by_col in ['pc.name', 'u.name', 'proj.name']: 
        order_by_clause = f"COALESCE({order_by_col}, 'zzzz')"
    else: 
        order_by_clause = order_by_col
         
    base_query += f" ORDER BY {order_by_clause} {direction}, p.sku ASC, lot_name_ordered ASC"
    
    if limit is not None:
        base_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
    
    return execute_query(base_query, tuple(params), fetchall=True)

def get_full_product_kardex_data(company_id, date_from, date_to, warehouse_id=None, product_filter=None):
    """
    Obtiene TODOS los movimientos de stock detallados ('done') para el EXPORT CSV.
    [CORREGIDO] Usa ILIKE para búsqueda de productos insensible a mayúsculas.
    """
    
    # --- 1. Parámetros y cláusulas para el WHERE ---
    where_clauses = ["p.state = 'done'", "p.company_id = %s"]
    params = [company_id]

    if date_from:
        where_clauses.append("date(p.date_done) >= %s")
        params.append(date_from)
    if date_to:
        where_clauses.append("date(p.date_done) <= %s")
        params.append(date_to)
    
    # Filtro de Almacén
    if warehouse_id and warehouse_id != "all":
        where_clauses.append("l.warehouse_id = %s")
        params.append(warehouse_id)
    else:
        where_clauses.append("l.type = 'internal'")
        
    # Filtro de Producto
    if product_filter:
        # [CORRECCIÓN] Usar ILIKE para búsqueda flexible
        where_clauses.append("(prod.sku ILIKE %s OR prod.name ILIKE %s)")
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
        
        AND sm.quantity_done > 0 

        ORDER BY prod.sku ASC, p.date_done ASC, p.id ASC
    """

    print(f"[DB DEBUG] get_full_product_kardex_data Query Params: {tuple(params)}")
    return execute_query(query, tuple(params), fetchall=True)

# --- REPORTE DE PROYECTOS ---

def get_project_kardex(company_id, project_id):
    """
    Obtiene el estado logístico detallado de un proyecto.
    [CORREGIDO FINAL] Filtra l.type = 'internal' para evitar contar stock 
    que ya está en manos del cliente (Ubicación Externa) como 'En Custodia'.
    """
    # 1. Stock en Mano (En Custodia)
    query_on_hand = """
        SELECT 
            p.sku, 
            p.name, 
            COALESCE(pc.name, 'Sin Categoría') as category,
            
            COALESCE(w.name, 'Ubicación Virtual / Tránsito') as warehouse, 
            COALESCE(l.path, 'Sin Ubicación') as location, 
            
            SUM(sq.quantity) as qty,
            (SUM(sq.quantity) * p.standard_price) as value,
            u.name as uom
            
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        LEFT JOIN uom u ON p.uom_id = u.id
        
        -- LEFT JOIN para ver todo, pero luego filtraremos por tipo
        LEFT JOIN locations l ON sq.location_id = l.id
        LEFT JOIN warehouses w ON l.warehouse_id = w.id
        
        WHERE sq.project_id = %s 
          AND sq.quantity > 0
          AND l.type = 'internal'  -- <--- ¡ESTE FILTRO ES LA CLAVE!
          
        GROUP BY p.id, p.sku, p.name, pc.name, w.name, l.path, u.name, p.standard_price
        ORDER BY w.name NULLS LAST, p.name
    """
    on_hand = execute_query(query_on_hand, (project_id,), fetchall=True)

    # 2. Consumo Histórico (Liquidado / Instalado)
    # (Esta parte no cambia, ya está correcta)
    query_consumed = """
        SELECT 
            p.sku, 
            p.name, 
            COALESCE(pc.name, 'Sin Categoría') as category,
            COALESCE(w_src.name, 'Origen Externo') as contractor,
            
            SUM(sm.quantity_done) as qty,
            SUM(sm.quantity_done * sm.price_unit) as value,
            u.name as uom
            
        FROM stock_moves sm
        JOIN products p ON sm.product_id = p.id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        LEFT JOIN uom u ON p.uom_id = u.id
        
        LEFT JOIN locations l_src ON sm.location_src_id = l_src.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        
        JOIN locations l_dest ON sm.location_dest_id = l_dest.id
        
        WHERE sm.project_id = %s 
          AND sm.state = 'done' 
          AND l_dest.category IN ('CLIENTE', 'CONTRATA CLIENTE')
          
        GROUP BY p.id, p.sku, p.name, pc.name, u.name, w_src.name
        ORDER BY w_src.name NULLS LAST, p.name
    """
    consumed = execute_query(query_consumed, (project_id,), fetchall=True)

    return {
        "on_hand": [dict(r) for r in on_hand],
        "consumed": [dict(r) for r in consumed]
    }

def get_warehouses_kpi_paginated(company_id, user_id, role_name, search=None, limit=12, offset=0):
    """
    [OPTIMIZADO V2 - CTE] Estrategia 'Divide y Vencerás'.
    1. Primero filtra y pagina los almacenes (tabla pequeña).
    2. Luego une solo esos 12 almacenes con el inventario (tabla gigante).
    Esto evita scans secuenciales masivos y timeouts.
    """
    params = [company_id]
    where_clauses = ["w.company_id = %s", "w.status = 'activo'"]

    # 1. Filtro de Permisos
    if role_name != 'Administrador':
        where_clauses.append("w.id IN (SELECT warehouse_id FROM user_warehouses WHERE user_id = %s)")
        params.append(user_id)

    # 2. Filtro de Búsqueda
    if search:
        where_clauses.append("(w.name ILIKE %s OR wc.name ILIKE %s)")
        term = f"%{search}%"
        params.extend([term, term])

    where_sql = " AND ".join(where_clauses)

    # --- QUERY OPTIMIZADA CON CTE (Common Table Expression) ---
    query = f"""
        WITH TargetWarehouses AS (
            -- Paso 1: Obtener solo los IDs de la página actual
            -- Esto es rapidísimo porque solo toca la tabla 'warehouses'
            SELECT w.id, w.name, wc.name as category
            FROM warehouses w
            JOIN warehouse_categories wc ON w.category_id = wc.id
            WHERE {where_sql}
            ORDER BY wc.name, w.name
            LIMIT %s OFFSET %s
        )
        -- Paso 2: Calcular KPIs SOLO para los almacenes filtrados
        SELECT 
            tw.id, tw.name, tw.category,
            COUNT(DISTINCT sq.product_id) as items_count,
            COALESCE(SUM(sq.quantity * p.standard_price), 0) as total_value
        FROM TargetWarehouses tw
        LEFT JOIN locations l ON tw.id = l.warehouse_id AND l.type = 'internal'
        LEFT JOIN stock_quants sq ON l.id = sq.location_id AND sq.quantity > 0
        LEFT JOIN products p ON sq.product_id = p.id
        GROUP BY tw.id, tw.name, tw.category
        ORDER BY tw.category, tw.name
    """
    
    # Añadir limit y offset al final de los parámetros
    params.extend([limit, offset])
    
    # Debug: Imprimir si quieres ver qué ejecuta
    # print(f"[DB-PERF] Executing optimized KPI query with params: {params}")

    results = execute_query(query, tuple(params), fetchall=True)
    return results

def get_warehouses_kpi_count(company_id, user_id, role_name, search=None):
    """Cuenta el total de almacenes que coinciden con los filtros."""
    params = [company_id]
    where_clauses = ["w.company_id = %s", "w.status = 'activo'"]

    if role_name != 'Administrador':
        where_clauses.append("w.id IN (SELECT warehouse_id FROM user_warehouses WHERE user_id = %s)")
        params.append(user_id)

    if search:
        where_clauses.append("(w.name ILIKE %s OR wc.name ILIKE %s)")
        term = f"%{search}%"
        params.extend([term, term])

    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT COUNT(DISTINCT w.id) as total
        FROM warehouses w
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE {where_sql}
    """
    res = execute_query(query, tuple(params), fetchone=True)
    return res['total'] if res else 0

def get_stock_on_hand_count(company_id, warehouse_id=None, filters={}):
    """
    [CORREGIDO] Cuenta grupos únicos (agrupando quants fragmentados).
    """
    base_query = """
    SELECT COUNT(*) as total FROM (
        SELECT 1
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        JOIN warehouses w ON l.warehouse_id = w.id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        LEFT JOIN stock_lots sl ON sq.lot_id = sl.id
        LEFT JOIN projects proj ON sq.project_id = proj.id
        WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0.001
    """
    
    params = [company_id]
    where_clauses = []

    filter_map = {
        'warehouse_name': 'w.name','location_name': 'l.name', 'sku': 'p.sku', 'product_name': 'p.name',
        'category_name': 'pc.name', 'lot_name': 'sl.name', 'uom_name': 'u.name',
        'warehouse_id': 'w.id', 'location_id': 'l.id', 
        'project_name': 'proj.name'
    }
    
    for key, value in filters.items():
        db_column = filter_map.get(key)
        if db_column and value is not None and value != "":
            if key in ['warehouse_id', 'location_id']:
                where_clauses.append(f"{db_column} = %s"); params.append(value)
            elif key == 'lot_name' and value == '-':
                where_clauses.append("sl.id IS NULL")
            else:
                where_clauses.append(f"{db_column} ILIKE %s"); params.append(f"%{value}%")

    if where_clauses:
        base_query += " AND " + " AND ".join(where_clauses)

    # [CLAVE] Agrupar por todo lo que define una fila única en el detalle
    base_query += """
        GROUP BY p.id, l.id, sl.id, proj.id
    ) as subquery
    """

    res = execute_query(base_query, tuple(params), fetchone=True)
    return res['total'] if res else 0

def get_product_reservations(product_id, location_id, lot_id=None):
    """
    Devuelve una lista enriquecida de operaciones 'listo' que reservan stock.
    [CORREGIDO] Muestra el Nombre del Almacén en lugar del Path completo en 'dest_location'.
    """
    params = [product_id, location_id]
    
    select_clause = """
        SELECT 
            p.name as picking_name,
            p.custom_operation_type as op_type,
            pt.code as type_code,
            
            COALESCE(
                TO_CHAR(p.date_transfer, 'DD/MM/YYYY'), 
                TO_CHAR(p.attention_date, 'DD/MM/YYYY'), 
                TO_CHAR(p.scheduled_date, 'DD/MM/YYYY')
            ) as attention_date,
            
            p.responsible_user,
            
            COALESCE(proj.name, 'Stock General') as project_name,
            COALESCE(part.name, 'Uso Interno') as partner_name,
            
            -- [CAMBIO AQUI] Priorizamos el Nombre del Almacén (w_dest.name)
            -- Si no es un almacén (ej. es un Cliente), mostramos el path o el nombre de la ubicación.
            COALESCE(w_dest.name, l_dest.name, l_dest.path, 'Desconocido') as dest_location,
            
            SUM(sm.product_uom_qty) as reserved_qty
    """

    # El resto de la función se mantiene idéntico
    joins_clause = """
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        LEFT JOIN locations l_dest ON sm.location_dest_id = l_dest.id
        LEFT JOIN warehouses w_dest ON l_dest.warehouse_id = w_dest.id
        LEFT JOIN projects proj ON p.project_id = proj.id
        LEFT JOIN partners part ON p.partner_id = part.id
    """

    where_clause = """
        WHERE sm.product_id = %s
          AND sm.location_src_id = %s
          AND p.state = 'listo'
          AND sm.state != 'cancelled'
    """

    # [IMPORTANTE] Aseguramos que el nuevo campo esté en el GROUP BY
    group_by_clause = """
        GROUP BY 
            p.name, p.custom_operation_type, pt.code, 
            p.date_transfer, p.attention_date, p.scheduled_date, p.responsible_user,
            proj.name, part.name, 
            w_dest.name, l_dest.name, l_dest.path -- Agregados al Group By
    """

    if lot_id is not None:
        joins_clause += " JOIN stock_move_lines sml ON sm.id = sml.move_id"
        where_clause += " AND sml.lot_id = %s"
        params.append(lot_id)
        select_clause = select_clause.replace("SUM(sm.product_uom_qty)", "SUM(sml.qty_done)")
    
    query = f"{select_clause} {joins_clause} {where_clause} {group_by_clause} ORDER BY p.scheduled_date ASC"
    
    return execute_query(query, tuple(params), fetchall=True)

def get_product_incoming(product_id, location_id):
    """
    Devuelve el detalle de operaciones 'listo' que están por llegar (En Tránsito).
    [ACTUALIZADO] Incluye Orden de Compra.
    """
    query = """
        SELECT 
            p.name as picking_name,
            p.custom_operation_type as op_type,
            pt.code as type_code,
            
            -- [NUEVO] Orden de Compra
            p.purchase_order,
            
            COALESCE(
                TO_CHAR(p.date_transfer, 'DD/MM/YYYY'), 
                TO_CHAR(p.attention_date, 'DD/MM/YYYY'), 
                TO_CHAR(p.scheduled_date, 'DD/MM/YYYY')
            ) as attention_date,
            
            p.responsible_user,
            
            COALESCE(proj.name, 'Stock General') as project_name,
            
            CASE 
                WHEN pt.code = 'IN' THEN COALESCE(part.name, 'Proveedor Externo')
                ELSE COALESCE(w_src.name, l_src.path, 'Origen Desconocido')
            END as origin_location,
            
            SUM(sm.product_uom_qty) as incoming_qty

        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        
        LEFT JOIN locations l_src ON sm.location_src_id = l_src.id
        LEFT JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        LEFT JOIN projects proj ON p.project_id = proj.id
        LEFT JOIN partners part ON p.partner_id = part.id
        
        WHERE sm.product_id = %s
          AND sm.location_dest_id = %s
          AND p.state = 'listo'
          AND sm.state != 'cancelled'
          
        GROUP BY 
            p.name, p.custom_operation_type, pt.code, 
            p.purchase_order, -- <--- IMPORTANTE: Agregar al Group By
            p.date_transfer, p.attention_date, p.scheduled_date, p.responsible_user,
            proj.name, part.name, w_src.name, l_src.path
            
        ORDER BY p.scheduled_date ASC
    """
    
    return execute_query(query, (product_id, location_id), fetchall=True)

# --- NUEVAS CONSULTAS PARA DASHBOARD GERENCIAL ---

def get_top_projects_statistics(company_id, limit=5):
    """
    Obtiene el Top 5 Proyectos con mayor valor en custodia,
    incluyendo su avance de liquidación.
    [CORREGIDO] Filtra l.type='internal' para que los montos coincidan
    con las tarjetas y reportes detallados.
    """
    query = """
        WITH ProjectStock AS (
            SELECT 
                sq.project_id as id, 
                COALESCE(SUM(sq.quantity * prod.standard_price), 0) as stock_val
            FROM stock_quants sq
            JOIN products prod ON sq.product_id = prod.id
            JOIN locations l ON sq.location_id = l.id
            JOIN projects p ON sq.project_id = p.id -- Join para filtrar por company
            WHERE p.company_id = %s 
              AND p.status = 'active'
              AND l.type = 'internal' -- <--- FILTRO DE CONSISTENCIA
            GROUP BY sq.project_id
        ),
        ProjectConsumed AS (
            SELECT 
                sm.project_id, 
                COALESCE(SUM(sm.quantity_done * sm.price_unit), 0) as liq_val
            FROM stock_moves sm
            JOIN locations l_dest ON sm.location_dest_id = l_dest.id
            WHERE sm.state = 'done' 
              AND l_dest.category IN ('CLIENTE', 'CONTRATA CLIENTE') 
              AND sm.project_id IS NOT NULL
            GROUP BY sm.project_id
        )
        SELECT 
            p.id, p.name,
            COALESCE(ps.stock_val, 0) as stock_value,
            COALESCE(pc.liq_val, 0) as liquidated_value
        FROM projects p
        LEFT JOIN ProjectStock ps ON p.id = ps.id
        LEFT JOIN ProjectConsumed pc ON p.id = pc.project_id
        WHERE p.company_id = %s AND p.status = 'active'
        -- Solo mostramos si tienen movimiento (stock o liquidado > 0)
        AND (COALESCE(ps.stock_val, 0) > 0 OR COALESCE(pc.liq_val, 0) > 0)
        ORDER BY stock_value DESC
        LIMIT %s
    """
    results = execute_query(query, (company_id, company_id, limit), fetchall=True)
    
    data = []
    for r in results:
        s = float(r['stock_value'])
        l = float(r['liquidated_value'])
        total = s + l
        progress = (l / total) if total > 0 else 0.0
        data.append({
            "id": r['id'], 
            "name": r['name'],
            "stock_value": s, 
            "liquidated_value": l, 
            "progress": progress
        })
    return data

def get_ownership_distribution(company_id):
    """
    Calcula valor total agrupado por Propiedad (Propio vs Consignado).
    """
    query = """
        SELECT 
            p.ownership,
            COUNT(sq.id) as items_count,
            SUM(sq.quantity * p.standard_price) as total_value
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0
        GROUP BY p.ownership
    """
    results = execute_query(query, (company_id,), fetchall=True)
    
    data = []
    for r in results:
        label = "Propio" if r['ownership'] == 'owned' else "Consignado"
        data.append({"type": label, "value": r['total_value'], "count": r['items_count']})
    return data

def get_total_liquidated_value_global(company_id):
    """ KPI Global: ¿Cuánto dinero hemos liquidado en total histórico? """
    query = """
        SELECT COALESCE(SUM(sm.quantity_done * sm.price_unit), 0) as total
        FROM stock_moves sm
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.company_id = %s 
          AND p.state = 'done' 
          AND pt.code = 'OUT' -- Solo salidas (consumos)
    """
    res = execute_query(query, (company_id,), fetchone=True)
    return res['total'] if res else 0.0

# --- NUEVAS CONSULTAS PARA INTELIGENCIA DE INVENTARIO ---

def get_top_products_by_value(company_id, limit=5):
    """
    Ranking de productos que representan mayor valor inmovilizado (Pareto).
    """
    query = """
        SELECT 
            p.name, p.sku,
            SUM(sq.quantity * p.standard_price) as total_value
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        JOIN locations l ON sq.location_id = l.id
        WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0
        GROUP BY p.id
        ORDER BY total_value DESC
        LIMIT %s
    """
    results = execute_query(query, (company_id, limit), fetchall=True)
    return [dict(r) for r in results]

def get_value_by_category(company_id):
    """
    Valor del inventario agrupado por Categoría de Producto.
    """
    query = """
        SELECT 
            COALESCE(pc.name, 'Sin Categoría') as category_name,
            SUM(sq.quantity * p.standard_price) as total_value
        FROM stock_quants sq
        JOIN products p ON sq.product_id = p.id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        JOIN locations l ON sq.location_id = l.id
        WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0
        GROUP BY pc.name
        ORDER BY total_value DESC
    """
    results = execute_query(query, (company_id,), fetchall=True)
    return [dict(r) for r in results]

def get_value_by_region(company_id):
    """
    [NUEVO] Agrupa el valor en custodia por Departamento (Mapa de Calor).
    """
    query = """
        SELECT 
            -- Si el campo está vacío, lo agrupamos como 'Sin Región'
            COALESCE(NULLIF(p.department, ''), 'Sin Región') as region,
            COUNT(DISTINCT p.id) as projects_count,
            SUM(sq.quantity * prod.standard_price) as total_value
        FROM stock_quants sq
        JOIN products prod ON sq.product_id = prod.id
        JOIN projects p ON sq.project_id = p.id
        WHERE p.company_id = %s 
          AND p.status = 'active'
          AND sq.quantity > 0
        GROUP BY region
        ORDER BY total_value DESC
    """
    results = execute_query(query, (company_id,), fetchall=True)
    return [dict(r) for r in results]

def get_warehouse_ranking_by_category(company_id, category_name, limit=10):
    """
    Obtiene el valor total de stock agrupado por Almacén, filtrado por Categoría.
    Útil para: Top Almacenes Principales y Top Contratistas.
    """
    query = """
        SELECT 
            w.name,
            COUNT(DISTINCT sq.product_id) as sku_count,
            COALESCE(SUM(sq.quantity * prod.standard_price), 0) as total_value
        FROM stock_quants sq
        JOIN products prod ON sq.product_id = prod.id
        JOIN locations l ON sq.location_id = l.id
        JOIN warehouses w ON l.warehouse_id = w.id
        JOIN warehouse_categories wc ON w.category_id = wc.id
        WHERE p.company_id = %s 
          AND l.type = 'internal'
          AND wc.name = %s
          AND sq.quantity > 0
        GROUP BY w.id, w.name
        ORDER BY total_value DESC
        LIMIT %s
    """
    # Nota: en la query usé 'p.company_id' pero el join es 'prod'. Corrigiendo alias:
    query = query.replace("p.company_id", "prod.company_id")
    
    results = execute_query(query, (company_id, category_name, limit), fetchall=True)
    return [dict(r) for r in results]

def get_material_flow_series(company_id, days=30):
    """
    [MEJORADO] Soporta parámetro 'days' y corrige valorización de despachos.
    """
    # 1. Generar serie de fechas dinámica
    dates_query = f"""
        SELECT to_char(d, 'YYYY-MM-DD') as day 
        FROM generate_series(CURRENT_DATE - INTERVAL '{days} days', CURRENT_DATE, '1 day') d
    """
    
    # 2. Query Despachado (Principal -> Contrata)
    # FIX: Si price_unit es 0, usamos standard_price del producto
    query_dispatch = f"""
        SELECT to_char(p.date_done, 'YYYY-MM-DD') as day, 
               SUM(sm.quantity_done * COALESCE(NULLIF(sm.price_unit, 0), prod.standard_price)) as val
        FROM stock_moves sm
        JOIN products prod ON sm.product_id = prod.id
        JOIN pickings p ON sm.picking_id = p.id
        JOIN locations l_src ON sm.location_src_id = l_src.id
        JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        JOIN warehouse_categories wc_src ON w_src.category_id = wc_src.id
        WHERE p.company_id = %s 
          AND p.state = 'done'
          AND wc_src.name ILIKE '%%PRINCIPAL%%' -- Flexible
          AND p.date_done >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY day
    """
    
    # 3. Query Liquidado (Consumo final)
    query_liquidated = f"""
        SELECT to_char(p.date_done, 'YYYY-MM-DD') as day, 
               SUM(sm.quantity_done * COALESCE(NULLIF(sm.price_unit, 0), prod.standard_price)) as val
        FROM stock_moves sm
        JOIN products prod ON sm.product_id = prod.id
        JOIN pickings p ON sm.picking_id = p.id
        JOIN locations l_dest ON sm.location_dest_id = l_dest.id
        WHERE p.company_id = %s 
          AND p.state = 'done'
          AND l_dest.category IN ('CLIENTE', 'CONTRATA CLIENTE')
          AND p.date_done >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY day
    """
    
    dates = [r['day'] for r in execute_query(dates_query, (), fetchall=True)]
    dispatch_data = {r['day']: r['val'] for r in execute_query(query_dispatch, (company_id,), fetchall=True)}
    liquidated_data = {r['day']: r['val'] for r in execute_query(query_liquidated, (company_id,), fetchall=True)}
    
    result = []
    for d in dates:
        # Recortar fecha para mostrar solo DD/MM (e.g., "2023-11-25" -> "25/11")
        display_day = f"{d[8:]}/{d[5:7]}" 
        result.append({
            "day": display_day,
            "dispatch": dispatch_data.get(d, 0.0),
            "liquidated": liquidated_data.get(d, 0.0)
        })
    return result

def get_abc_stats(company_id):
    """
    Clasificación ABC simple basada en valor total actual.
    A: Top 80% del valor. B: Siguiente 15%. C: Último 5%.
    """
    # Obtener todos los productos con stock > 0 ordenados por valor total
    query = """
        SELECT p.sku, SUM(sq.quantity * p.standard_price) as total_val
        FROM stock_quants sq JOIN products p ON sq.product_id = p.id
        WHERE p.company_id = %s AND sq.quantity > 0
        GROUP BY p.id ORDER BY total_val DESC
    """
    rows = execute_query(query, (company_id,), fetchall=True)
    
    total_inventory_val = sum(r['total_val'] for r in rows)
    if total_inventory_val == 0: return {"A": 0, "B": 0, "C": 0}
    
    accumulated = 0
    counts = {"A": 0, "B": 0, "C": 0}
    
    for r in rows:
        accumulated += r['total_val']
        perc = accumulated / total_inventory_val
        if perc <= 0.80: counts["A"] += 1
        elif perc <= 0.95: counts["B"] += 1
        else: counts["C"] += 1
            
    return counts

def get_reverse_logistics_rate(company_id):
    """
    Tasa de Devolución = (Valor Retirado / Valor Despachado) en los últimos 30 días.
    [CORREGIDO] Arreglado el JOIN de almacén en q_out.
    """
    # 1. Valor Retirado (IN desde Obras/Devoluciones)
    q_ret = """
        SELECT COALESCE(SUM(sm.quantity_done * sm.price_unit), 0) as val
        FROM stock_moves sm 
        JOIN pickings p ON sm.picking_id = p.id
        JOIN picking_types pt ON p.picking_type_id = pt.id
        WHERE p.company_id = %s 
          AND p.state = 'done' 
          AND (pt.code = 'RET' OR p.custom_operation_type ILIKE '%%Devoluci%%')
          AND p.date_done >= CURRENT_DATE - INTERVAL '30 days'
    """
    val_ret = execute_query(q_ret, (company_id,), fetchone=True)['val']
    
    # 2. Valor Despachado (OUT a Obras desde Principal)
    q_out = """
        SELECT COALESCE(SUM(sm.quantity_done * sm.price_unit), 0) as val
        FROM stock_moves sm 
        JOIN pickings p ON sm.picking_id = p.id
        -- [FIX] JOIN CORRECTO: Move -> Location Src -> Warehouse
        JOIN locations l_src ON sm.location_src_id = l_src.id
        JOIN warehouses w_src ON l_src.warehouse_id = w_src.id
        JOIN warehouse_categories wc ON w_src.category_id = wc.id
        
        WHERE p.company_id = %s 
          AND p.state = 'done' 
          AND wc.name = 'ALMACEN PRINCIPAL'
          AND p.date_done >= CURRENT_DATE - INTERVAL '30 days'
    """
    val_out = execute_query(q_out, (company_id,), fetchone=True)['val']
    
    # Evitar división por cero
    rate = (val_ret / val_out * 100) if val_out > 0 else 0.0
    return rate

def get_distinct_filter_values(company_id, field, warehouse_id=None):
    """
    Obtiene valores únicos para los filtros de dropdown (Ubicación, Categoría).
    [MEJORA] Normaliza a mayúsculas para unificar opciones duplicadas por case.
    """
    column_map = {
        'location_name': 'l.name',
        'category_name': 'pc.name'
    }
    
    db_col = column_map.get(field)
    if not db_col: return []

    # [MEJORA] Usamos DISTINCT UPPER() para limpiar la lista visual
    query = f"""
        SELECT DISTINCT UPPER({db_col}) as value
        FROM stock_quants sq
        JOIN locations l ON sq.location_id = l.id
        JOIN products p ON sq.product_id = p.id
        LEFT JOIN product_categories pc ON p.category_id = pc.id
        WHERE l.type = 'internal' AND p.company_id = %s AND sq.quantity > 0.001
    """
    params = [company_id]

    if warehouse_id:
        query += " AND l.warehouse_id = %s"
        params.append(warehouse_id)
    
    query += " ORDER BY value"
    
    res = execute_query(query, tuple(params), fetchall=True)
    return [r['value'] for r in res if r['value']]

