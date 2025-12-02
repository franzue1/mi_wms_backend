# app/database/utils.py
import psycopg2.extras

def _create_warehouse_with_cursor(cursor, name, code, category_id, company_id, social_reason, ruc, email, phone, address, status):
    """Función interna para crear almacén durante init_db."""
    cursor.execute(
        """INSERT INTO warehouses (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (company_id, code) DO NOTHING RETURNING id""",
        (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status)
    )
    row = cursor.fetchone()
    if row:
        create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=True, warehouse_id=row[0])

def create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=False, warehouse_id=None):
    """
    Crea ubicaciones y tipos de operación (IN/OUT/INT/RET).
    [CORREGIDO] Asegura que el almacén exista y tenga ID antes de crear hijos.
    """
    
    # --- BLOQUE DE SEGURIDAD NUEVO ---
    # Si no es para un almacén existente (es nuevo) y no tenemos ID, debemos crearlo primero.
    if not for_existing and warehouse_id is None:
        cursor.execute(
            """INSERT INTO warehouses (company_id, name, code, category_id, status) 
               VALUES (%s, %s, %s, %s, 'activo') 
               ON CONFLICT (company_id, code) DO UPDATE SET name = EXCLUDED.name 
               RETURNING id""", 
            (company_id, name, code, category_id)
        )
        row = cursor.fetchone()
        if row:
            warehouse_id = row[0]
        else:
            # Fallback por si el ON CONFLICT no devolvió nada (raro pero posible)
            cursor.execute("SELECT id FROM warehouses WHERE company_id=%s AND code=%s", (company_id, code))
            warehouse_id = cursor.fetchone()[0]
            
    if warehouse_id is None:
        raise ValueError(f"Error crítico: No se pudo obtener ID para el almacén '{name}'")
    # -------------------------------

    # 1. Ubicaciones
    stock_name = f"{code}/Stock"
    damaged_name = f"{code}/Averiados"
    
    cursor.execute("INSERT INTO locations (company_id, name, path, type, category, warehouse_id) VALUES (%s, 'Stock', %s, 'internal', %s, %s) ON CONFLICT (company_id, path) DO UPDATE SET warehouse_id=EXCLUDED.warehouse_id RETURNING id", (company_id, stock_name, "ALMACEN PRINCIPAL", warehouse_id))
    stock_id = cursor.fetchone()[0]
    
    cursor.execute("INSERT INTO locations (company_id, name, path, type, category, warehouse_id) VALUES (%s, 'Averiados', %s, 'internal', %s, %s) ON CONFLICT (company_id, path) DO NOTHING RETURNING id", (company_id, damaged_name, "AVERIADO", warehouse_id))
    damaged_row = cursor.fetchone()
    damaged_id = damaged_row[0] if damaged_row else None # Podría ya existir
    
    if not damaged_id: # Buscar si no se insertó
        cursor.execute("SELECT id FROM locations WHERE company_id=%s AND path=%s", (company_id, damaged_name))
        damaged_id = cursor.fetchone()[0]

    # 2. Ubicaciones Virtuales (IDs)
    cursor.execute("SELECT id FROM locations WHERE category='PROVEEDOR' AND company_id=%s", (company_id,))
    v_row = cursor.fetchone()
    v_id = v_row[0] if v_row else None
    
    cursor.execute("SELECT id FROM locations WHERE category='CLIENTE' AND company_id=%s", (company_id,))
    c_row = cursor.fetchone()
    c_id = c_row[0] if c_row else None

    # Validación extra para evitar errores silenciosos en los tipos de operación
    if not v_id or not c_id:
        print(f"[WARN] Faltan ubicaciones virtuales para Cía {company_id}. Los tipos IN/OUT podrían fallar.")

    # 3. Tipos de Operación
    ops = [
        (company_id, f"Recepciones {code}", 'IN', warehouse_id, v_id, stock_id),
        (company_id, f"Liquidaciones {code}", 'OUT', warehouse_id, stock_id, c_id),
        (company_id, f"Despachos {code}", 'INT', warehouse_id, None, None), # INT no tiene defaults fijos usualmente
        (company_id, f"Retiros {code}", 'RET', warehouse_id, c_id, damaged_id)
    ]
    
    cursor.executemany("""
        INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING
    """, ops)

