import psycopg2.extras

def _create_warehouse_with_cursor(cursor, name, code, category_id, company_id, social_reason, ruc, email, phone, address, status):
    """Función interna para crear almacén durante init_db."""
    # Aseguramos que el código del almacén sí sea mayúscula (estándar logístico)
    clean_code = code.strip().upper() if code else None
    
    # El nombre del almacén lo dejamos tal cual viene (o lo limpiamos básico)
    clean_name = name.strip() if name else None

    cursor.execute(
        """INSERT INTO warehouses (name, code, category_id, company_id, social_reason, ruc, email, phone, address, status) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (company_id, code) DO NOTHING RETURNING id""",
        (clean_name, clean_code, category_id, company_id, social_reason, ruc, email, phone, address, status)
    )
    row = cursor.fetchone()
    if row:
        create_warehouse_with_data(cursor, clean_name, clean_code, company_id, category_id, for_existing=True, warehouse_id=row[0])

def create_warehouse_with_data(cursor, name, code, company_id, category_id, for_existing=False, warehouse_id=None):
    """
    Crea ubicaciones y tipos de operación.
    [AJUSTE FINAL] 
    - Ubicaciones: "STOCK" y "AVERIADOS" (Mayúsculas).
    - Operaciones: "Recepciones", "Despachos" (Mantiene formato original legible).
    """
    # 1. Normalización
    clean_code = code.strip().upper()
    clean_name = name.strip()

    # --- BLOQUE DE SEGURIDAD ---
    if not for_existing and warehouse_id is None:
        cursor.execute(
            """INSERT INTO warehouses (company_id, name, code, category_id, status) 
               VALUES (%s, %s, %s, %s, 'activo') 
               ON CONFLICT (company_id, code) DO UPDATE SET name = EXCLUDED.name 
               RETURNING id""", 
            (company_id, clean_name, clean_code, category_id)
        )
        row = cursor.fetchone()
        if row:
            warehouse_id = row[0]
        else:
            cursor.execute("SELECT id FROM warehouses WHERE company_id=%s AND code=%s", (company_id, clean_code))
            warehouse_id = cursor.fetchone()[0]
            
    if warehouse_id is None:
        raise ValueError(f"Error crítico: No se pudo obtener ID para el almacén '{clean_name}'")
    # ---------------------------

    # 2. Ubicaciones (AQUÍ SÍ FORZAMOS MAYÚSCULAS)
    stock_path = f"{clean_code}/STOCK"
    damaged_path = f"{clean_code}/AVERIADOS"
    
    # Crear STOCK (Mayúscula)
    cursor.execute(
        """INSERT INTO locations (company_id, name, path, type, category, warehouse_id) 
           VALUES (%s, 'STOCK', %s, 'internal', %s, %s) 
           ON CONFLICT (company_id, path) DO UPDATE SET warehouse_id=EXCLUDED.warehouse_id RETURNING id""", 
        (company_id, stock_path, "ALMACEN PRINCIPAL", warehouse_id)
    )
    stock_id = cursor.fetchone()[0]
    
    # Crear AVERIADOS (Mayúscula)
    cursor.execute(
        """INSERT INTO locations (company_id, name, path, type, category, warehouse_id) 
           VALUES (%s, 'AVERIADOS', %s, 'internal', %s, %s) 
           ON CONFLICT (company_id, path) DO NOTHING RETURNING id""", 
        (company_id, damaged_path, "AVERIADO", warehouse_id)
    )
    damaged_row = cursor.fetchone()
    damaged_id = damaged_row[0] if damaged_row else None 
    
    if not damaged_id:
        cursor.execute("SELECT id FROM locations WHERE company_id=%s AND path=%s", (company_id, damaged_path))
        damaged_id = cursor.fetchone()[0]

    # 3. Ubicaciones Virtuales (IDs)
    cursor.execute("SELECT id FROM locations WHERE category='PROVEEDOR' AND company_id=%s", (company_id,))
    v_row = cursor.fetchone()
    v_id = v_row[0] if v_row else None
    
    cursor.execute("SELECT id FROM locations WHERE category='CLIENTE' AND company_id=%s", (company_id,))
    c_row = cursor.fetchone()
    c_id = c_row[0] if c_row else None

    if not v_id or not c_id:
        print(f"[WARN] Faltan ubicaciones virtuales para Cía {company_id}. Los tipos IN/OUT podrían fallar.")

    # 4. Tipos de Operación (MANTENEMOS FORMATO LEGIBLE 'Recepciones...')
    # El código del almacén sí va en mayúscula porque es un código.
    ops = [
        (company_id, f"Recepciones {clean_code}", 'IN', warehouse_id, v_id, stock_id),
        (company_id, f"Liquidaciones {clean_code}", 'OUT', warehouse_id, stock_id, c_id),
        (company_id, f"Despachos {clean_code}", 'INT', warehouse_id, None, None),
        (company_id, f"Retiros {clean_code}", 'RET', warehouse_id, c_id, damaged_id)
    ]
    
    cursor.executemany("""
        INSERT INTO picking_types (company_id, name, code, warehouse_id, default_location_src_id, default_location_dest_id) 
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (company_id, name) DO NOTHING
    """, ops)