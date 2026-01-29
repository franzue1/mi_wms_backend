# app/database/repositories/employee_repo.py

import psycopg2.extras
from ..core import get_db_connection, return_db_connection, execute_query, execute_commit_query

def create_employee(company_id, first_name, last_name, document_number, internal_code=None, job_title=None):
    """Crea un nuevo empleado."""
    # Normalización básica
    doc_num = document_number.strip().upper()
    
    query = """
        INSERT INTO employees (company_id, first_name, last_name, document_number, internal_code, job_title)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, full_name
    """
    params = (company_id, first_name.strip(), last_name.strip(), doc_num, internal_code, job_title)
    
    try:
        res = execute_commit_query(query, params, fetchone=True)
        return res
    except Exception as e:
        if "uq_employee_document_company" in str(e):
            raise ValueError(f"El documento {doc_num} ya existe en esta compañía.")
        raise e

def update_employee(employee_id, updates: dict):
    """Actualiza campos de un empleado."""
    if not updates: return False
    
    set_clauses = []
    params = []
    
    allowed = {'first_name', 'last_name', 'document_number', 'internal_code', 'job_title', 'status'}
    
    for key, val in updates.items():
        if key in allowed:
            set_clauses.append(f"{key} = %s")
            # Normalización ligera
            if isinstance(val, str): val = val.strip()
            params.append(val)
            
    if not set_clauses: return False
    
    params.append(employee_id)
    query = f"UPDATE employees SET {', '.join(set_clauses)} WHERE id = %s RETURNING id"
    
    try:
        res = execute_commit_query(query, tuple(params), fetchone=True)
        return bool(res)
    except Exception as e:
        if "uq_employee_document_company" in str(e):
            raise ValueError("El documento ya pertenece a otro empleado.")
        raise e

def get_employees_paginated(company_id, skip=0, limit=50, search_query=None, status_filter='active'):
    """Lista paginada con filtros."""
    params = [company_id]
    where_clauses = ["company_id = %s"]
    
    if status_filter:
        where_clauses.append("status = %s")
        params.append(status_filter)
        
    if search_query:
        # Búsqueda por nombre completo, DNI o código interno
        where_clauses.append("(full_name ILIKE %s OR document_number ILIKE %s OR internal_code ILIKE %s)")
        term = f"%{search_query}%"
        params.extend([term, term, term])
        
    where_str = " AND ".join(where_clauses)
    
    # Count Total
    count_query = f"SELECT COUNT(*) FROM employees WHERE {where_str}"
    total = execute_query(count_query, tuple(params), fetchone=True)[0]
    
    # Data
    data_query = f"""
        SELECT * FROM employees 
        WHERE {where_str} 
        ORDER BY first_name ASC 
        LIMIT %s OFFSET %s
    """
    params.extend([limit, skip])
    rows = execute_query(data_query, tuple(params), fetchall=True)
    
    return [dict(r) for r in rows], total

def get_employee_by_id(employee_id):
    query = "SELECT * FROM employees WHERE id = %s"
    res = execute_query(query, (employee_id,), fetchone=True)
    return dict(res) if res else None

def search_employees_simple(company_id, term):
    """
    Búsqueda ultrarrápida para el autocompletado en Operaciones.
    Devuelve solo lo necesario para el dropdown.
    """
    t = f"%{term}%"
    query = """
        SELECT id, full_name, document_number, job_title 
        FROM employees 
        WHERE company_id = %s 
          AND status = 'active'
          AND (full_name ILIKE %s OR document_number ILIKE %s)
        ORDER BY full_name LIMIT 20
    """
    rows = execute_query(query, (company_id, t, t), fetchall=True)
    return [dict(r) for r in rows]