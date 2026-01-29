# app/api/employees.py

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData, verify_company_access
import asyncio
import traceback

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- Permisos Sugeridos ---
# Crearás estos permisos en tu tabla 'permissions' si no existen, o reusarás 'config.can_edit'
# Por ahora usaremos 'config.can_view' y 'config.can_edit' como base.

@router.get("/", response_model=List[schemas.EmployeeResponse])
async def get_employees(
    auth: AuthDependency,
    company_id: int = Query(...),
    skip: int = 0,
    limit: int = 50,
    search: Optional[str] = None,
    status: Optional[str] = 'active'
):
    """Obtiene la lista paginada de empleados."""
    verify_company_access(auth, company_id)
    # Permiso básico de lectura
    if "employees.can_view" not in auth.permissions and "employees.can_edit" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    try:
        employees, total = await asyncio.to_thread(
            db.get_employees_paginated, company_id, skip, limit, search, status
        )
        # Podrías devolver el total en un header o en una estructura envuelta, 
        # pero por simplicidad con Flet devolvemos la lista directa aquí.
        return employees
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/", response_model=dict)
async def create_employee(
    data: schemas.EmployeeCreate,
    auth: AuthDependency,
    company_id: int = Query(...) # Forzamos company_id explícito en query para validar
):
    """Crea un nuevo empleado."""
    verify_company_access(auth, company_id)
    if "employees.can_edit" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    try:
        res = await asyncio.to_thread(
            db.create_employee,
            company_id,
            data.first_name,
            data.last_name,
            data.document_number,
            data.internal_code,
            data.job_title
        )
        return {"id": res['id'], "message": "Empleado creado correctamente"}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

@router.put("/{employee_id}", response_model=dict)
async def update_employee(
    employee_id: int,
    data: schemas.EmployeeUpdate,
    auth: AuthDependency
):
    """Actualiza datos de un empleado."""
    if "employees.can_edit" not in auth.permissions:
        raise HTTPException(status_code=403, detail="No autorizado")

    try:
        # Convertir a dict eliminando Nones
        updates = data.dict(exclude_unset=True)
        
        success = await asyncio.to_thread(db.update_employee, employee_id, updates)
        if not success:
            raise HTTPException(status_code=404, detail="Empleado no encontrado")
            
        return {"message": "Empleado actualizado"}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search/simple", response_model=List[dict])
async def search_employees(
    auth: AuthDependency,
    company_id: int = Query(...),
    term: str = Query(...)
):
    """
    Búsqueda rápida para el dropdown de Operaciones.
    Accesible para cualquier usuario operativo.
    """
    verify_company_access(auth, company_id)
    
    try:
        results = await asyncio.to_thread(db.search_employees_simple, company_id, term)
        # Formateamos para que el Dropdown de Flet lo consuma fácil
        # Retornamos dicts simples
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))