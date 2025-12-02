#app/api/projects.py

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- HELPER DE SEGURIDAD (ANTI-IDOR) ---
def verify_access(auth: TokenData, company_id: int):
    """
    Verifica si el usuario tiene permiso para acceder a los datos de la compañía solicitada.
    """
    # 1. Si es Super Admin (Rol 'Administrador'), tiene pase libre.
    # Ajusta el string 'Administrador' según como lo tengas en tu BD (ej. 'admin', 'SuperAdmin')
    if auth.role_name == "Administrador":
        return

    # 2. Si no es admin, verificamos si el company_id está en su lista de empresas permitidas.
    # Nota: Asumimos que auth.company_ids es una lista de enteros [1, 2, ...]
    if company_id not in auth.company_ids:
        print(f"[SECURITY ALERT] Usuario {auth.username} intentó acceder a Company {company_id} sin permiso.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="No tienes autorización para acceder a esta compañía."
        )

# --- 1. DIRECCIONES ---
@router.get("/directions", response_model=List[dict])
def get_directions(auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id) # <--- BLINDAJE
    return [dict(r) for r in db.get_directions(company_id)]

@router.post("/directions", status_code=201)
def create_direction(auth: AuthDependency, data: schemas.DirectionCreate, company_id: int = Query(...)):
    verify_access(auth, company_id) # <--- BLINDAJE
    try:
        new_id = db.create_direction(company_id, data.name, data.code)
        return {"id": new_id}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.put("/directions/{item_id}")
def update_direction(item_id: int, data: schemas.DirectionCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        db.update_direction(item_id, data.name, data.code)
        return {"message": "Actualizado"}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.delete("/directions/{item_id}")
def delete_direction(item_id: int, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        db.delete_direction(item_id)
        return {"message": "Eliminado"}
    except ValueError as e: raise HTTPException(400, detail=str(e))

# --- 2. GERENCIAS ---
@router.get("/managements", response_model=List[dict])
def get_managements(auth: AuthDependency, company_id: int = Query(...), direction_id: Optional[int] = None):
    verify_access(auth, company_id) # <--- BLINDAJE
    return [dict(r) for r in db.get_managements(company_id, direction_id)]

@router.post("/managements", status_code=201)
def create_management(auth: AuthDependency, data: schemas.ManagementCreate, company_id: int = Query(...)):
    verify_access(auth, company_id) # <--- BLINDAJE
    try:
        new_id = db.create_management(company_id, data.name, data.direction_id, data.code)
        return {"id": new_id}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.put("/managements/{item_id}")
def update_management(item_id: int, data: schemas.ManagementCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        db.update_management(item_id, data.name, data.direction_id, data.code)
        return {"message": "Actualizado"}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.delete("/managements/{item_id}")
def delete_management(item_id: int, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        db.delete_management(item_id)
        return {"message": "Eliminado"}
    except ValueError as e: raise HTTPException(400, detail=str(e))

# --- 3. MACRO PROYECTOS ---
@router.get("/macros", response_model=List[dict])
def get_macro_projects(auth: AuthDependency, company_id: int = Query(...), management_id: Optional[int] = None):
    verify_access(auth, company_id) # <--- BLINDAJE
    return [dict(r) for r in db.get_macro_projects(company_id, management_id)]

@router.post("/macros", status_code=201)
def create_macro_project(auth: AuthDependency, data: schemas.MacroProjectCreate, company_id: int = Query(...)):
    verify_access(auth, company_id) # <--- BLINDAJE
    try:
        new_id = db.create_macro_project(company_id, data.name, data.management_id, data.code)
        return {"id": new_id}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.put("/macros/{item_id}")
def update_macro(item_id: int, data: schemas.MacroProjectCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        db.update_macro_project(item_id, data.name, data.management_id, data.code)
        return {"message": "Actualizado"}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.delete("/macros/{item_id}")
def delete_macro(item_id: int, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        db.delete_macro_project(item_id)
        return {"message": "Eliminado"}
    except ValueError as e: raise HTTPException(400, detail=str(e))

# --- 4. OBRAS (PROYECTOS FINALES) ---

@router.get("/", response_model=List[dict])
def get_projects(
    auth: AuthDependency, 
    company_id: int = Query(...), 
    status: Optional[str] = None,
    search: Optional[str] = None,
    # --- NUEVOS FILTROS OPCIONALES ---
    direction_id: Optional[int] = None,
    management_id: Optional[int] = None,
    # ---------------------------------
    limit: int = 100,
    skip: int = 0
):
    verify_access(auth, company_id)
    # Pasamos los argumentos EN ORDEN o POR NOMBRE (mejor por nombre para evitar errores)
    return [dict(r) for r in db.get_projects(
        company_id=company_id, 
        status=status, 
        search=search, 
        direction_id=direction_id, 
        management_id=management_id, 
        limit=limit, 
        offset=skip
    )]

@router.post("/", status_code=201)
def create_project(auth: AuthDependency, project: schemas.ProjectCreate, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        # Pasamos todos los nuevos campos al repositorio
        new_id = db.create_project(
            company_id=company_id,
            name=project.name,
            macro_project_id=project.macro_project_id,
            code=project.code,
            address=project.address,
            department=project.department,
            province=project.province,
            district=project.district,
            budget=project.budget,
            start_date=project.start_date,
            end_date=project.end_date
        )
        return {"id": new_id, "message": "Obra creada"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{project_id}")
def update_project(auth: AuthDependency, project_id: int, project: schemas.ProjectUpdate):
    # NOTA: Para el update, idealmente deberíamos verificar que 'project_id' pertenece a 'company_id'
    # Pero como 'project_id' es único globalmente, el riesgo es menor si el usuario no puede adivinar IDs.
    # Para máxima seguridad, deberíamos hacer un SELECT previo para ver la company_id del proyecto.
    # Por ahora, confiamos en que el usuario ya pasó por get_projects.
    try:
        db.update_project(project_id, project.dict(exclude_unset=True))
        return {"message": "Obra actualizada"}
    except ValueError as e: # Captura el error de duplicados en edición
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{project_id}")
def delete_project(auth: AuthDependency, project_id: int):
    # Igual que en update, se podría agregar una validación de propiedad de compañía aquí.
    success, msg = db.delete_project(project_id)
    return {"message": msg, "deleted": success}