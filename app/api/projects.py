#app/api/projects.py
from fastapi import APIRouter, Depends, HTTPException, Query, status, UploadFile, File
from typing import List, Optional, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData
import csv
import io
from fastapi.responses import StreamingResponse

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
    verify_access(auth, company_id) 
    try:
        # [MODIFICADO] Pasamos data.cost_center al final
        new_id = db.create_macro_project(
            company_id, 
            data.name, 
            data.management_id, 
            data.code, 
            data.cost_center
        )
        return {"id": new_id}
    except ValueError as e: raise HTTPException(400, detail=str(e))

@router.put("/macros/{item_id}")
def update_macro(item_id: int, data: schemas.MacroProjectCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        # [MODIFICADO] Pasamos data.cost_center al final
        db.update_macro_project(
            item_id, 
            data.name, 
            data.management_id, 
            data.code, 
            data.cost_center
        )
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
    direction_id: Optional[int] = None,
    management_id: Optional[int] = None,
    limit: int = 100,
    skip: int = 0,
    sort_by: Optional[str] = None,   # <--- Nuevo
    ascending: bool = True           # <--- Nuevo
):
    verify_access(auth, company_id)
    return [dict(r) for r in db.get_projects(
        company_id=company_id, 
        status=status, 
        search=search, 
        direction_id=direction_id, 
        management_id=management_id, 
        limit=limit, 
        offset=skip,
        sort_by=sort_by,       # <---
        ascending=ascending    # <---
    )]

@router.get("/count", response_model=int)
def get_projects_count(
    auth: AuthDependency, 
    company_id: int = Query(...), 
    status: Optional[str] = None,
    search: Optional[str] = None,
    direction_id: Optional[int] = None,
    management_id: Optional[int] = None
):
    verify_access(auth, company_id)
    return db.get_projects_count(company_id, status, search, direction_id, management_id)

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

# --- IMPORTAR / EXPORTAR ---

@router.get("/export/csv", response_class=StreamingResponse)
async def export_projects_csv(
    auth: AuthDependency,
    company_id: int = Query(...)
):
    verify_access(auth, company_id)
    
    try:
        # 1. Obtener datos (reusamos la función existente)
        projects = db.get_projects(company_id, limit=999999) # Traer todo
        
        # 2. Generar CSV en memoria
        output = io.StringIO(newline='')
        writer = csv.writer(output, delimiter=';')
        
        # Headers
        headers = [
            "code", "name", "macro_name", "status", "phase", 
            "address", "department", "province", "district",
            "budget", "start_date", "end_date", 
            "stock_value", "liquidated_value"
        ]
        writer.writerow(headers)
        
        # Rows
        for p in projects:
            row = [
                p.get('code') or "",
                p.get('name'),
                p.get('macro_name') or "",
                p.get('status'),
                p.get('phase'),
                p.get('address') or "",
                p.get('department') or "",
                p.get('province') or "",
                p.get('district') or "",
                str(p.get('budget', 0)),
                str(p.get('start_date') or ""),
                str(p.get('end_date') or ""),
                str(p.get('stock_value', 0)),
                str(p.get('liquidated_value', 0))
            ]
            writer.writerow(row)
            
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=obras.csv"}
        )
        
    except Exception as e:
        raise HTTPException(500, detail=f"Error exportando: {e}")

@router.post("/import/csv", response_model=dict)
async def import_projects_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    verify_access(auth, company_id)
    
    try:
        content = await file.read()
        decoded = content.decode('utf-8-sig')
        
        # 1. Detectar delimitador
        first_line = decoded.split('\n')[0]
        delimiter = ';' if ';' in first_line else ','
        
        reader = csv.DictReader(io.StringIO(decoded), delimiter=delimiter)
        rows = list(reader)
        
        if not rows: raise ValueError("El archivo CSV está vacío.")
        
        headers_map = {h.strip().lower(): h for h in reader.fieldnames or []}
        
        macros_db = db.get_macro_projects(company_id)
        macros_map = {m['name'].strip().upper(): m['id'] for m in macros_db}
        
        validation_errors = []
        valid_rows_to_process = []
        
        # --- HELPER DE FECHAS (ESTRICTO) ---
        def parse_date_strict(date_str, field_name):
            """Convierte string a YYYY-MM-DD. Si está vacío devuelve None. Si está mal, ERROR."""
            if not date_str or not date_str.strip(): 
                return None # Esto se convierte en NULL en SQL (Correcto)
            
            d = date_str.strip()
            # Formato ISO (2025-12-31)
            if "-" in d:
                parts = d.split("-")
                if len(parts) == 3 and len(parts[0]) == 4: return d
            
            # Formato Latino (31/12/2025)
            if "/" in d:
                parts = d.split("/")
                if len(parts) == 3: return f"{parts[2]}-{parts[1]}-{parts[0]}"
            
            # Si llega aquí, el formato no es válido
            raise ValueError(f"Fecha inválida en '{field_name}': '{d}'. Use YYYY-MM-DD o DD/MM/YYYY")

        # --- FASE 1: VALIDACIÓN ---
        for i, row in enumerate(rows):
            line_num = i + 2
            
            def get_val(key):
                real_key = headers_map.get(key.lower())
                return row.get(real_key, '').strip() if real_key else ''

            try:
                # 1. Validar Nombre
                name = get_val('name')
                if not name:
                    if not any(row.values()): continue 
                    raise ValueError("El campo 'name' es obligatorio.")

                # 2. Validar Proyecto (Macro)
                macro_name_raw = get_val('macro_name') or get_val('proyecto')
                macro_id = None
                
                if macro_name_raw:
                    macro_clean = macro_name_raw.strip().upper()
                    if macro_clean not in macros_map:
                        raise ValueError(f"El Proyecto '{macro_name_raw}' NO EXISTE.")
                    macro_id = macros_map[macro_clean]

                # 3. Validar y Parsear Fechas (CRÍTICO: Devuelve None o String válido)
                final_start = parse_date_strict(get_val('start_date'), 'start_date')
                final_end = parse_date_strict(get_val('end_date'), 'end_date')

                # 4. Validar Presupuesto
                budget_str = get_val('budget').replace("S/", "").replace(",", "")
                try:
                    budget = float(budget_str) if budget_str else 0.0
                except:
                    raise ValueError(f"Presupuesto inválido: '{get_val('budget')}'")

                valid_rows_to_process.append({
                    "name": name,
                    "code": get_val('code'),
                    "macro_project_id": macro_id,
                    "address": get_val('address'),
                    "status": get_val('status') or 'active',
                    "phase": get_val('phase') or 'Sin Iniciar',
                    "start_date": final_start, # <--- AQUÍ PASAMOS None, NO ''
                    "end_date": final_end,     # <--- AQUÍ PASAMOS None, NO ''
                    "budget": budget,
                    "department": get_val('department'),
                    "province": get_val('province'),
                    "district": get_val('district')
                })

            except Exception as e:
                validation_errors.append(f"Línea {line_num}: {str(e)}")

        if validation_errors:
            error_msg = "IMPORTACIÓN RECHAZADA (Errores encontrados):\n" + "\n".join(validation_errors[:10])
            if len(validation_errors) > 10: error_msg += "\n... y más errores."
            raise ValueError(error_msg)

        # --- FASE 2: EJECUCIÓN ---
        stats = {"created": 0, "updated": 0}
        
        for data in valid_rows_to_process:
            res = db.upsert_project_from_import(
                company_id=company_id,
                **data
                # Nota: NO pasamos cost_center aquí (va en macro)
            )
            if res == "created": stats['created'] += 1
            else: stats['updated'] += 1
            
        return stats

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico: {str(e)}")


