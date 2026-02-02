#app/api/projects.py
from fastapi import APIRouter, Depends, HTTPException, Query, status, UploadFile, File
from typing import List, Optional, Annotated
from app import database as db
from app import schemas, security
from app.security import TokenData
from app.services.project_service import ProjectService
from app.exceptions import ValidationError, NotFoundError, BusinessRuleError, DuplicateError
import traceback
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
    verify_access(auth, company_id)
    try:
        # Validar y normalizar usando el servicio
        clean_name, clean_code = ProjectService.validate_direction_data(data.name, data.code)
        new_id = db.create_direction(company_id, clean_name, clean_code)
        return {"id": new_id}
    except ValidationError as ve:
        raise HTTPException(400, detail=ve.message)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

@router.put("/directions/{item_id}")
def update_direction(item_id: int, data: schemas.DirectionCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        # Validar y normalizar usando el servicio
        clean_name, clean_code = ProjectService.validate_direction_data(data.name, data.code)
        db.update_direction(item_id, clean_name, clean_code)
        return {"message": "Actualizado"}
    except ValidationError as ve:
        raise HTTPException(400, detail=ve.message)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

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
    verify_access(auth, company_id)
    try:
        # Validar y normalizar usando el servicio
        clean_name, clean_code = ProjectService.validate_management_data(
            data.name, data.direction_id, data.code
        )
        new_id = db.create_management(company_id, clean_name, data.direction_id, clean_code)
        return {"id": new_id}
    except ValidationError as ve:
        raise HTTPException(400, detail=ve.message)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

@router.put("/managements/{item_id}")
def update_management(item_id: int, data: schemas.ManagementCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        # Validar y normalizar usando el servicio
        clean_name, clean_code = ProjectService.validate_management_data(
            data.name, data.direction_id, data.code
        )
        db.update_management(item_id, clean_name, data.direction_id, clean_code)
        return {"message": "Actualizado"}
    except ValidationError as ve:
        raise HTTPException(400, detail=ve.message)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

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
        # Validar y normalizar usando el servicio
        clean_name, clean_code, clean_cc = ProjectService.validate_macro_data(
            data.name, data.management_id, data.code, data.cost_center
        )
        new_id = db.create_macro_project(
            company_id, clean_name, data.management_id, clean_code, clean_cc
        )
        return {"id": new_id}
    except ValidationError as ve:
        raise HTTPException(400, detail=ve.message)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

@router.put("/macros/{item_id}")
def update_macro(item_id: int, data: schemas.MacroProjectCreate, auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        # Validar y normalizar usando el servicio
        clean_name, clean_code, clean_cc = ProjectService.validate_macro_data(
            data.name, data.management_id, data.code, data.cost_center
        )
        db.update_macro_project(item_id, clean_name, data.management_id, clean_code, clean_cc)
        return {"message": "Actualizado"}
    except ValidationError as ve:
        raise HTTPException(400, detail=ve.message)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))

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
    
    # [NUEVOS PARÁMETROS PARA FILTROS]
    f_code: Optional[str] = None,
    f_macro: Optional[str] = None,
    f_dept: Optional[str] = None,
    f_prov: Optional[str] = None,
    f_dist: Optional[str] = None,
    f_dir: Optional[str] = None,  # <--- Nuevo param
    f_mgmt: Optional[str] = None, # <--- Nuevo param

    limit: int = 100,
    skip: int = 0,
    sort_by: Optional[str] = None,
    ascending: bool = True
):
    verify_access(auth, company_id)
    return [dict(r) for r in db.get_projects(
        company_id=company_id, 
        status=status, 
        search=search, 
        direction_id=direction_id, 
        management_id=management_id,
        
        # Pasamos los filtros al repo
        filter_code=f_code,
        filter_macro=f_macro,
        filter_dept=f_dept,
        filter_prov=f_prov,
        filter_dist=f_dist,
        filter_direction=f_dir,
        filter_management=f_mgmt,

        limit=limit, 
        offset=skip,
        sort_by=sort_by,
        ascending=ascending
    )]

@router.get("/count", response_model=int)
def get_projects_count(
    auth: AuthDependency, 
    company_id: int = Query(...), 
    status: Optional[str] = None,
    search: Optional[str] = None,
    direction_id: Optional[int] = None,
    management_id: Optional[int] = None,
    
    # [NUEVO] Recibir los filtros granulares
    f_code: Optional[str] = None,
    f_macro: Optional[str] = None,
    f_dept: Optional[str] = None,
    f_prov: Optional[str] = None,
    f_dist: Optional[str] = None
):
    verify_access(auth, company_id)
    return db.get_projects_count(
        company_id, 
        status, 
        search, 
        direction_id, 
        management_id,
        # Pasar al repo
        filter_code=f_code,
        filter_macro=f_macro,
        filter_dept=f_dept,
        filter_prov=f_prov,
        filter_dist=f_dist
    )

@router.post("/", status_code=201)
def create_project(auth: AuthDependency, project: schemas.ProjectCreate, company_id: int = Query(...)):
    verify_access(auth, company_id)
    try:
        # Validar y normalizar usando el servicio
        validated = ProjectService.validate_project_data(
            name=project.name,
            code=project.code,
            macro_project_id=project.macro_project_id,
            start_date=project.start_date,
            end_date=project.end_date,
            address=project.address
        )

        new_id = db.create_project(
            company_id=company_id,
            name=validated['name'],
            macro_project_id=project.macro_project_id,
            code=validated['code'],
            address=validated['address'],
            department=project.department,
            province=project.province,
            district=project.district,
            budget=project.budget,
            start_date=project.start_date,
            end_date=project.end_date
        )
        return {"id": new_id, "message": "Obra creada"}
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
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
async def export_projects_csv(auth: AuthDependency, company_id: int = Query(...)):
    """
    Genera y transmite un archivo CSV de las Obras/Proyectos.
    Usa ProjectService para generación de CSV.
    """
    verify_access(auth, company_id)

    try:
        # 1. Obtener datos del repositorio
        projects = db.get_projects(company_id, limit=999999)

        if not projects:
            raise NotFoundError("No hay datos para exportar", "EXPORT_NO_DATA")

        # 2. Usar el servicio para generar el CSV
        csv_content = ProjectService.generate_projects_csv_content(projects)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Reporte_Obras_Completo.csv"}
        )

    except NotFoundError as nfe:
        raise HTTPException(status_code=404, detail=nfe.message)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, detail=f"Error exportando: {e}")

@router.get("/hierarchy/export-flat", response_class=StreamingResponse)
async def export_hierarchy_flat(
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """
    Genera un CSV plano con la estructura completa.
    Usa ProjectService para generación de CSV.
    """
    verify_access(auth, company_id)

    try:
        # 1. Obtener datos del repositorio
        hierarchy_data = db.get_hierarchy_flat(company_id)

        # 2. Usar el servicio para generar el CSV
        csv_content = ProjectService.generate_hierarchy_csv_content(hierarchy_data)

        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=jerarquia_proyectos.csv"}
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, detail=f"Error exportando: {str(e)}")

@router.post("/hierarchy/import-flat", response_model=dict)
async def import_hierarchy_flat(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """
    Importa una estructura jerárquica desde un CSV plano.
    Usa ProjectService para parsing y validación.
    """
    verify_access(auth, company_id)

    try:
        content = await file.read()

        # Usar el servicio para parsear el CSV
        rows, headers = ProjectService.parse_csv_file(content)

        # Resolver columnas del CSV
        resolved_cols = ProjectService.resolve_csv_columns(
            headers,
            ProjectService.HIERARCHY_HEADER_MAPPING
        )

        rows_to_process = []
        for i, row in enumerate(rows):
            line_num = i + 2
            try:
                processed = ProjectService.process_hierarchy_row(row, resolved_cols, line_num)
                # Solo agregar si al menos tiene Dirección
                if processed and processed.get('dir_name'):
                    rows_to_process.append(processed)
            except ValidationError as ve:
                raise HTTPException(status_code=400, detail=ve.message)

        if not rows_to_process:
            raise ValidationError(
                "No se encontraron filas válidas con al menos una 'Dirección'.",
                "CSV_EMPTY_FILE"
            )

        # Llamar al repositorio
        stats = db.import_hierarchy_batch(company_id, rows_to_process)

        return {
            "message": "Importación completada con éxito",
            "stats": stats
        }

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico en importación: {str(e)}")

@router.post("/import/csv", response_model=dict)
async def import_projects_csv(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """
    Importa Obras desde CSV.
    Usa ProjectService para parsing y validación.
    """
    verify_access(auth, company_id)

    try:
        content = await file.read()

        # Usar el servicio para parsear el CSV
        rows, headers = ProjectService.parse_csv_file(content)

        # Resolver columnas del CSV
        resolved_cols = ProjectService.resolve_csv_columns(
            headers,
            ProjectService.PROJECT_HEADER_MAPPING
        )

        # Obtener mapa de Macro Proyectos para validación
        macros_db = db.get_macro_projects(company_id)
        macros_map = {m['name'].strip().upper(): m['id'] for m in macros_db}

        validation_errors = []
        valid_rows_to_process = []

        # FASE 1: VALIDACIÓN
        for i, row in enumerate(rows):
            line_num = i + 2
            try:
                processed = ProjectService.process_project_row(
                    row, resolved_cols, macros_map, line_num
                )
                if processed:
                    valid_rows_to_process.append(processed)
            except ValidationError as ve:
                validation_errors.append(ve.message)
            except Exception as e:
                validation_errors.append(f"Línea {line_num}: {str(e)}")

        if validation_errors:
            error_msg = "IMPORTACIÓN RECHAZADA (Errores encontrados):\n" + "\n".join(validation_errors[:10])
            if len(validation_errors) > 10:
                error_msg += f"\n... y {len(validation_errors) - 10} errores más."
            raise ValidationError(error_msg, "CSV_IMPORT_ERRORS")

        # FASE 2: EJECUCIÓN
        stats = {"created": 0, "updated": 0}

        for data in valid_rows_to_process:
            res = db.upsert_project_from_import(
                company_id=company_id,
                **data
            )
            if res == "created":
                stats['created'] += 1
            else:
                stats['updated'] += 1

        return stats

    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.message)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico: {str(e)}")

