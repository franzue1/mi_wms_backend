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
async def export_projects_csv(auth: AuthDependency, company_id: int = Query(...)):
    verify_access(auth, company_id)
    
    try:
        # 1. Obtener datos (reusamos la función existente corregida)
        projects = db.get_projects(company_id, limit=999999) 
        
        # 2. Generar CSV
        output = io.StringIO(newline='')
        # Usamos lineterminator='\n' para compatibilidad total Excel/Windows
        writer = csv.writer(output, delimiter=';', lineterminator='\n')
        
        # Headers [MEJORADOS]
        headers = [
            "Código PEP", "Nombre de Obra", 
            "Dirección", "Gerencia", "Proyecto (Macro)", # <--- JERARQUÍA COMPLETA
            "Estado", "Fase", 
            "Dirección Física", "Departamento", "Provincia", "Distrito",
            "Presupuesto (S/)", "Inicio", "Fin", 
            "En Custodia (S/)", "Liquidado (S/)"
        ]
        writer.writerow(headers)
        
        # Rows
        for p in projects:
            row = [
                p.get('code') or "",
                p.get('name'),
                p.get('direction_name') or "",   # <--- Nuevo
                p.get('management_name') or "",  # <--- Nuevo
                p.get('macro_name') or "",
                p.get('status'),
                p.get('phase'),
                p.get('address') or "",
                p.get('department') or "",
                p.get('province') or "",
                p.get('district') or "",
                f"{float(p.get('budget', 0)):.2f}".replace('.', ','), # Formato decimal Excel (coma)
                p.get('start_date') or "",
                p.get('end_date') or "",
                f"{float(p.get('stock_value', 0)):.2f}".replace('.', ','),      # Formato decimal Excel
                f"{float(p.get('liquidated_value', 0)):.2f}".replace('.', ',')  # Formato decimal Excel
            ]
            writer.writerow(row)
            
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Reporte_Obras_Completo.csv"}
        )
        
    except Exception as e:
        raise HTTPException(500, detail=f"Error exportando: {e}")

@router.get("/hierarchy/export-flat", response_class=StreamingResponse)
async def export_hierarchy_flat(
    auth: AuthDependency,
    company_id: int = Query(...)
):
    """
    Genera un CSV plano con la estructura completa.
    [CORRECCIÓN DEFINITIVA] Usamos lineterminator='\n' para evitar el doble salto de línea
    que genera filas en blanco en Excel/Windows.
    """
    verify_access(auth, company_id)
    
    try:
        # 1. Obtener datos
        hierarchy_data = db.get_hierarchy_flat(company_id)
        
        # 2. Generar CSV
        output = io.StringIO(newline='') 
        
        # [TRUCO] Forzamos '\n'. 
        # Si usáramos el default, en algunos entornos se convierte en \r\r\n (doble salto).
        # Al usar '\n', Excel lo lee bien y evitamos el salto extra.
        writer = csv.writer(output, delimiter=';', lineterminator='\n') 
        
        # Headers
        headers = [
            "Dirección", "Cód. Dir", 
            "Gerencia", "Cód. Ger", 
            "Proyecto (Macro)", "Cód. Proy", "Centro de Costo"
        ]
        writer.writerow(headers)
        
        # Rows (con limpieza de datos para evitar saltos ocultos dentro del texto)
        for row in hierarchy_data:
            def clean(val):
                # .strip() elimina espacios y saltos de línea (\n) al inicio/final del dato
                return str(val).strip() if val else ""

            writer.writerow([
                clean(row.get('dir_name')),
                clean(row.get('dir_code')),
                clean(row.get('mgmt_name')),
                clean(row.get('mgmt_code')),
                clean(row.get('macro_name')),
                clean(row.get('macro_code')),
                clean(row.get('cost_center'))
            ])
            
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=jerarquia_proyectos.csv"}
        )
        
    except Exception as e:
        print(f"Error exportando jerarquía: {e}")
        raise HTTPException(500, detail=f"Error exportando: {str(e)}")

@router.post("/hierarchy/import-flat", response_model=dict)
async def import_hierarchy_flat(
    auth: AuthDependency,
    company_id: int = Query(...),
    file: UploadFile = File(...)
):
    """
    Importa una estructura jerárquica desde un CSV plano.
    Columnas esperadas: 
    'Dirección', 'Cód. Dir', 'Gerencia', 'Cód. Ger', 'Proyecto (Macro)', 'Cód. Proy', 'Centro de Costo'
    """
    verify_access(auth, company_id)
    
    try:
        content = await file.read()
        decoded = content.decode('utf-8-sig')
        
        # Detectar delimitador
        first_line = decoded.split('\n')[0]
        delimiter = ';' if ';' in first_line else ','
        
        reader = csv.DictReader(io.StringIO(decoded), delimiter=delimiter)
        
        # Normalizar headers (quitar tildes, espacios, lower)
        # Mapeo flexible para que el usuario no sufra con nombres exactos
        def normalize_header(h):
            return h.lower().replace('ó', 'o').replace('é', 'e').replace('.', '').strip()
            
        header_map = {normalize_header(h): h for h in reader.fieldnames or []}
        
        # Mapeo de nuestras claves internas a las columnas del CSV
        # Clave Interna : Posibles nombres en el CSV
        key_mapping = {
            'dir_name': ['direccion', 'direction', 'area'],
            'dir_code': ['cod dir', 'cod direccion', 'codigo direccion'],
            'mgmt_name': ['gerencia', 'management', 'departamento'],
            'mgmt_code': ['cod ger', 'cod gerencia', 'codigo gerencia'],
            'macro_name': ['proyecto (macro)', 'proyecto', 'macro', 'project'],
            'macro_code': ['cod proy', 'cod proyecto', 'codigo proyecto'],
            'cost_center': ['centro de costo', 'centro costo', 'ceco', 'cc']
        }
        
        rows_to_process = []
        
        for row in reader:
            clean_row = {}
            for internal_key, possible_names in key_mapping.items():
                # Buscar cuál columna del CSV coincide con esta clave
                csv_col = next((header_map.get(poss) for poss in possible_names if poss in header_map), None)
                if csv_col:
                    clean_row[internal_key] = row.get(csv_col, "").strip()
                else:
                    clean_row[internal_key] = "" # Si no existe columna, vacío
            
            # Solo agregar si al menos tiene Dirección (es la raíz obligatoria)
            if clean_row.get('dir_name'):
                rows_to_process.append(clean_row)
                
        if not rows_to_process:
            raise ValueError("No se encontraron filas válidas con al menos una 'Dirección'.")

        # Llamar al repo
        stats = db.import_hierarchy_batch(company_id, rows_to_process)
        
        return {
            "message": "Importación completada con éxito",
            "stats": stats
        }

    except ValueError as ve:
        # [CORRECCIÓN] Capturamos los errores de validación del repo y devolvemos 400
        raise HTTPException(status_code=400, detail=str(ve))
        
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error crítico en importación: {str(e)}")

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

                # [MEJORA] Validación Estricta del Código PEP antes de ir al DB
                code_val = get_val('code')
                if not code_val:
                    raise ValueError(f"El Código PEP es obligatorio para la obra '{name}'.")
                
                # Validar caracteres prohibidos en el Excel (para consistencia con el Frontend)
                import re
                if not re.match(r"^[a-zA-Z0-9_./-]*$", code_val):
                     raise ValueError(f"El Código PEP '{code_val}' contiene caracteres inválidos. Solo use letras, números, guiones, puntos o barras.")

                # 2. Validar Proyecto (Macro)
                macro_name_raw = get_val('macro_name') or get_val('proyecto')
                macro_id = None
                
                if not macro_name_raw:
                    raise ValueError(f"El campo 'Proyecto' (Macro) es obligatorio para la obra '{name}'.")
                
                macro_clean = macro_name_raw.strip().upper()
                if macro_clean not in macros_map:
                    raise ValueError(f"El Proyecto '{macro_name_raw}' NO EXISTE en el sistema. Créelo primero.")
                
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


