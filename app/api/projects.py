# app/api/projects.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Annotated, Optional
from app import database as db
from app import schemas, security
from app.security import TokenData
import traceback
import asyncio

router = APIRouter()
AuthDependency = Annotated[TokenData, Depends(security.get_current_user_data)]

# --- GERENCIAS ---

@router.get("/management-units", response_model=List[schemas.ManagementProjectResponse])
async def get_management_units(auth: AuthDependency, company_id: int = Query(...)):
    try:
        data = await asyncio.to_thread(db.get_management_projects, company_id)
        return [dict(row) for row in data]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Error: {e}")

@router.post("/management-units", status_code=201)
async def create_management_unit(item: schemas.ManagementProjectCreate, auth: AuthDependency, company_id: int = Query(...)):
    try:
        res = await asyncio.to_thread(db.create_management_project, company_id, item.name, item.analytic_account, item.direction_type)
        return {"id": res['id'], "message": "Gerencia creada"}
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")

@router.put("/management-units/{item_id}")
async def update_management_unit(item_id: int, item: schemas.ManagementProjectCreate, auth: AuthDependency):
    try:
        await asyncio.to_thread(db.update_management_project, item_id, item.name, item.analytic_account, item.direction_type)
        return {"message": "Actualizado"}
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")

@router.delete("/management-units/{item_id}")
async def delete_management_unit(item_id: int, auth: AuthDependency):
    try:
        await asyncio.to_thread(db.delete_management_project, item_id)
        return {"message": "Eliminado"}
    except Exception as e:
        raise HTTPException(400, f"No se puede eliminar (probablemente tiene obras asociadas): {e}")

# --- OBRAS (PROJECTS) ---

@router.get("/", response_model=List[schemas.ProjectResponse])
async def get_projects(
    auth: AuthDependency, 
    company_id: int = Query(...), 
    mgmt_id: Optional[int] = None,
    status: Optional[str] = None
):
    try:
        data = await asyncio.to_thread(db.get_projects, company_id, mgmt_id, status)
        return [dict(row) for row in data]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Error: {e}")

@router.post("/", status_code=201)
async def create_project(item: schemas.ProjectCreate, auth: AuthDependency, company_id: int = Query(...)):
    try:
        res = await asyncio.to_thread(
            db.create_project, 
            company_id, item.management_project_id, item.name, item.code, 
            item.address, item.department, item.contractor_partner_id
        )
        return {"id": res['id'], "message": "Obra creada"}
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")

@router.put("/{project_id}")
async def update_project(project_id: int, item: schemas.ProjectCreate, auth: AuthDependency):
    try:
        await asyncio.to_thread(
            db.update_project, 
            project_id, item.management_project_id, item.name, item.code, 
            item.address, item.department, item.status, item.contractor_partner_id
        )
        return {"message": "Actualizado"}
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")

@router.delete("/{project_id}")
async def delete_project(project_id: int, auth: AuthDependency):
    try:
        await asyncio.to_thread(db.delete_project, project_id)
        return {"message": "Eliminado"}
    except Exception as e:
        raise HTTPException(500, f"Error: {e}")