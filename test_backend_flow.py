# test_backend_flow.py
import pytest
from httpx import AsyncClient, ASGITransport # <--- [NUEVO] Importar ASGITransport
from app.main import app
from app.database.core import get_db_connection

# Configuración
BASE_URL = "http://test" # Para pruebas internas con ASGITransport, la URL base puede ser ficticia
TEST_USER = "admin"
TEST_PASS = "admin" 

@pytest.mark.asyncio
async def test_full_operation_lifecycle():
    """
    Prueba de Integración: Ciclo de vida completo de una Salida (OUT).
    Creación -> Verificación BD -> Validación -> Verificación Stock
    """
    
    # [CORRECCIÓN] Usamos ASGITransport para pasar la app
    transport = ASGITransport(app=app)
    
    async with AsyncClient(transport=transport, base_url=BASE_URL) as ac:
        
        print("\n--- 1. AUTENTICACIÓN ---")
        login_res = await ac.post("/auth/token", data={"username": TEST_USER, "password": TEST_PASS})
        
        # Debug si falla login
        if login_res.status_code != 200:
            pytest.fail(f"Fallo al loguearse: {login_res.text}")
            
        token = login_res.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        
        # Datos maestros (Company ID 1 asumido)
        company_id = 1 
        

# [CORRECCIÓN] Usamos el endpoint de LISTADO (/products/) en lugar de BÚSQUEDA.
        # Pasamos limit=1 porque solo necesitamos un producto para el test.
        prod_res = await ac.get("/products/", params={"company_id": company_id, "limit": 1}, headers=headers)

        # --- DIAGNÓSTICO DE ERROR (Mantener esto igual) ---
        if prod_res.status_code != 200:
            pytest.fail(f"Fallo al buscar productos. Status: {prod_res.status_code}. Respuesta: {prod_res.text}")
        
        products = prod_res.json()
        
        # [NUEVO] Manejo de respuesta paginada vs lista plana
        # A veces los endpoints paginados devuelven { "items": [...], "total": ... }
        # O a veces devuelven directo [...]
        if isinstance(products, dict) and "items" in products:
            products = products["items"]
        
        if not isinstance(products, list):
                pytest.fail(f"El endpoint devolvió un objeto inesperado: {products}")

        if not products: 
            pytest.skip("No hay productos en la BD para probar (La lista está vacía)")
        
        target_product = products[0]
        product_id = target_product['id']
        print(f" -> Producto encontrado: ID {product_id} ({target_product.get('name')})")

        # ... (resto del código) ...
        
        # Buscamos tipo de operación OUT
        # Nota: Si falla, asegúrate de tener tipos de operación creados
        ops_res = await ac.get("/pickings/helpers/operation-types", params={"code": "OUT"}, headers=headers)
        op_types = ops_res.json()
        if not op_types: pytest.skip("No hay tipos de operación OUT configurados")
        op_type_name = op_types[0]['name'] 

        # Buscamos Picking Type ID (Un 'Picking Type' válido para la compañía)
        # Hacemos una consulta rápida a la API de helpers
        pt_res = await ac.get("/pickings/helpers/picking-types-summary", params={"company_id": company_id}, headers=headers)
        pts = pt_res.json()
        if not pts: pytest.skip("No hay Picking Types configurados")
        picking_type_id = pts[0]['id'] # Usamos el primero que encontremos
        
        print(f"--- 2. CREACIÓN TRANSACCIONAL (LAZY) ---")
        payload = {
            "company_id": company_id,
            "picking_type_id": picking_type_id,
            "responsible_user": TEST_USER,
            "custom_operation_type": op_type_name,
            "project_id": None, # Stock General
            "moves": [
                {
                    "product_id": product_id,
                    "quantity": 5.0,
                    "price_unit": 100.50
                }
            ]
        }
        
        create_res = await ac.post("/pickings/create-full", json=payload, headers=headers)
        if create_res.status_code != 201:
            pytest.fail(f"Error al crear: {create_res.text}")
            
        new_picking_id = create_res.json()["id"]
        print(f"✅ Albarán creado con ID: {new_picking_id}")

        print("--- 3. VALIDACIÓN DE BASE DE DATOS (Integridad) ---")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT state, project_id FROM pickings WHERE id = %s", (new_picking_id,))
        picking_row = cursor.fetchone()
        assert picking_row[0] == 'draft', "El estado debería ser 'draft'"
        assert picking_row[1] is None, "El proyecto debería ser NULL"
        
        # Verificar PRECIO (El punto crítico que arreglamos hoy)
        cursor.execute("SELECT quantity_done, price_unit FROM stock_moves WHERE picking_id = %s", (new_picking_id,))
        move_row = cursor.fetchone()
        assert float(move_row[0]) == 5.0, "Cantidad incorrecta"
        assert float(move_row[1]) == 100.50, "Precio incorrecto (Fallo de persistencia)"
        
        conn.close()
        print("✅ Datos en BD verificados correctamente.")

        print("--- 4. PRUEBA DE REGLAS DE NEGOCIO (Marcar como Listo) ---")
        # Intentamos marcar como listo
        ready_res = await ac.post(f"/pickings/{new_picking_id}/mark-ready", headers=headers)
        
        if ready_res.status_code == 200:
            print("✅ Marcado como listo exitosamente.")
        elif ready_res.status_code == 400:
            print(f"⚠️ Validación de negocio correcta (Stock insuficiente u otro): {ready_res.text}")
        else:
            pytest.fail(f"Error inesperado: {ready_res.status_code} - {ready_res.text}")

        # --- LIMPIEZA (Opcional: Borrar datos de prueba) ---
        # Descomentar si quieres que la BD quede limpia después del test
        # conn = get_db_connection()
        # cursor = conn.cursor()
        # cursor.execute("DELETE FROM stock_move_lines WHERE move_id IN (SELECT id FROM stock_moves WHERE picking_id = %s)", (new_picking_id,))
        # cursor.execute("DELETE FROM stock_moves WHERE picking_id = %s", (new_picking_id,))
        # cursor.execute("DELETE FROM pickings WHERE id = %s", (new_picking_id,))
        # conn.commit()
        # conn.close()