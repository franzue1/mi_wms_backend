# mi_wms_backend\reset_admin.py
import sys
import os
import getpass

# 1. Añadimos el directorio actual al path para poder importar 'app'
sys.path.append(os.getcwd())

try:
    # 2. Importamos las herramientas de TU proyecto
    from app.database.core import get_db_connection
    # Importamos hash_password del repo, que es el que usa SHA-256 (el que la BD espera)
    from app.database.repositories.security_repo import hash_password 
except ImportError as e:
    print("Error de importación. Asegúrate de ejecutar esto desde la carpeta raíz 'mi_wms_backend'")
    print(f"Detalle: {e}")
    sys.exit(1)

def reset_admin_password():
    print("--- ASISTENTE DE CAMBIO DE CLAVE ADMIN ---")
    
    # 3. Solicitar nueva contraseña
    new_pass = input("Ingresa la nueva contraseña para 'admin': ").strip()
    if not new_pass:
        print("La contraseña no puede estar vacía.")
        return

    # 4. Generar Hash (Usando tu algoritmo SHA-256 actual)
    hashed_pass = hash_password(new_pass)
    print(f"Hash generado (SHA-256): {hashed_pass[:10]}...")

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 5. Verificar si existe el usuario 'admin'
            cursor.execute("SELECT id FROM users WHERE username = 'admin'")
            res = cursor.fetchone()
            
            if not res:
                print("❌ Error: No se encontró el usuario 'admin' en la base de datos.")
                return
            
            user_id = res[0]

            # 6. Actualizar contraseña y quitar flag de cambio obligatorio
            cursor.execute("""
                UPDATE users 
                SET hashed_password = %s, 
                    must_change_password = FALSE,
                    is_active = 1
                WHERE id = %s
            """, (hashed_pass, user_id))
            
            conn.commit()
            print(f"✅ ¡Éxito! La contraseña de 'admin' (ID {user_id}) ha sido actualizada.")

    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ Error de Base de Datos: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    reset_admin_password()