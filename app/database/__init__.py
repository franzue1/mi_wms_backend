# 1. Importar la infraestructura (Core)
# Esto hace que 'app.database.init_db_pool' funcione
from .core import (
    init_db_pool,
    execute_query,
    execute_commit_query,
    db_pool,
    get_db_connection,
    return_db_connection
)

# 2. Importar Schema (para inicialización)
from .schema import create_schema, create_initial_data

# 3. Importar todos los Repositorios
# Esto hace que las funciones de negocio estén disponibles
from .repositories.security_repo import *
from .repositories.product_repo import *
from .repositories.partner_repo import *
from .repositories.warehouse_repo import *
from .repositories.operation_repo import *
from .repositories.work_order_repo import *
from .repositories.report_repo import *
from .repositories.project_repo import *