from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_data_transformer import BasicsTransformOperations
from etl.transformer.advanced_data_transforms import TransformOperations
from etl.transformer.selecs import DataSelect
from etl.transformer.header import HeaderOperations
from etl.transformer.fecha import DateTime

def create_db_connection():
    """Crea y retorna la conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }
    return DB_Extractor(**db_params)

def extract_tables(db_connected, table_names):
    """Extrae múltiples tablas usando un bucle"""
    tables_data = {}
    
    for table_name in table_names:
        print(f"Extrayendo datos de la tabla {table_name.upper()}...")
        try:
            data = db_connected.get_table(table_name)
            tables_data[table_name] = data
            print(f"✅ {table_name.upper()} extraída exitosamente")
            BasicsTransformOperations.show_head(data, 3)
            
        except Exception as e:
            print(f"❌ Error extrayendo {table_name}: {e}")
            tables_data[table_name] = None
            
    return tables_data


