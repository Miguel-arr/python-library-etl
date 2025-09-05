from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_data_transformer import BasicsTransformOperations
from etl.loaders.db_loader import DB_Loader
from etl.transformer.convert import ConvertOperations
from etl.transformer.selecs import DataSelect

# Objetos globales de transformación
basic_transform = BasicsTransformOperations
data_select = DataSelect
data_convert = ConvertOperations

def create_db_connection():
    """Crea y retorna una conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }
    return DB_Extractor(**db_params)


def extract_tables(db_loader, tablas):
    """Extrae las tablas de la base de datos y muestra sus cabeceras"""
    
    resultados = []
    
    for tabla in tablas:
        print(f"Extrayendo datos de la tabla {tabla.upper()}...")
        datos = db_loader.get_table(tabla)
        basic_transform.show_head(datos, 5)
        resultados.append(datos)
    
    return tuple(resultados)