from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_data_transformer import BasicsTransformOperations
from etl.loaders.db_loader import DB_Loader
from etl.transformer.convert import ConvertOperations
from etl.transformer.selecs import DataSelect

def setup_database_connection():
    """Configura y retorna la conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }
    
    db_loader = DB_Extractor(**db_params)
    db_loader.connect()
    return db_loader

def extract_data(db_loader):
    """Extrae datos de las tablas necesarias"""
    print("Extrayendo datos de la tabla CITAS_GENERALES...")
    citas_generales = db_loader.get_table("citas_generales")
    BasicsTransformOperations.show_head(citas_generales, 5)
    
    print("Extrayendo datos de la tabla URGENCIAS...")
    urgencias = db_loader.get_table("urgencias")
    BasicsTransformOperations.show_head(urgencias, 5)
    
    return citas_generales, urgencias

def filter_cirugia_citas(citas_generales):
    """Filtra las citas de cirugía"""
    print("\nFiltrando citas de cirugía...")
    citas_cirugia = DataSelect.filter_equal(
        citas_generales, 
        'diagnostico', 
        'cirugia', 
        show=5
    )
    return citas_cirugia

def process_datetime_columns(df):
    """Procesa y convierte columnas de fecha y hora"""
    print("\nProcesando columnas de fecha y hora...")
    
    # Limpiar formatos de fecha
    ConvertOperations.clean_date_format(df, "fecha_solicitud", show=0)
    ConvertOperations.clean_date_format(df, "fecha_atencion", show=0)
    
    # Crear columnas combinadas de fecha y hora
    funcion_lambda = lambda row: f"{row['fecha_solicitud']} {row['hora_solicitud']}"
    BasicsTransformOperations.add_new_column(df, 'fechahora_solicitud', funcion_lambda, 0)
    
    funcion_lambda2 = lambda row: f"{row['fecha_atencion']} {row['hora_atencion']}"
    BasicsTransformOperations.add_new_column(df, 'fechahora_atencion', funcion_lambda2, 0)
    
    # Convertir a datetime
    ConvertOperations.convert_column_type(df, ['fechahora_solicitud'], {'fechahora_solicitud': 'datetime'})
    ConvertOperations.convert_column_type(df, 'fechahora_atencion', 'datetime')
    
    return df

def calculate_wait_time(df):
    """Calcula el tiempo de espera entre solicitud y atención"""
    print("\nCalculando tiempo de espera...")
    
    funcion3 = lambda row: row["fechahora_atencion"] - row["fechahora_solicitud"]
    df = BasicsTransformOperations.add_new_column(df, 'tiempo_espera', funcion3, 1)
    
    return df

def remove_unnecessary_columns(df):
    """Elimina columnas innecesarias"""
    print("\nEliminando columnas irrelevantes...")
    
    df_cleaned = DataSelect.select_columns(
        df, 
        "fecha_solicitud", 
        "hora_solicitud", 
        complement=True, 
        show=3
    )
    
    return df_cleaned

def calculate_average_wait_time(df):
    """Calcula el tiempo promedio de espera"""
    print("\nCalculando tiempo promedio de espera...")
    
    promedio_tiempo_espera = df['tiempo_espera'].mean()
    print(f"El promedio del tiempo de espera es: {promedio_tiempo_espera}")
    
    return promedio_tiempo_espera

def load_dimension_table(loader, df):
    """Carga los datos en la tabla dimensional"""
    print("\nCargando dimensión: dim_citas_fechas3")
    loader.load_dimension(df, 
                          "dim_citas_fechas3")

def load_facts_table(loader, df):
    """Carga los datos en la tabla dimensional"""
    print("\nCargando dimensión: dim_citas_fechas3")
    loader.load_fact(df, "ventas", 
                        foreign_keys_map={
                            "id_producto": ("dim_producto", "producto_id"),
                            "id_cliente": ("dim_clientes", "cliente_id")
                        })



def connection():
    """Función principal que orquesta todo el proceso ETL"""
    try:
        # Configurar conexión
        db_loader = setup_database_connection()
        loader_colombiasaludable = DB_Loader(engine=db_loader.engine)
        
        # Extraer datos
        citas_generales, urgencias = extract_data(db_loader)
        
        # Filtrar citas de cirugía
        citas_cirugia = filter_cirugia_citas(citas_generales) #esto solo es para saber si hay ciru
        
        # Procesar fechas y horas
        citas_procesadas = process_datetime_columns(citas_generales.copy())
        
        # Calcular tiempo de espera
        citas_con_tiempo = calculate_wait_time(citas_procesadas)
        
        # Limpiar columnas
        citas_limpias = remove_unnecessary_columns(citas_con_tiempo)
        
        # Calcular promedio
        promedio = calculate_average_wait_time(citas_limpias)
        
        # Cargar datos
        load_dimension_table(loader_colombiasaludable, citas_limpias)
        
        # Opcional: Mostrar valores únicos (comentado)
        # print("\nValores únicos citas:")
        # DataSelect.unique_values(citas_generales, 'diagnostico', True)
        # print("\nValores únicos urgencias:")
        # DataSelect.unique_values(urgencias, 'diagnostico', True)
        
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
        raise
    finally:
        if 'db_loader' in locals():
            db_loader.close_connection()

if __name__ == "__main__":
    connection()