from etl import TransformOperations, DataSelect, ConvertOperations, BasicsTransformOperations, DB_Extractor, DB_Loader

def database_extract_connection():
    """Configura y retorna la conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }
    
    db_extract = DB_Extractor(**db_params)
    db_extract.connect()
    return db_extract

def database_loader_connection():
    """Configura y retorna la conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "carga_colombia"   
    }
    
    db_loader = DB_Extractor(**db_params)
    db_loader.connect()
    return db_loader

def extract_data(db_extract):
    """Extrae datos de las tablas necesarias"""
    print("Extrayendo datos de la tabla CITAS_GENERALES...")
    citas_generales = db_extract.get_table("citas_generales")
    BasicsTransformOperations.show_head(citas_generales, 5)
    
    print("Extrayendo datos de la tabla URGENCIAS...")
    urgencias = db_extract.get_table("urgencias")
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
    """Calcula el tiempo de espera entre solicitud y atención y lo convierte a segundos"""
    print("\nCalculando tiempo de espera...")
    
    # Crea la columna 'tiempo_espera' como un objeto Timedelta
    funcion3 = lambda row: row["fechahora_atencion"] - row["fechahora_solicitud"]
    df = BasicsTransformOperations.add_new_column(df, 'tiempo_espera', funcion3, 1)

    
    # Convierte el tiempo de espera a segundos totales (float)
    df['tiempo_espera_segundos'] = df['tiempo_espera'].dt.total_seconds()
    
    return df

def remove_unnecessary_columns(df):
    """Elimina columnas innecesarias"""
    print("\nEliminando columnas irrelevantes...")
    
    df_cleaned = DataSelect.select_columns(
        df, 
        'codigo_cita', 'id_usuario', 'id_medico', 'tiempo_espera_segundos',
        complement=False, 
        show=3
    )
    
    return df_cleaned

def calculate_average_wait_time(df):
    """Calcula el tiempo promedio de espera en segundos"""
    print("\nCalculando tiempo promedio de espera...")
    
    promedio_tiempo_espera = df['tiempo_espera_segundos'].mean()
    print(f"El promedio del tiempo de espera es: {promedio_tiempo_espera} segundos")
    
    return promedio_tiempo_espera

def load_dimension_table(loader, df, names):

    """Carga los datos en la tabla dimensional"""
    print("\nCargando dimensión: dim_citas_fechas3")
    loader.load_dimension(df, names)
    




def load_facts_table(loader, df):
    """Carga los datos en la tabla de hechos"""
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
        db_extractor = database_extract_connection()
        db_loader_carcol = database_loader_connection()
        loader_colombiasaludable = DB_Loader(engine=db_loader_carcol.engine)
        
        # Extraer datos
        citas_generales, urgencias = extract_data(db_extractor)
        
        # Filtrar citas de cirugía
        citas_cirugia = filter_cirugia_citas(citas_generales) #esto solo es para saber si hay ciru
        
        # Procesar fechas y horas
        citas_procesadas = process_datetime_columns(citas_generales.copy())
        cotizante = db_extractor.get_table('cotizante')
        beneficiario = db_extractor.get_table('beneficiario')
        cc_beneficiario = DataSelect.select_columns(beneficiario, 'cedula', 'nombre', 'sexo', show=1)
        cc_cotizante = DataSelect.select_columns(cotizante, 'cedula', 'nombre', 'sexo', show=1)
        personas = TransformOperations.union_all([cc_cotizante, cc_beneficiario], show=1)
        
        # Calcular tiempo de espera
        citas_con_tiempo = calculate_wait_time(citas_procesadas)
        
        # Limpiar columnas
        citas_limpias = remove_unnecessary_columns(citas_con_tiempo)
        print("citas limpias")
        print(citas_limpias)
        
        # Calcular promedio
        promedio = calculate_average_wait_time(citas_limpias)
        
        # Cargar datos
        load_dimension_table(loader_colombiasaludable, [citas_limpias, personas])
        
        # Opcional: Mostrar valores únicos (comentado)
        # print("\nValores únicos citas:")
        # DataSelect.unique_values(citas_generales, 'diagnostico', True)
        # print("\nValores únicos urgencias:")
        # DataSelect.unique_values(urgencias, 'diagnostico', True)
        
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
        raise
    finally:
        if 'db_extractor' in locals():
            db_extractor.close_connection()

if __name__ == "__main__":
    connection()