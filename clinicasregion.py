from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_data_transformer import BasicsTransformOperations
from etl.transformer.advanced_data_transforms import TransformOperations
from etl.transformer.selecs import DataSelect
from etl.transformer.header import HeaderOperations
from etl.loaders.db_loader import DB_Loader

# Objetos globales de transformación
data_transformer = BasicsTransformOperations
data_select = DataSelect
advances_transform = TransformOperations
header_operations = HeaderOperations

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
        print(data_transformer.show_head(datos, 0))
        resultados.append(datos)
    
    return tuple(resultados)
    
def transform_and_merge_data(citas, urg, hosp, medico, ips):

    # Diccionario con las tablas y sus columnas de código
    tablas = {
        "citas": (citas, "codigo_cita"),
        "hospitalizaciones": (hosp, "codigo_hospitalizacion"),
        "urgencias": (urg, "codigo_urgencia")
    }

    # --- PRIMER BUCLE: JOIN CON MÉDICO ---
    resultados_join_medico = []
    for nombre_tabla, (tabla, _) in tablas.items():
        print(f"\n>>> JOIN {nombre_tabla.upper()} CON MÉDICO")
        datos = advances_transform.left_join(tabla, medico, ("id_medico", "cedula"),1)
        data_transformer.show_head(datos, 0)
        resultados_join_medico.append((nombre_tabla, datos))

    # --- SEGUNDO BUCLE: SELECCIÓN DE COLUMNAS ---
    resultados_seleccion = []
    for nombre_tabla, datos in resultados_join_medico:
        codigo_col = tablas[nombre_tabla][1]  # Recuperamos la columna de código
        print(f"\n>>> SELECCIÓN EN {nombre_tabla.upper()}")
        datos_select = data_select.select_columns(datos, "id_ips", codigo_col, "id_medico", show=1
        )
        resultados_seleccion.append((nombre_tabla, datos_select))

    # --- TERCER BUCLE: JOIN CON IPS Y ESTANDARIZACIÓN ---
    resultados_finales = []
    for nombre_tabla, datos_select in resultados_seleccion:
        codigo_col = tablas[nombre_tabla][1]  # Columna de código original
        print(f"\n>>> JOIN {nombre_tabla.upper()} CON IPS")
        datos_left = advances_transform.left_join(ips, datos_select, "id_ips", 3)
        
        print("\n>>> ESTANDARIZANDO DATOS")#ponemos una columna tipo con el tipo de cita(urgencia, hospitalizacion, cita)
        datos_con_tipo = data_transformer.add_new_column(datos_left, "tipo", lambda row: nombre_tabla, 2
        )
        datos_estandarizados = header_operations.rename_columns(datos_con_tipo, {codigo_col: "codigo"}, show=1
        )
        resultados_finales.append(datos_estandarizados)

    # --- CONSOLIDACIÓN FINAL (Ejemplo: concatenar todas las tablas) ---
    
    ipsXregion = advances_transform.union_all([resultados_finales[0], resultados_finales[1], resultados_finales[2]], show=0)
    data_select.unique_values(ipsXregion, 'tipo', True)

    print("not none codigo")
    ipsXregion_limpia = data_select.select_not_none(ipsXregion, 'codigo', show=10)
    print('otra vez not none codigo')
    data_select.select_not_none(ipsXregion_limpia, 'codigo', True, 5)

    return ipsXregion_limpia

    

def analyze_data(ipsXregion_limpia):
    """Identifica los centros con más atenciones por región/ciudad"""
    # Paso 0: Seleccionamos las columnas relevantes para la carga de la tabla de hechos
    ipsXregion_Fact = data_select.select_columns(ipsXregion_limpia, 'id_ips', 'id_medico', 'departamento', 'municipio', 'tipo', show=2)
    # Paso 1: Agrupar por IPS, departamento y municipio, contando registros (o sumando 'conteo' si existe)
    atenciones_por_ips = advances_transform.group_by_count(
        ipsXregion_limpia, 
        ['id_ips', 'departamento', 'municipio',], 
        show=1
    )
    
    # Paso 2: Ordenar por departamento/municipio y conteo (descendente)
    #atenciones_ordenadas = advances_transform.sort_by(atenciones_por_ips, ['departamento', 'municipio', 'conteo'], ascending=[True, True, False],show=0)
    
    # Paso 3: Obtener el TOP 1 de IPS por municipio
    #top_ips_por_municipio = atenciones_ordenadas.groupby(['departamento', 'municipio']).first().reset_index()
        
    # Mostrar resultados
    print("\n--- TOP IPS POR MUNICIPIO ---")
    #print(data_transformer.show_head(top_ips_por_municipio, 0))
    
    return ipsXregion_Fact

    
def load_dimension_table(loader, dfs, table_names):
    """Carga los datos en la tabla dimensional"""
    print("\nCargando dimensión: dim_citas_fechas3")
    loader.load_dimension(dfs, table_names) 


def load_fact_table(loader, df, table_names):
    """Carga los datos en la tabla hechos"""
    foreign_keys={
                            "id_ips": ("dim_ips", "id_ips"),
                            "id_medico": ("dim_medico", "cedula"),
                            }
    
    print("\nCargando dimensión: dim_citas_fechas3")
    loader.load_fact(df, table_names, foreign_keys) 



def connection():
    """Función principal que orquesta todo el proceso ETL"""
    db_loader = create_db_connection()

    tablas = ["citas_generales", "urgencias", "hospitalizaciones", "medico", "ips", ]
    
    try:
        db_loader.connect()
        loader_colombiasaludable = DB_Loader(engine=db_loader.engine)
        
        # Extracción (ahora con el bucle for mejorado)
        citas, urg, hosp, medico, ips = extract_tables(db_loader, tablas)
        dimensiones = (citas, urg, hosp, medico, ips)
        nombres_dimensiones = ("dim_citas", "dim_urgencias", "dim_hospitalizaciones", "dim_medico", "dim_ips")
        # Transformación y unión completa
        ipsXregion_limpia = transform_and_merge_data(citas, urg, hosp, medico, ips)
        
        # Análisis final
        df_fact = analyze_data(ipsXregion_limpia)

        # Carga de dimensiones
        load_dimension_table(loader_colombiasaludable, dimensiones, nombres_dimensiones)
        print(advances_transform.head(df_fact))
        load_fact_table(loader_colombiasaludable, df_fact, "fact_ips_region")

        

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_loader.close_connection()

if __name__ == "__main__":
    connection()