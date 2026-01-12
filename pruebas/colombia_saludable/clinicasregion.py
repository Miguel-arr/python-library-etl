from etl import  ConvertOperations, DB_Extractor, TransformOperations, BasicsTransformOperations, DataSelect, HeaderOperations, DB_Loader

# Objetos globales de transformación
data_transformer = BasicsTransformOperations
data_select = DataSelect
advances_transform = TransformOperations
header_operations = HeaderOperations

def db_connection_extract():
    """Crea y retorna una conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }
    return DB_Extractor(**db_params)

def db_connection_loader():
    """Crea y retorna una conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "carga_colombia"   
    }
    return DB_Extractor(**db_params)





def extract_tables(db_loader, tablas):
    """Extrae las tablas de la base de datos y muestra sus cabeceras"""
    
    resultados = []
    
    for tabla in tablas:
        print(f"Extrayendo datos de la tabla {tabla.upper()}...")
        datos = db_loader.get_table(tabla)
        print(data_transformer.show_head(datos, 2))
        resultados.append(datos)
    
    return tuple(resultados)




    

def transform_and_merge_data(citas, urg, hosp, medico, ips, empresa):

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
        datos_select = data_select.select_columns(datos, "id_ips", codigo_col, "id_medico", "id_usuario", show=1
        )
        resultados_seleccion.append((nombre_tabla, datos_select))

    # --- TERCER BUCLE: JOIN CON  USUARIO ---
    resultados_join_usuario = []
    for nombre_tabla, datos_select in resultados_seleccion:
        print(f"\n>>> JOIN {nombre_tabla.upper()} CON USUARIO")
        # Asumiendo que la columna de join es "id_usuario" en ambas tablas
        datos_con_usuario = advances_transform.left_join(datos_select, empresa, ("id_usuario","cotizante"), 1)
        
        data_transformer.show_head(datos_con_usuario, 1)
        resultados_join_usuario.append((nombre_tabla, datos_con_usuario))   

    # --- TERCER BUCLE: JOIN CON IPS Y ESTANDARIZACIÓN ---
    resultados_finales = []
    for nombre_tabla, datos_select in resultados_join_usuario:
        codigo_col = tablas[nombre_tabla][1]  # Columna de código original
        print(f"\n>>> JOIN {nombre_tabla.upper()} CON IPS")
        datos_left = advances_transform.left_join(ips, datos_select, "id_ips", 1)
        
        print("\n>>> ESTANDARIZANDO DATOS")#ponemos una columna tipo con el tipo de cita(urgencia, hospitalizacion, cita)
        datos_con_tipo = data_transformer.add_new_column(datos_left, "tipo", lambda row: nombre_tabla, 1)
        datos_estandarizados = header_operations.rename_columns(datos_con_tipo, {codigo_col: "codigo"}, show=1)
        resultados_finales.append(datos_estandarizados)

    # --- CONSOLIDACIÓN FINAL (Ejemplo: concatenar todas las tablas) ---
    
    ipsXregion = advances_transform.union_all([resultados_finales[0], resultados_finales[1], resultados_finales[2]], show=1)
    print("\n>>> COLUMNAS DISPONIBLES TRAS UNION ALL")
    ipsXregion2 = data_select.select_columns(ipsXregion, 'id_ips', 'codigo', 'id_medico', 'id_usuario','empresa', 'departamento', 'municipio', 'tipo',  show=3) 
      
   

    print("\n>>> DATOS UNION ALL")
    ipsXregion3 = data_select.unique_values(ipsXregion2, 'tipo', True)

    print("not none codigo")
    ipsXregion_limpia = data_select.select_not_none(ipsXregion2, 'codigo', show=0)
    ipsXregion_limpia = data_select.select_not_none(ipsXregion2, 'empresa', show=0)
    print('otra vez not none codigo')
    data_select.select_not_none(ipsXregion_limpia, 'codigo', True, 0)
    data_select.select_not_none(ipsXregion_limpia, 'empresa', True, 0)

    return ipsXregion_limpia

    





def analyze_data(ipsXregion_limpia):
    """Identifica los centros con más atenciones por región/ciudad"""
    print("Paso 1: Seleccionamos las columnas relevantes para la carga de la tabla de hechos")
    ipsXregion_Fact = data_select.select_columns(ipsXregion_limpia, 'id_ips', 'id_medico','id_usuario','empresa', 'departamento', 'municipio', 'tipo', show=2)


    print("Paso 2: Agrupar por IPS, departamento y municipio, contando registros (o sumando 'conteo' si existe)")
    atenciones_por_departamento = advances_transform.group_by_count(
        ipsXregion_limpia, 
        ['departamento','id_ips'], 
        show=0
    )
    
    atenciones_por_municipio = advances_transform.group_by_count(
        ipsXregion_limpia, 
        ['municipio','id_ips'], 
        show=0
    )

    print("Paso 3: Ordenar por departamento/municipio y conteo (descendente)")
    atenciones_por_departamento = advances_transform.sort_by(atenciones_por_departamento, ['departamento', 'conteo'],show=5)
    atenciones_por_municipio = advances_transform.sort_by(atenciones_por_municipio, ['municipio', 'conteo'], show=0)


    # Mostrar resultados
    print("\n--- TOP IPS POR DEPARTAMENTO ---")
    top_departamento = atenciones_por_departamento.groupby(['departamento']).first().reset_index()
    top_departamento = advances_transform.sort_by(top_departamento, ['conteo'], ascending=[False],show=9)

    print("\n--- TOP IPS POR MUNICIPIO ---")
    top_municipio = atenciones_por_municipio.groupby(['municipio']).first().reset_index()
    top_municipio = advances_transform.sort_by(top_municipio, ['conteo'], ascending=[False],show=9)

    return top_departamento

    



def load_dimension_table(loader, dfs, table_names):
    """Carga los datos en la tabla dimensional"""
    print("\nCargando dimensión: dim_citas_fecha")
    loader.load_dimension(dfs, table_names) 


def load_fact_table(loader, df, table_names):
    """Carga los datos en la tabla hechos"""
    foreign_keys={
                            "id_ips": ("dim_ips", "id_ips"),
                            }
    
    print("\nCargando hechos: fact_ips_region")
    loader.load_fact(df, table_names, foreign_keys)



def connection():
    """Función principal que orquesta todo el proceso ETL"""
    db_extractor = db_connection_extract()
    db_loader = db_connection_loader()

    tablas = ["citas_generales", "urgencias", "hospitalizaciones", "medico", "ips", "empresa_cotizante"]
    
    try:
        db_extractor.connect()
        db_loader.connect()
        loader_cargacolombia = DB_Loader(engine=db_loader.engine)
        
        # Extracción (ahora con el bucle for mejorado)
        citas, urg, hosp, medico, ips, empresa = extract_tables(db_extractor, tablas)

        medico = ConvertOperations.convert_column_type(
                medico, 
                columns=['cedula'],
                dtype={'cedula': 'string'},
                show=0  # Mostrar todo el dataframe
            )
        
        dimensiones = (citas, urg, hosp, medico, ips)
        nombres_dimensiones = ("dim_citas", "dim_urgencias", "dim_hospitalizaciones", "dim_medico", "dim_ips")
        # Transformación y unión completa
        ipsXregion_limpia = transform_and_merge_data(citas, urg, hosp, medico, ips, empresa)
        
        # Análisis final
        df_fact = analyze_data(ipsXregion_limpia)

        # Carga de dimensiones
        load_dimension_table(loader_cargacolombia, dimensiones, nombres_dimensiones)
        load_fact_table(loader_cargacolombia, df_fact, "fact_ips_region")

        

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_extractor.close_connection()
        db_loader.close_connection()

if __name__ == "__main__":
    connection()