from etl import DB_Loader, DB_Extractor, TransformOperations, DataSelect, ConvertOperations, BasicsTransformOperations, DataExpresion, HeaderOperations
import pandas as pd

def create_db_connection():
    """Crea y retorna la conexión a la base de datos"""
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

def extract_tables(db_connected, table_names):
    """Extrae múltiples tablas usando un bucle"""
    tables_data = {}
    
    for table_name in table_names:
        print(f"Extrayendo datos de la tabla {table_name.upper()}...")
        try:
            data = db_connected.get_table(table_name)
            tables_data[table_name] = data
            print(f"✅ {table_name.upper()} extraída exitosamente")
            BasicsTransformOperations.show_head(data, 0)
            
        except Exception as e:
            print(f"❌ Error extrayendo {table_name}: {e}")
            tables_data[table_name] = None
            
    return tables_data

def calcular_costo_promedio_general(tablas):
    """
    ANÁLISIS 1: Costo promedio de atención a usuario (GENERAL)
    Calcula el costo promedio de todos los servicios para todos los usuarios
    """
    print("\n" + "="*60)
    print("ANÁLISIS 1: COSTO PROMEDIO DE ATENCIÓN A USUARIO (GENERAL)")
    print("="*60)
    
    # Unir remisiones con servicios_pos para obtener costos
    costo_servicio = TransformOperations.left_join(
        tablas["remisiones"],
        tablas["servicios_pos"],
        ("servicio_pos", "id_servicio_pos"),
        0
    )

    # Seleccionar solo las columnas necesarias
    df_costo = DataSelect.select_columns(
        costo_servicio, 
        'codigo_remision', 
        'id_usuario', 
        'servicio_pos', 
        'descripcion', 
        'costo',
        show=5
    )
    
    # Calcular estadísticas GENERALES (todos los servicios)
    promedio_costo = df_costo['costo'].mean()
    costo_total = df_costo['costo'].sum()
    cantidad_servicios = len(df_costo)
    
    print(f"\n📊 RESULTADOS GENERALES (TODOS LOS SERVICIOS):")
    print(f"  • Cantidad total de servicios: {cantidad_servicios:,}")
    print(f"  • Costo total general: ${costo_total:,.0f}")
    print(f"  • Costo promedio general: ${promedio_costo:,.0f}")
    
    return {
        'tipo_analisis': 'general',
        'df_costo': df_costo,
        'promedio_costo': promedio_costo,
        'total_servicios': cantidad_servicios,
        'costo_total': costo_total
    }

def calcular_costo_promedio_enfermedades(tablas):
    """
    ANÁLISIS 2: Costo promedio de atención de pacientes con enfermedades específicas
    (sida, insuficiencia renal, cáncer, diabetes, etc.)
    """
    print("\n" + "="*60)
    print("ANÁLISIS 2: COSTO PROMEDIO DE ATENCIÓN POR ENFERMEDAD ESPECÍFICA")
    print("="*60)
    
    # Unir remisiones con servicios_pos para obtener costos
    costo_servicio = TransformOperations.left_join(
        tablas["remisiones"],
        tablas["servicios_pos"],
        ("servicio_pos", "id_servicio_pos"),
        0
    )

    # Seleccionar columnas necesarias
    df_costo = DataSelect.select_columns(
        costo_servicio, 
        'codigo_remision', 
        'id_usuario', 
        'servicio_pos', 
        'descripcion', 
        'costo',
        show=5
    )
    
    # Lista de enfermedades a buscar (palabras clave)
    enfermedades = DataSelect.unique_values['sida', 'VIH', 'insuficiencia renal', 'renal', 'diabetes', 'cáncer', 
                   'cancer', 'hipertensión', 'cardíaco', 'cardiaco', 'hepatitis']
    
    print(f"\n🔍 Buscando servicios relacionados con enfermedades:")
    print(f"   Enfermedades a buscar: {', '.join(enfermedades)}")
    
    # Filtrar servicios que contengan alguna enfermedad en la descripción
    servicios_enfermedades = df_costo[
        df_costo['descripcion'].str.contains(
            '|'.join(enfermedades), 
            case=False, 
            na=False
        )
    ]
    
    if len(servicios_enfermedades) == 0:
        print("❌ No se encontraron servicios relacionados con enfermedades específicas")
        return None
    
    # Calcular por enfermedad específica
    print(f"\n📊 RESULTADOS POR ENFERMEDAD:")
    resultados_enfermedades = {}
    
    for enfermedad in enfermedades:
        # Filtrar servicios para esta enfermedad
        servicios_filtrados = df_costo[
            df_costo['descripcion'].str.contains(enfermedad, case=False, na=False)
        ]
        
        if len(servicios_filtrados) > 0:
            cantidad = len(servicios_filtrados)
            costo_total = servicios_filtrados['costo'].sum()
            promedio = servicios_filtrados['costo'].mean()
            
            resultados_enfermedades[enfermedad] = {
                'cantidad': cantidad,
                'costo_total': costo_total,
                'promedio': promedio
            }
            
            print(f"  • {enfermedad.upper()}:")
            print(f"    - Servicios: {cantidad:,}")
            print(f"    - Costo total: ${costo_total:,.0f}")
            print(f"    - Costo promedio: ${promedio:,.0f}")
    
    # Calcular estadísticas totales de servicios por enfermedades
    cantidad_total = len(servicios_enfermedades)
    costo_total_enfermedades = servicios_enfermedades['costo'].sum()
    promedio_total = servicios_enfermedades['costo'].mean()
    
    print(f"\n📈 RESUMEN TOTAL DE SERVICIOS POR ENFERMEDADES:")
    print(f"  • Servicios totales por enfermedades: {cantidad_total:,}")
    print(f"  • Costo total enfermedades: ${costo_total_enfermedades:,.0f}")
    print(f"  • Costo promedio enfermedades: ${promedio_total:,.0f}")
    
    # Comparar con el promedio general
    promedio_general = df_costo['costo'].mean()
    diferencia = promedio_total - promedio_general
    porcentaje = (diferencia / promedio_general) * 100
    
    print(f"\n📊 COMPARACIÓN CON EL PROMEDIO GENERAL:")
    print(f"  • Promedio enfermedades: ${promedio_total:,.0f}")
    print(f"  • Promedio general: ${promedio_general:,.0f}")
    print(f"  • Diferencia: ${diferencia:,.0f} ({porcentaje:+.1f}%)")
    
    return {
        'tipo_analisis': 'enfermedades_especificas',
        'df_enfermedades': servicios_enfermedades,
        'resultados_por_enfermedad': resultados_enfermedades,
        'promedio_total': promedio_total,
        'total_servicios': cantidad_total,
        'costo_total': costo_total_enfermedades,
        'comparacion_promedio_general': diferencia
    }

def transform(tablas, tipo_analisis='ambos'):
    """Transforma los datos según el tipo de análisis solicitado"""
    
    # Preparar datos de personas (para ambos análisis si es necesario)
    beneficiario = HeaderOperations.rename_columns(tablas["beneficiario"], {'id_beneficiario': 'cedula'}, show=0)
    cc_beneficiario = DataSelect.select_columns(beneficiario, 'cedula', 'nombre', 'sexo', show=0)
    cc_cotizante = DataSelect.select_columns(tablas["cotizante"], 'cedula', 'nombre', 'sexo', show=0)
    personas = TransformOperations.union_all([cc_cotizante, cc_beneficiario], show=0)
    
    personas = ConvertOperations.convert_column_type(
        personas, 
        columns=['cedula'],
        dtype={'cedula': 'string'},
        show=0
    )
    
    resultados = {}
    
    # Ejecutar los análisis solicitados
    if tipo_analisis in ['general', 'ambos']:
        resultados['general'] = calcular_costo_promedio_general(tablas)
    
    if tipo_analisis in ['enfermedades', 'ambos']:
        resultados['enfermedades'] = calcular_costo_promedio_enfermedades(tablas)
    
    # Agregar personas a resultados si se necesitan después
    resultados['personas'] = personas
    
    return resultados

def connection(tipo_analisis='ambos'):
    """Función principal - Controla qué análisis ejecutar"""
    print(f"\n🔧 INICIANDO ANÁLISIS: {tipo_analisis.upper()}")
    
    # Crear conexión
    db_connected = create_db_connection()
    db_loader = db_connection_loader()

    try:
        # Conectar a la base de datos
        db_connected.connect()
        db_loader.connect()
        loader_carga_colombia = DB_Loader(engine=db_loader.engine)

        # Tablas a extraer
        tablas_a_extraer = ["remisiones", "servicios_pos", "beneficiario", "cotizante"]
        
        # Extraer tablas usando bucle
        datos_extraidos = extract_tables(db_connected, tablas_a_extraer)
        
        # Transformar datos según el tipo de análisis
        resultados_transformacion = transform(datos_extraidos, tipo_analisis)
        
        # Mostrar resumen final
        print("\n" + "="*60)
        print("🎯 RESUMEN EJECUTIVO")
        print("="*60)
        
        if 'general' in resultados_transformacion:
            general = resultados_transformacion['general']
            print(f"📋 ANÁLISIS GENERAL COMPLETADO:")
            print(f"   • Costo promedio general: ${general['promedio_costo']:,.0f}")
            print(f"   • Total servicios analizados: {general['total_servicios']:,}")
        
        if 'enfermedades' in resultados_transformacion and resultados_transformacion['enfermedades']:
            enfermedades = resultados_transformacion['enfermedades']
            print(f"\n📋 ANÁLISIS POR ENFERMEDADES COMPLETADO:")
            print(f"   • Costo promedio enfermedades: ${enfermedades['promedio_total']:,.0f}")
            print(f"   • Servicios por enfermedades: {enfermedades['total_servicios']:,}")
        
        return resultados_transformacion

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        db_connected.close_connection()
        db_loader.close_connection()

if __name__ == "__main__":
    # OPCIÓN 1: Ejecutar solo análisis general
    # resultados = connection(tipo_analisis='general')
    
    # OPCIÓN 2: Ejecutar solo análisis por enfermedades
    # resultados = connection(tipo_analisis='enfermedades')
    
    # OPCIÓN 3: Ejecutar AMBOS análisis (por defecto)
    resultados = connection(tipo_analisis='ambos')
    
    # Los resultados están disponibles para usar después
    if resultados:
        print("\n🎯 Todos los análisis completados exitosamente.")
        print("📋 Resultados disponibles en la variable 'resultados'")