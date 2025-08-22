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

def analyze_payments_data(pagos, cotizante):
    """Analiza los datos de pagos por diferentes categorías"""
    print("\n" + "="*50)
    print("ANÁLISIS DE PAGOS")
    print("="*50)
    
    # Unir pagos con cotizante
    print("UNIENDO PAGOS X COTIZANTE")
    unionP_C = TransformOperations.left_join(
        pagos, cotizante, on=("id_usuario", "cedula"), show=5
    )
    
    # Seleccionar columnas relevantes
    print("\nSELECCIONANDO DATOS RELEVANTES")
    citas_ips = DataSelect.select_columns(
        unionP_C, 
        "id_usuario", "sexo", "nivel_escolaridad", 
        "valor_pagado", "estracto", "fecha_nacimiento", 
        show=10
    )
    
    return citas_ips

def calculate_averages(data):
    """Calcula promedios para diferentes categorías"""
    resultados = {}
    
    # Columnas por las cuales agrupar
    columnas_agrupacion = ['estracto', 'nivel_escolaridad', 'sexo']
    
    for columna in columnas_agrupacion:
        print(f"\n📊 CALCULANDO PROMEDIO POR {columna.upper()}")
        try:
            resultado = TransformOperations.group_by_mean(
                data, columna, 'valor_pagado', 5
            )
            resultados[columna] = resultado
            print(f"✅ Promedio por {columna} calculado exitosamente")
        except Exception as e:
            print(f"❌ Error calculando promedio por {columna}: {e}")
    
    return resultados

def display_results(resultados):
    """Muestra los resultados de forma organizada"""
    print("\n" + "="*60)
    print("RESULTADOS DEL ANÁLISIS")
    print("="*60)
    
    for categoria, dataframe in resultados.items():
        print(f"\n📍 PROMEDIO DE VALOR PAGADO POR {categoria.upper()}:")
        print(BasicsTransformOperations.show_head(dataframe, 0))
        print("-" * 40)

def connection():
    """Función principal"""
    # Inicializar operaciones
    data_transformer = BasicsTransformOperations
    data_select = DataSelect
    advances_transform = TransformOperations
    
    # Crear conexión
    db_connected = create_db_connection()

    try:
        # Conectar a la base de datos
        db_connected.connect()

        # Tablas a extraer
        tablas_a_extraer = ["pagos", "cotizante", "hospitalizaciones"]
        
        # Extraer tablas usando bucle
        datos_extraidos = extract_tables(db_connected, tablas_a_extraer)
        
        # Obtener datos específicos
        pagos = datos_extraidos["pagos"]
        cotizante = datos_extraidos["cotizante"]
        hospitalizaciones = datos_extraidos["hospitalizaciones"]
        
        # Analizar datos de pagos
        datos_analisis = analyze_payments_data(pagos, cotizante)
        
        # Calcular promedios usando bucle
        resultados = calculate_averages(datos_analisis)
        
        # Mostrar resultados
        display_results(resultados)
        
        print("\n✅ ANÁLISIS COMPLETADO EXITOSAMENTE")
        
        return resultados

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        db_connected.close_connection()

if __name__ == "__main__":
    # Ejecutar análisis
    resultados_finales = connection()
    
    # Los resultados están disponibles para usar después
    if resultados_finales:
        print("\n🎯 Análisis completado. Resultados disponibles para reportes.")