
from etl import DB_Extractor, TransformOperations, BasicsTransformOperations, DataSelect, ConvertOperations, DB_Loader
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
        tablas_a_extraer = ['formulas_medicas']
        
        # Extraer tablas usando bucle
        dato = extract_tables(db_connected, tablas_a_extraer)
        dfs = list(dato.values())

        df1 = ConvertOperations.split_string_column(df=dfs[0],
                                                    column='medicamentos_recetados', delimiter=";",
                                                    new_columns=["medicamento1", "medicamento2", "medicamento3", "medicamento4"],
                                                    show=5)
        
        return df1

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
