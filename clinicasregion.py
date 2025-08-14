from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_data_transformer import BasicsTransformOperations
from etl.transformer.advanced_data_transforms import TransformOperations
from etl.transformer.selecs import DataSelect
from etl.transformer.header import HeaderOperations

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

def extract_tables(db_loader):
    """Extrae las tablas de la base de datos y muestra sus cabeceras"""
    tablas = ["citas_generales", "urgencias", "hospitalizaciones", "medico", "ips"]
    resultados = []
    
    for tabla in tablas:
        print(f"Extrayendo datos de la tabla {tabla.upper()}...")
        datos = db_loader.get_table(tabla)
        data_transformer.show_head(datos, 5)
        resultados.append(datos)
    
    return tuple(resultados)

def transform_and_merge_data(citas, urg, hosp, medico, ips):
    """
    Realiza todas las transformaciones y uniones desde el join con médico hasta la limpieza final
    Retorna la tabla limpia y consolidada
    """
    # 1. Join con tabla médico
    tablas = [citas, hosp, urg]
    resultados = []

    for tabla in tablas:
        print(f"\nREALIZAMOS LEFT JOIN ENTRE {tabla.upper()} CON MEDICO")
        datos = advances_transform.left_join(tabla, medico, on=("id_medico","cedula"), show=1)
        data_transformer.show_head(datos, 5)
        resultados.append(datos)






    print("\nREALIZAMOS LEFT JOIN ENTRE CITAS, HOSPITALIZACION Y URGENCIAS CON MEDICO")
    print("CITAS X MEDICO")
    unionM_C = advances_transform.left_join(citas, medico, on=("id_medico","cedula"), show=1)
    print("HOSPITALIZACION X MEDICO")
    unionM_H = advances_transform.left_join(hosp, medico, on=("id_medico","cedula"), show=1)
    print("URGENCIAS X MEDICO")
    unionM_U = advances_transform.left_join(urg, medico, on=("id_medico","cedula"), show=0)

    # 2. Selección de columnas relevantes
    print("\nESCOGEMOS LOS DATOS RELEVANTES")
    citas_ips = data_select.select_columns(unionM_C, "id_ips", "codigo_cita", "id_medico", show=1)
    hosp_ips = data_select.select_columns(unionM_H, "id_ips", "codigo_hospitalizacion", "id_medico", show=1)
    urg_ips = data_select.select_columns(unionM_U, "id_ips", "codigo_urgencia", "id_medico", show=1)

    # 3. Join con tabla IPS
    print("\nREALIZAMOS LEFT JOIN ENTRE IPS Y LAS NUEVAS TABLAS")
    print("CITAS_MED X IPS")
    union_citasxmed_ips = advances_transform.left_join(ips, citas_ips, on="id_ips", show=1)
    print("HOSPITALIZACION_MED X IPS")
    union_hospxmed_ips = advances_transform.left_join(ips, hosp_ips, on="id_ips", show=1)
    print("URGENCIAS_MED X IPS")
    union_urgxmed_ips = advances_transform.left_join(ips, urg_ips, on="id_ips", show=1)

    # 4. Transformación de tablas (añadir tipo y renombrar)
    print("\nCREAMOS COLUMNA 'TIPO' Y ESTANDARIZAMOS NOMBRES DE COLUMNAS")
    tipo1 = data_transformer.add_new_column(union_citasxmed_ips, "tipo", lambda row: "general", 0)
    nuevotipo_citas = header_operations.rename_columns(tipo1, {"codigo_cita": "codigo"}, show=1)
    
    tipo2 = data_transformer.add_new_column(union_hospxmed_ips, "tipo", lambda row: "hospitalizacion", 0)
    nuevotipo_hosp = header_operations.rename_columns(tipo2, {"codigo_hospitalizacion": "codigo"}, show=1)
    
    tipo3 = data_transformer.add_new_column(union_urgxmed_ips, "tipo", lambda row: "urgencias", 0)
    nuevotipo_urg = header_operations.rename_columns(tipo3, {"codigo_urgencia": "codigo"}, show=1)

    # 5. Unión y limpieza de tablas
    print("\nUNIMOS LAS 3 TABLAS Y LIMPIAMOS DATOS")
    ipsXregion = advances_transform.union_all([nuevotipo_citas, nuevotipo_hosp, nuevotipo_urg], show=2)
    data_select.unique_values(ipsXregion, 'tipo', True)

    ipsXregion_limpia = data_select.select_not_none(ipsXregion, 'codigo', show=2)
    data_select.select_not_none(ipsXregion_limpia, 'codigo', complement=True, show=2)
    
    return ipsXregion_limpia

def analyze_data(ipsXregion_limpia):
    """Realiza análisis final de los datos"""
    agr = advances_transform.group_by_count(ipsXregion_limpia, ['id_ips','departamento', 'municipio'], show=0)
    sorted_data = data_transformer.sort_by_columns(agr, "departamento", ascending=True)
    print(data_transformer.show_head(sorted_data, 5))

def connection():
    """Función principal que orquesta todo el proceso ETL"""
    db_loader = create_db_connection()
    
    try:
        db_loader.connect()
        
        # Extracción (ahora con el bucle for mejorado)
        citas, urg, hosp, medico, ips = extract_tables(db_loader)

        # Transformación y unión completa
        #ipsXregion_limpia = transform_and_merge_data(citas, urg, hosp, medico, ips)
        
        # Análisis final
        
    
        #analyze_data(ipsXregion_limpia)
        
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_loader.close_connection()

if __name__ == "__main__":
    connection()