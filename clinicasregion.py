from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_transformer import BasicsTransformOperations
from etl.transformer.operaciones_transformer import TransformOperations
from etl.transformer.selecs import DataSelect
from etl.transformer.header import HeaderOperations
def connection():
    db_params = {
        "db_type": "postgresql",
        "user" : "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }

    btf = BasicsTransformOperations
    ds = DataSelect
    tf = TransformOperations
    hd = HeaderOperations

    db_loader = DB_Extractor(**db_params)

    try:
            # Conectar a la base de datos
            db_loader.connect()

            print("Extrayendo datos de la tabla CITAS_GENERALES...")
            query_citas_generales = "SELECT * FROM citas_generales"
            citas_generales = db_loader.execute_query(query_citas_generales)
            data = btf.show_head(citas_generales, 5)
            print(data)

            print("Extrayendo datos de la tabla URGENCIAS...")
            query_urgencias = "SELECT * FROM urgencias"
            urgencias = db_loader.execute_query(query_urgencias)
            data2 = btf.show_head(urgencias, 5)
            print(data2)

            print("Extrayendo datos de la tabla HOSPITALIZACIONES...")
            query_hospitalizaciones = "SELECT * FROM hospitalizaciones"
            hospitalizaciones = db_loader.execute_query(query_hospitalizaciones)
            data3 = btf.show_head(hospitalizaciones, 5)
            print(data3)

            print("Extrayendo datos de la tabla MEDICOS...")
            query_medico = "SELECT * FROM medico"
            medico = db_loader.execute_query(query_medico)
            data4 = btf.show_head(medico, 5)
            print(data4)

            print("Extrayendo datos de la tabla IPS...")
            query_ips = "SELECT * FROM ips"
            ips = db_loader.execute_query(query_ips)
            data5 = btf.show_head(ips, 5)
            print(data5)

            print("REALIZAMOS LEFT JOIN ENTRE CITAS, HOSPITALIZACION Y URGENCIAS CON MEDICO PARA OBTENER ID:IPS")
            #medico_renombre = hd.rename_columns(medico, {"cedula": "id_medico"}, show = 0)
            print("CITAS X MEDICO")
            unionM_C = tf.left_join( citas_generales, medico, on=("id_medico","cedula"), show=1)
            print("HOSPITALIZACION X MEDICO")
            unionM_H = tf.left_join( hospitalizaciones, medico, on=("id_medico","cedula"), show=1)
            print("URGENCIAS X MEDICO")
            unionM_U = tf.left_join( urgencias, medico, on=("id_medico","cedula"), show=0)


            print("\n ESCOGEMOS LO DATOS RELEVANTES")
            citas_ips = ds.select_columns(unionM_C, "id_ips", "codigo_cita", "id_medico", show = 1)
            hosp_ips = ds.select_columns(unionM_H, "id_ips", "codigo_hospitalizacion", "id_medico", show = 1)
            urg_ips = ds.select_columns(unionM_U, "id_ips", "codigo_urgencia", "id_medico", show = 1)


            print("REALIZAMOS LEFT JOIN ENTRE IPS Y NUEVA TABLA PARA OBTENER REGIONES")
            print("CITAS_MED X IPS")
            union_citasxmed_ips = tf.left_join( ips, citas_ips, on="id_ips", show=1)
            print("HOSPITALIZACION_MED X IPS")
            union_hospxmed_ips = tf.left_join( ips, hosp_ips, on="id_ips", show=1)
            print("URGENCIAS_MED X IPS")
            union_urgxmed_ips = tf.left_join( ips, urg_ips, on="id_ips", show=1)

            print("CREAMOS NUEVA COLUMNA TIPO Y RENOMBRAMOS LOS CAMPOS DE CODIGO UN NOMBRE UNICO")
            
            tipo1 = btf.add_new_column(union_citasxmed_ips, "tipo", lambda row: "general", 0)
            nuevotipo_citas = hd.rename_columns(tipo1, {"codigo_cita": "codigo"}, show = 1)
            tipo2 = btf.add_new_column(union_hospxmed_ips, "tipo", lambda row: "hospitalizacion", 0)
            nuevotipo_hosp = hd.rename_columns(tipo2, {"codigo_hospitalizacion": "codigo"}, show = 1)
            tipo3 = btf.add_new_column(union_urgxmed_ips, "tipo", lambda row: "urgencias", 0)
            nuevotipo_urg = hd.rename_columns(tipo3, {"codigo_urgencia": "codigo"}, show = 1)


            print("UNIMOS LAS 3 TABLAS Y VERIFICAMOS VALORES UNICOS EN TIPO PARA SABER SI SE UNIO BIEN")
            ipsXregion=tf.union_all([nuevotipo_citas, nuevotipo_hosp, nuevotipo_urg], show=2)
            pedidos_cliente = ds.unique_values(ipsXregion, 'tipo', True)

            print("LIMPIAMOS LA TABLA Y QUITAMOS VALORES NULOS")
            ipsXregion_limpia = ds.select_not_none(ipsXregion, 'codigo', show=2)
            ds.select_not_none(ipsXregion_limpia, 'codigo', complement=True, show=2)

            agr = tf.group_by_count(ipsXregion_limpia, ['id_ips','departamento', 'municipio'], show= 0)
            a = btf.sort_by_columns(agr, "departamento", ascending=True)
            print(btf.show_head(a, 5)) 


    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_loader.close_connection()

if __name__ == "__main__":
    connection()