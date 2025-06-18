from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.basics_transformer import BasicsTransformOperations
from etl.transformer.operaciones_transformer import TransformOperations
from etl.transformer.selecs import DataSelect
from etl.transformer.header import HeaderOperations
from etl.transformer.fecha import DateTime


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
    tm = DateTime
    date_dim = tm(2020, 2025)

    db_loader = DB_Extractor(**db_params)

    try:
            # Conectar a la base de datos
            db_loader.connect()

            print("Extrayendo datos de la tabla PAGOS...")
            pagos = db_loader.get_table("pagos")
            print(btf.show_head(pagos, 5))

            print("Extrayendo datos de la tabla URGENCIAS...")

            cotizante = db_loader.get_table("cotizante")
            
            print(btf.show_head(cotizante, 5))

            print("Extrayendo datos de la tabla HOSPITALIZACIONES con QUERY...")
            query_hospitalizaciones = "SELECT * FROM hospitalizaciones"
            hospitalizaciones = db_loader.execute_query(query_hospitalizaciones)
            print(btf.show_head(hospitalizaciones, 5))
            

            print("PAGOS X COTIZANTE")
            unionP_C = tf.left_join( pagos, cotizante, on=("id_usuario","cedula"), show=1)
            
            print("\n ESCOGEMOS LO DATOS RELEVANTES")
            citas_ips = ds.select_columns(unionP_C, "id_usuario", "sexo", "nivel_escolaridad","valor_pagado", "estracto", "fecha_nacimiento", show = 2)


            df_agrupado_X_estrato = TransformOperations.group_by_sum(citas_ips, by='estracto', column='valor_pagado', show=5)
            
            df_agrupado_X_escolaridad = TransformOperations.group_by_sum(citas_ips, by='nivel_escolaridad', column='valor_pagado', show=5)


            

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_loader.close_connection()

if __name__ == "__main__":
    connection()