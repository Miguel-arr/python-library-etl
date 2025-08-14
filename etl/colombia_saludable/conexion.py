from ..extractors.db_extractor import DB_Extractor

def connection():
    db_params = {
        "db_type": "postgresql",
        "user" : "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }


    db_loader = DB_Extractor(**db_params)

    try:
            # Conectar a la base de datos
            db_loader.connect()

            print("Extrayendo datos de la tabla cliente...")
            query_citas_generales = "SELECT * FROM citas_generales"
            citas_generales = db_loader.execute_query(query_citas_generales)

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        db_loader.close_connection()

if __name__ == "__main__":
    connection()