from etl.extractors.db_extractor import DB_Extractor
from etl.transformer.advanced_data_transforms import TransformOperations
from etl.transformer.selecs import DataSelect
from etl.transformer.convert import ConvertOperations
from etl.loaders.db_loader import DB_Loader
from etl.transformer.header import HeaderOperations


def test_petl_search():
    db_params_postgres = {
        "db_type": "postgresql",
        "user" : "postgres",
        "password": "admin",           
        "database": "colombia_saludable"      
    }

    db_params_oracle = {
        "db_type": "oracle",
        "user" : "USER_MIGUEL",
        "password": "Miguel99",           
        "database": "ventas",
        "service_name": "xe"     
    }

    db_params_mysql = {
        "db_type": "mysql",
        "password": "admin",           
        "database": "ventas",   
    }
    
    
    db_loader_postgres = DB_Extractor(**db_params_postgres)
    db_loader_oracle = DB_Extractor(**db_params_oracle)
    db_loader_mysql = DB_Extractor(**db_params_mysql)



    try:
        # Conectar a la base de datos
        db_loader_postgres.connect()
        db_loader_oracle.connect()
        db_loader_mysql.connect()
        loader_oracle = DB_Loader(engine=db_loader_oracle.engine)
        loader_mysql = DB_Loader(engine=db_loader_mysql.engine)

        # Extraer los datos de las tablas
        print("Extrayendo datos de la tabla cliente...")
        formulas = db_loader_postgres.get_table('formulas_medicas')
        medico = db_loader_postgres.get_table('medico')
        cotizante = db_loader_postgres.get_table('cotizante')
        beneficiario = db_loader_postgres.get_table('beneficiario')
        print("hola")
        beneficiario = HeaderOperations.rename_columns(beneficiario, {'id_beneficiario' : 'cedula'}, show =1)
        
        cc_beneficiario = DataSelect.select_columns(beneficiario, 'cedula', 'nombre', 'sexo', show=1)
        cc_cotizante = DataSelect.select_columns(cotizante, 'cedula', 'nombre', 'sexo', show=1)
        personas = TransformOperations.union_all([cc_cotizante, cc_beneficiario], show=1)

        DataSelect.unique_values(formulas, "medicamentos_recetados", show=5)
        print("hola")
        df1 = ConvertOperations.split_string_column(df=formulas, column='medicamentos_recetados', delimiter=";", new_columns=["medicamento1", "medicamento2", "medicamento3", "medicamento4"],show=0)
        

        medico2 = ConvertOperations.convert_column_type(
                medico, 
                columns=['cedula'],
                dtype={'cedula': 'int'},
                show=0  # Mostrar todo el dataframe
            )
        personas2 = ConvertOperations.convert_column_type(
                personas, 
                columns=['cedula'],
                dtype={'cedula': 'int'},
                show=0  # Mostrar todo el dataframe
 )
        

        df2 = ConvertOperations.convert_column_type(
                df1, 
    columns=[
        'codigo_formula', 
        'id_medico', 
        'id_usuario', 
        'fecha', 
        'medicamentos_recetados', 
        'medicamento1', 
        'medicamento2', 
        'medicamento3', 
        'medicamento4'
    ],
    dtype={
        'codigo_formula': 'string',
        'id_medico': 'int',
        'id_usuario': 'int',
        'fecha': 'datetime',
        'medicamentos_recetados': 'string',
        'medicamento1': 'string',
        'medicamento2': 'string',
        'medicamento3': 'string',
        'medicamento4': 'string'
    },
    show=0  # Mostrar todo el dataframe
)
        ConvertOperations.fill_nulls(df2, 'medicamento2', '0')
        ConvertOperations.fill_nulls(df2, 'medicamento3', '0')
        ConvertOperations.fill_nulls(df2, 'medicamento4', '0', 1)

        DataSelect.unique_values(df2, 'medicamento1', 5)
        DataSelect.unique_values(df2, 'medicamento2', 5)
        DataSelect.unique_values(df2, 'medicamento3', 5)
        DataSelect.unique_values(df2, 'medicamento4', 5)
       

        loader_mysql.load_dimension([medico2, personas2, df2], ['dim_medico', 'dim_personas', 'fact'])
        foreign_keys={
                            "id_usuario": ("dim_personas", "cedula"),
                            "id_medico": ("dim_medico", "cedula"),
                            }
        loader_mysql.load_fact(df2, 'fact_prueba', foreign_keys)
                                                   

    except Exception as e:
        print(f"Error en la prueba ETL: {e}")
    finally:
        db_loader_postgres.close_connection()
        db_loader_oracle.close_connection()
        db_loader_mysql.close_connection()

if __name__ == "__main__":
    test_petl_search()
