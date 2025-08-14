from sqlalchemy import create_engine
import pandas as pd

class DB_Extractor:
    def __init__(self, db_type, password, database, host="localhost", user="root", port=None, service_name=None):
        self.db_type = db_type.lower()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.service_name = service_name
        self.port = port or self._default_port()
        self.engine = None

    def _default_port(self):
        if self.db_type == "mysql":
            return 3306
        elif self.db_type == "postgresql":
            return 5432
        elif self.db_type == "oracle":
            return 1521
        else:
            raise ValueError("Tipo de base de datos no soportado.")

    def connect(self):
        try:
            if self.db_type == "mysql":  
                self.engine = create_engine(
                    f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                )
            elif self.db_type == "postgresql":
                self.engine = create_engine(
                    f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
                    connect_args={'client_encoding': 'latin1'}
                )
            elif self.db_type == "oracle":
                if not self.service_name:
                    raise ValueError("Debe proporcionar 'service_name' para Oracle.")
                
                dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"
                self.engine = create_engine(f"oracle+cx_oracle://{self.user}:{self.password}@{dsn}")
            else:
                raise ValueError("Tipo de base de datos no soportado. Usa 'mysql', 'postgresql' o 'oracle'.")

            with self.engine.connect():
                print(f"✅ Conexión exitosa a {self.db_type.upper()} en la base de datos '{self.database}'.")
        except Exception as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            raise

    def close_connection(self):
        if self.engine:
            self.engine.dispose()
            print("🔒 Conexión cerrada.")

    

    def execute_query(self, query):
        try:
            if self.engine is None:
                raise ValueError("No hay conexión activa. Llame a `connect()` primero.")

            with self.engine.connect() as connection:
                df = pd.read_sql_query(query, connection)
            
            print("✅ Consulta ejecutada con éxito.")
            return df
        except Exception as e:
            print(f"❌ Error al ejecutar la consulta SQL: {e}")
            raise

    def get_table(self, table_name):
        try:
            if self.engine is None:
                raise ValueError("No hay conexión activa. Llame a connect() primero.")
            
            df = pd.read_sql_table(table_name, con=self.engine)
            print(f"✅ Tabla '{table_name}' extraída con éxito.")
            return df
        except Exception as e:
            print(f"❌ Error al extraer la tabla '{table_name}': {e}")
            raise
