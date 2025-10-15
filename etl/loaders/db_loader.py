import pandas as pd
import sqlalchemy as sqlch

class DB_Loader:
    def __init__(self, engine=None):
        self.engine = engine
        if self.engine is None:
            print("⚠️ Advertencia: No se ha proporcionado un engine en el constructor. Deberás pasarlo a los métodos.")

    def _get_engine(self, engine):
        if engine is not None:
            return engine
        elif self.engine is not None:
            return self.engine
        else:
            raise ValueError("❌ No se ha proporcionado un engine de base de datos.")

    def load_dimension(self, dfs, table_names, if_exists="replace", index=False, engine=None):
        """
        Carga una o varias tablas de dimensión. Por defecto reemplaza la tabla completa.
        Parámetros:
        -----------
        dfs : DataFrame o lista de DataFrames
            Datos a cargar
        table_names : str o lista de str
            Nombre(s) de la(s) tabla(s) de dimensión
        if_exists : str
            Comportamiento si la tabla existe ('fail', 'replace', 'append')
        index : bool
            Si se incluye el índice del DataFrame
        engine : 
            Conexión a la base de datos
        """
        try:
            eng = self._get_engine(engine)
            db_backend = eng.url.get_backend_name()
            # Si es solo un DataFrame y un nombre, lo convierte en listas
            if isinstance(dfs, pd.DataFrame):
                dfs = [dfs]
            if isinstance(table_names, str):
                table_names = [table_names]
            if len(dfs) != len(table_names):
                raise ValueError("La cantidad de DataFrames y nombres de tabla debe coincidir.")
            for df, table_name in zip(dfs, table_names):
                df.to_sql(name=table_name, con=eng, if_exists=if_exists, index=index)
                print(f"✅ Dimensión '{table_name}' cargada exitosamente en la base de datos '{db_backend} (modo: {if_exists}).")
        except Exception as e:
            print(f"❌ Error al cargar dimensión: {e}")
            raise


    def load_fact(self, df, table_name, foreign_keys_map=None, if_exists="append", index=False, engine=None, dtype=None, validate_foreign_keys=True):
        """
        Carga una tabla de hechos con validación de claves foráneas.
        Convierte claves foráneas y primarias a string para evitar errores de tipo.
        """
        try:
            eng = self._get_engine(engine)
            db_backend = eng.url.get_backend_name()

            # Limitar columnas de texto a 4000 caracteres si el motor es Oracle
            if db_backend == "oracle":
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.slice(0, 4000)
                if dtype is None:
                    
                    dtype = {col: sqlch.types.VARCHAR(4000) 
                            for col in df.select_dtypes(include=['object']).columns}

            if foreign_keys_map:
                missing_in_fact = [fk for fk in foreign_keys_map.keys() if fk not in df.columns]
                if missing_in_fact:
                    raise ValueError(f"🚫 Columnas no encontradas en tabla de hechos: {missing_in_fact}")

                with eng.connect() as conn:
                    for fk_col, (dim_table, dim_pk) in foreign_keys_map.items():
                        # Convierte claves foráneas a string
                        df[fk_col] = df[fk_col].astype(str)
                        unique_values = df[fk_col].unique()
                        if len(unique_values) > 0:
                            if db_backend == "postgresql":
                                query = f"SELECT {dim_pk} FROM {dim_table} WHERE {dim_pk} = ANY(%(values)s)"
                                result = pd.read_sql(query, conn, params={"values": list(unique_values)})
                            elif db_backend == "oracle":
                                values_str = ",".join([f"'{str(v)}'" for v in unique_values])
                                query = f"SELECT {dim_pk} FROM {dim_table} WHERE TO_CHAR({dim_pk}) IN ({values_str})"
                                result = pd.read_sql(query, conn)
                            elif db_backend == "mysql":
                                values_str = ",".join([f"'{str(v)}'" for v in unique_values])
                                query = f"SELECT {dim_pk} FROM {dim_table} WHERE {dim_pk} IN ({values_str})"
                                result = pd.read_sql(query, conn)
                            else:
                                raise ValueError("Validación de claves foráneas solo soportada para PostgreSQL y Oracle.")
                            # Convierte resultado a string para comparar
                            result_ids = set(result[dim_pk].astype(str).unique())
                            missing_values = set([str(v) for v in unique_values]) - result_ids
                            if missing_values:
                                raise ValueError(
                                    f"🚫 Valores no encontrados en dimensión {dim_table}.{dim_pk}: "
                                    f"{missing_values} (para la columna {fk_col})"
                                )

            # Cargar los datos
            df.to_sql(name=table_name, con=eng, if_exists=if_exists, index=index, dtype=dtype)
            print(f"✅ Hechos cargados exitosamente en la tabla '{table_name}' (modo: {if_exists}).")

        except Exception as e:
            print(f"❌ Error al cargar hechos '{table_name}': {e}")
            # No se vuelve a lanzar el error para evitar doble impresión

    def truncate_table(self, table_name, engine=None):
        """
        Vacía completamente una tabla antes de la carga.
        """
        try:
            eng = self._get_engine(engine)
            with eng.connect() as conn:
                conn.execute(f"TRUNCATE TABLE {table_name}")
            print(f"🧹 Tabla '{table_name}' truncada exitosamente.")
        except Exception as e:
            print(f"❌ Error al truncar la tabla '{table_name}': {e}")
            raise

    def load_data(self, dataframe, table_name="Transformacion", if_exists="append", index=False, engine=None):
        """
        Carga genérica de datos.
        """
        try:
            eng = self._get_engine(engine)
            dataframe.to_sql(name=table_name, con=eng, if_exists=if_exists, index=index)
            print(f"✅ Datos cargados exitosamente en la tabla '{table_name}'.")
        except Exception as e:
            print(f"❌ Error al cargar datos: {e}")
            raise


