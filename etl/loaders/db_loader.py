import pandas as pd

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
            # Si es solo un DataFrame y un nombre, lo convierte en listas
            if isinstance(dfs, pd.DataFrame):
                dfs = [dfs]
            if isinstance(table_names, str):
                table_names = [table_names]
            if len(dfs) != len(table_names):
                raise ValueError("La cantidad de DataFrames y nombres de tabla debe coincidir.")
            for df, table_name in zip(dfs, table_names):
                df.to_sql(name=table_name, con=eng, if_exists=if_exists, index=index)
                print(f"✅ Dimensión '{table_name}' cargada exitosamente (modo: {if_exists}).")
        except Exception as e:
            print(f"❌ Error al cargar dimensión: {e}")
            raise


    def load_fact(self, df, table_name, foreign_keys_map=None, if_exists="append", index=False, engine=None):
        """
        Carga una tabla de hechos con validación de claves foráneas.
        
        Parámetros:
        -----------
        df : DataFrame
            Datos a cargar
        table_name : str
            Nombre de la tabla de hechos
        foreign_keys_map : dict
            Diccionario que mapea {columna_en_hechos: (tabla_dimension, columna_dimension)}
        if_exists : str
            Comportamiento si la tabla existe ('fail', 'replace', 'append')
        index : bool
            Si se incluye el índice del DataFrame
        engine : 
            Conexión a la base de datos
            
        Ejemplo:
        --------
        loader.load_fact(df, "ventas", 
                        foreign_keys_map={
                            "id_producto": ("dim_producto", "producto_id"),
                            "id_cliente": ("dim_clientes", "cliente_id"),
                            
                        })
        """
        try:
            eng = self._get_engine(engine)
            
            if foreign_keys_map:
                # Verificar que las columnas del mapa existan en el DataFrame
                missing_in_fact = [fk for fk in foreign_keys_map.keys() if fk not in df.columns]
                if missing_in_fact:
                    raise ValueError(f"🚫 Columnas no encontradas en tabla de hechos: {missing_in_fact}")
                
                # Validar relaciones con dimensiones
                with eng.connect() as conn:
                    for fk_col, (dim_table, dim_pk) in foreign_keys_map.items():
                        unique_values = df[fk_col].unique()
                        if len(unique_values) > 0:
                            # Compatible con PostgreSQL: = ANY(%(values)s)
                            query = f"SELECT {dim_pk} FROM {dim_table} WHERE {dim_pk} = ANY(%(values)s)"
                            result = pd.read_sql(query, conn, params={"values": list(unique_values)})
                            missing_values = set(unique_values) - set(result[dim_pk].unique())
                            if missing_values:
                                raise ValueError(
                                    f"🚫 Valores no encontrados en dimensión {dim_table}.{dim_pk}: "
                                    f"{missing_values} (para la columna {fk_col})"
                                )
            
            # Si todas las validaciones pasan, cargar los datos
            df.to_sql(name=table_name, con=eng, if_exists=if_exists, index=index)
            print(f"✅ Hechos cargados exitosamente en la tabla '{table_name}' (modo: {if_exists}).")
            
        except Exception as e:
            print(f"❌ Error al cargar hechos '{table_name}': {e}")
            raise

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


