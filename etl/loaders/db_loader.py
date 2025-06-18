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

    def load_dimension(self, df, table_name, if_exists="replace", index=False, engine=None):
        """
        Carga una tabla de dimensión. Por defecto reemplaza la tabla completa.
        """
        try:
            eng = self._get_engine(engine)
            df.to_sql(name=table_name, con=eng, if_exists=if_exists, index=index)
            print(f"✅ Dimensión '{table_name}' cargada exitosamente (modo: {if_exists}).")
        except Exception as e:
            print(f"❌ Error al cargar dimensión '{table_name}': {e}")
            raise

    def load_fact(self, df, table_name, foreign_keys=None, if_exists="append", index=False, engine=None):
        """
        Carga una tabla de hechos con validación de claves foráneas.
        """
        try:
            eng = self._get_engine(engine)

            if foreign_keys:
                missing = [key for key in foreign_keys if key not in df.columns]
                if missing:
                    raise ValueError(f"🚫 Faltan columnas de clave foránea: {missing}")

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
