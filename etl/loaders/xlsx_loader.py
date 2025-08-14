import pandas as pd

class Excel_Loader:
    def __init__(self, default_path=None):
        self.default_path = default_path
    
    def _get_path(self, path):
        if path is not None:
            return path
        elif self.default_path is not None:
            return self.default_path
        else:
            raise ValueError("❌ No se ha proporcionado una ruta para el archivo Excel.")
    
    def load_dimension(self, df, sheet_name, path=None, if_exists="replace", index=False):
        """
        Carga una hoja de dimensión en un archivo Excel. Por defecto reemplaza el contenido.
        """
        try:
            file_path = self._get_path(path)
            mode = 'a' if if_exists == "append" else 'w'
            
            with pd.ExcelWriter(file_path, engine='openpyxl', mode=mode) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=index)
            print(f"✅ Dimensión '{sheet_name}' cargada exitosamente en Excel (modo: {if_exists}).")
        except Exception as e:
            print(f"❌ Error al cargar dimensión '{sheet_name}' en Excel: {e}")
            raise
    
    def load_fact(self, df, sheet_name, path=None, foreign_keys=None, if_exists="append", index=False):
        """
        Carga una hoja de hechos en un archivo Excel con validación de claves foráneas.
        """
        try:
            file_path = self._get_path(path)
            
            if foreign_keys:
                missing = [key for key in foreign_keys if key not in df.columns]
                if missing:
                    raise ValueError(f"🚫 Faltan columnas de clave foránea: {missing}")
            
            mode = 'a' if if_exists == "append" else 'w'
            
            with pd.ExcelWriter(file_path, engine='openpyxl', mode=mode) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=index)
            print(f"✅ Hechos cargados exitosamente en la hoja '{sheet_name}' (modo: {if_exists}).")
        except Exception as e:
            print(f"❌ Error al cargar hechos '{sheet_name}' en Excel: {e}")
            raise
    
    def clear_sheet(self, sheet_name, path=None):
        """
        Limpia completamente una hoja del archivo Excel.
        """
        try:
            file_path = self._get_path(path)
            
            # Leer el archivo completo
            with pd.ExcelFile(file_path) as xls:
                sheets = {sheet: pd.read_excel(xls, sheet_name=sheet) 
                         for sheet in xls.sheet_names if sheet != sheet_name}
            
            # Reescribir el archivo excluyendo la hoja a limpiar
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                for sheet, data in sheets.items():
                    data.to_excel(writer, sheet_name=sheet, index=False)
                # Añadir hoja vacía
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"🧹 Hoja '{sheet_name}' limpiada exitosamente.")
        except Exception as e:
            print(f"❌ Error al limpiar la hoja '{sheet_name}': {e}")
            raise
    
    def load_data(self, dataframe, sheet_name="Transformacion", path=None, if_exists="append", index=False):
        """
        Carga genérica de datos en una hoja Excel.
        """
        try:
            file_path = self._get_path(path)
            mode = 'a' if if_exists == "append" else 'w'
            
            with pd.ExcelWriter(file_path, engine='openpyxl', mode=mode) as writer:
                dataframe.to_excel(writer, sheet_name=sheet_name, index=index)
            print(f"✅ Datos cargados exitosamente en la hoja '{sheet_name}'.")
        except Exception as e:
            print(f"❌ Error al cargar datos en Excel: {e}")
            raise
    
    def get_sheet_names(self, path=None):
        """
        Obtiene la lista de hojas en el archivo Excel.
        """
        try:
            file_path = self._get_path(path)
            with pd.ExcelFile(file_path) as xls:
                return xls.sheet_names
        except Exception as e:
            print(f"❌ Error al obtener hojas del archivo Excel: {e}")
            raise