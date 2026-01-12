# Importación de librerías necesarias
import os                  # Permite interactuar con el sistema operativo (por ejemplo, verificar si existe un archivo)
import pandas as pd        # Librería para manejo y análisis de datos (DataFrames)
import petl as etl         # Librería para transformaciones y exportaciones de datos (ligera y eficiente)
from tabulate import tabulate  # Sirve para mostrar datos en formato de tabla en consola


class XLSXExtractor:
    """
    Clase para manejar operaciones de lectura, previsualización y escritura de archivos Excel (.xlsx).
    Integra pandas y petl para combinar facilidad de lectura, procesamiento y exportación eficiente.
    """

    def __init__(self, file_path):
        """
        Inicializa el objeto con la ruta del archivo Excel.
        Verifica que el archivo exista antes de proceder.

        Parámetros:
        - file_path (str): Ruta completa del archivo Excel a procesar.
        """
        if not os.path.exists(file_path):  # Verifica si el archivo existe
            raise FileNotFoundError(f"El archivo {file_path} no existe.")  # Lanza error si no existe
        self.file_path = file_path  # Guarda la ruta del archivo como atributo del objeto

    def read_sheet(self, sheet_name=None, **kwargs):
        """
        Lee una hoja específica del archivo Excel o todas las hojas si no se especifica una.

        Parámetros:
        - sheet_name (str, opcional): Nombre de la hoja a leer. Si es None, lee todas.
        - **kwargs: Argumentos adicionales que se pasan a pd.read_excel (ej. header, skiprows, etc.)

        Retorna:
        - DataFrame o diccionario de DataFrames (si se leen todas las hojas).
        """
        try:
            if sheet_name:
                # Lee una hoja específica del Excel
                data = pd.read_excel(self.file_path, sheet_name=sheet_name, **kwargs)
                print(f"Hoja '{sheet_name}' leída exitosamente.")
                return data
            else:
                # Lee todas las hojas del archivo (retorna un diccionario con nombre_hoja: DataFrame)
                all_sheets = pd.read_excel(self.file_path, sheet_name=None, **kwargs)
                print("Archivo leído exitosamente con todas las hojas.")
                return all_sheets
        except Exception as e:
            # Captura y muestra cualquier error al leer el archivo
            print(f"Error al leer el archivo Excel: {e}")
            raise


    def get_sheet_names(self):
        try:
            xls = pd.ExcelFile(self.file_path)
            return xls.sheet_names
        except Exception as e:
            print(f"Error al obtener los nombres de las hojas: {e}")
            raise


    def preview_data(self, sheet_name=None, n=5, **kwargs):
        try:
            # Lee la(s) hoja(s) solicitada(s)
            data = self.read_sheet(sheet_name, **kwargs)

            if sheet_name:
                # Si se pasa una hoja específica, la muestra formateada como tabla
                print(f"Hoja: {sheet_name}")
                print(tabulate(data.head(n), headers='keys', tablefmt='grid', showindex=False))
            else:
                # Si no se especifica, recorre todas las hojas del archivo
                for name, df in data.items():
                    print(f"Hoja: {name}")
                    print(tabulate(df.head(n), headers='keys', tablefmt='grid', showindex=False))
                    print("-" * 40)  # Separador visual entre hojas
        except Exception as e:
            print(f"Error al previsualizar los datos: {e}")
            raise

    def toxlsx(self, df, filename=None, sheet_name="Sheet1", write_header=True, mode="replace"):
        """
        Exporta un DataFrame a un archivo Excel, creando o reemplazando hojas según configuración.

        Parámetros:
        - df (DataFrame): Datos que se desean exportar al archivo Excel.
        - filename (str, opcional): Nombre o ruta del archivo de destino. 
          Si no se especifica, se sobrescribe el archivo original.
        - sheet_name (str): Nombre de la hoja donde se guardarán los datos.
        - write_header (bool): Indica si se escriben los nombres de las columnas.
        - mode (str): Modo de escritura ('replace', 'append', etc.)
        """
        # Si no se especifica un nuevo archivo, usa el original
        if filename is None:
            filename = self.file_path

        # Validación para eliminar columnas "Unnamed" que a veces crea Excel
        if isinstance(df, pd.DataFrame):
            if df.columns.str.contains("Unnamed").any():
                df.columns = df.iloc[0]          # Reasigna la primera fila como nombres de columnas
                df = df[1:]                      # Elimina la primera fila del cuerpo de datos
                df = df.reset_index(drop=True)   # Reinicia el índice del DataFrame

        # Convierte el DataFrame a formato petl (tabla)
        table = etl.fromdataframe(df)

        try:
            # Exporta los datos a Excel usando petl
            etl.toxlsx(table, filename, sheet=sheet_name, write_header=write_header, mode=mode)
            print(f"Datos guardados en el archivo '{filename}', hoja '{sheet_name}'.")
        except Exception as e:
            # Captura y muestra errores al exportar
            print(f"Error al guardar los datos en el archivo Excel: {e}")
            raise

