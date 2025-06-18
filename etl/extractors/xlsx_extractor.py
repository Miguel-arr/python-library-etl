import os
import pandas as pd
import petl as etl
from tabulate import tabulate

class XLSXExtractor:
    def __init__(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        self.file_path = file_path

    def read_sheet(self, sheet_name=None, **kwargs):
        try:
            if sheet_name:
                data = pd.read_excel(self.file_path, sheet_name=sheet_name, **kwargs)
                print(f"Hoja '{sheet_name}' leída exitosamente.")
                return data
            else:
                all_sheets = pd.read_excel(self.file_path, sheet_name=None, **kwargs)
                print("Archivo leído exitosamente con todas las hojas.")
                return all_sheets
        except Exception as e:
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
            data = self.read_sheet(sheet_name, **kwargs)
            if sheet_name:
                print(f"Hoja: {sheet_name}")
                print(tabulate(data.head(n), headers='keys', tablefmt='grid', showindex=False))
            else:
                for name, df in data.items():
                    print(f"Hoja: {name}")
                    print(tabulate(df.head(n), headers='keys', tablefmt='grid', showindex=False))
                    print("-" * 40)
        except Exception as e:
            print(f"Error al previsualizar los datos: {e}")
            raise

    def toxlsx(self, df, filename=None, sheet_name="Sheet1", write_header=True, mode="replace"):
        if filename is None:
            filename = self.file_path

        if isinstance(df, pd.DataFrame):
            if df.columns.str.contains("Unnamed").any():
                df.columns = df.iloc[0]
                df = df[1:]
                df = df.reset_index(drop=True)

        table = etl.fromdataframe(df)
        try:
            etl.toxlsx(table, filename, sheet=sheet_name, write_header=write_header, mode=mode)
            print(f"Datos guardados en el archivo '{filename}', hoja '{sheet_name}'.")
        except Exception as e:
            print(f"Error al guardar los datos en el archivo Excel: {e}")
            raise


ruta = r"C:\Users\rodri\Desktop\fecha.xlsx"

extractor = XLSXExtractor(ruta)

print("Hojas disponibles:", extractor.get_sheet_names())

extractor.preview_data(sheet_name="Sheet1", n=5)

df = extractor.read_sheet(sheet_name="Sheet1")
extractor.toxlsx(df, filename=r"C:\Users\rodri\Desktop\fecha_guardado.xlsx", sheet_name="Sheet1")
