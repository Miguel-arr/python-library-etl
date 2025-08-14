import os
import pandas as pd
import petl as etl
from tabulate import tabulate

class CSVExtractor:
    def __init__(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        self.file_path = file_path
    
    def read_csv(self, **kwargs):
        """
        Lee un archivo CSV y devuelve un DataFrame de pandas.
        
        Args:
            **kwargs: Argumentos adicionales para pd.read_csv()
        """
        try:
            data = pd.read_csv(self.file_path, **kwargs)
            print(f"Archivo CSV leído exitosamente: {self.file_path}")
            return data
        except Exception as e:
            print(f"Error al leer el archivo CSV: {e}")
            raise
    
    def preview_data(self, n=5, **kwargs):
        """
        Muestra una vista previa de los datos del CSV.
        
        Args:
            n: Número de filas a mostrar
            **kwargs: Argumentos adicionales para pd.read_csv()
        """
        try:
            data = self.read_csv(**kwargs)
            print(tabulate(data.head(n), headers='keys', tablefmt='grid', showindex=False))
        except Exception as e:
            print(f"Error al previsualizar los datos: {e}")
            raise
    
    def tocsv(self, df, filename=None, write_header=True, mode="replace", **kwargs):
        """
        Guarda datos en un archivo CSV.
        
        Args:
            df: DataFrame a guardar
            filename: Ruta del archivo destino (si None, usa self.file_path)
            write_header: Si escribe encabezados
            mode: "replace" (sobreescribe) o "append" (añade)
            **kwargs: Argumentos adicionales para pd.to_csv()
        """
        if filename is None:
            filename = self.file_path

        if isinstance(df, pd.DataFrame):
            if df.columns.str.contains("Unnamed").any():
                df.columns = df.iloc[0]
                df = df[1:]
                df = df.reset_index(drop=True)

        table = etl.fromdataframe(df)
        
        try:
            if mode == "append" and os.path.exists(filename):
                # Modo append usando petl
                existing = etl.fromcsv(filename)
                combined = etl.cat(existing, table)
                etl.tocsv(combined, filename, write_header=write_header)
            else:
                # Modo replace o archivo nuevo
                etl.tocsv(table, filename, write_header=write_header)
            
            print(f"Datos guardados en el archivo '{filename}' (modo: {mode}).")
        except Exception as e:
            print(f"Error al guardar los datos en el archivo CSV: {e}")
            raise


# Ejemplo de uso
if __name__ == "__main__":
    ruta = r"C:\Users\rodri\Desktop\datos.csv"
    
    try:
        extractor = CSVExtractor(ruta)
        
        # Vista previa de los datos
        extractor.preview_data(n=5)
        
        # Leer datos completos
        df = extractor.read_csv()
        
        # Guardar datos (nuevo archivo)
        extractor.tocsv(df, filename=r"C:\Users\rodri\Desktop\datos_copia.csv")
        
        # Añadir datos a un archivo existente
        extractor.tocsv(df, filename=r"C:\Users\rodri\Desktop\datos_copia.csv", mode="append")
        
    except Exception as e:
        print(f"Error en el proceso: {e}")