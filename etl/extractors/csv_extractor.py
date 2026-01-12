import os
import pandas as pd
import petl as etl
from tabulate import tabulate
from typing import Union, Optional, Dict, Any

class CSVExtractor:
    """
    Clase especializada en extracción, visualización y transformación de archivos CSV.
    
    Combina la potencia de pandas para análisis de datos con petl para transformaciones ETL
    y tabulate para visualización elegante de datos.
    
    Características principales:
    - Lectura y escritura de archivos CSV
    - Vista previa formateada de datos
    - Soporte para modos append/replace en escritura
    - Manejo automático de headers problemáticos
    - Integración con pandas DataFrame y petl Table
    """
    
    def __init__(self, file_path: str):
        """
        Inicializa el extractor CSV con validación de existencia del archivo.
        
        Parámetros:
        -----------
        file_path : str
            Ruta completa al archivo CSV a procesar
            
        Lanza:
        ------
        FileNotFoundError
            Si el archivo especificado no existe
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        self.file_path = file_path
    
    def read_csv(self, **kwargs) -> pd.DataFrame:
        """
        Lee un archivo CSV y devuelve un DataFrame de pandas.
        
        Utiliza pd.read_csv() internamente, por lo que soporta todos sus parámetros
        para manejar diferentes formatos y codificaciones de CSV.
        
        Parámetros:
        -----------
        **kwargs : dict
            Argumentos adicionales para pd.read_csv():
            - sep: Separador de campos (default ',')
            - encoding: Codificación del archivo (default 'utf-8')
            - dtype: Especificación de tipos de datos por columna
            - parse_dates: Columnas a parsear como fechas
            - na_values: Valores a considerar como NaN
            - skiprows: Filas a saltar al inicio
            - nrows: Número máximo de filas a leer
            - usecols: Columnas específicas a leer
            
        Retorna:
        --------
        DataFrame
            DataFrame de pandas con los datos del CSV
            
        Lanza:
        ------
        Exception
            - Si el archivo está corrupto o vacío
            - Si hay problemas de codificación
            - Si no hay permisos de lectura
            
        Ejemplos:
        ---------
        >>> extractor = CSVExtractor("datos.csv")
        >>> df = extractor.read_csv(encoding='latin-1', sep=';')
        >>> df = extractor.read_csv(usecols=['nombre', 'edad'], nrows=1000)
        """
        try:
            data = pd.read_csv(self.file_path, **kwargs)
            print(f"✅ Archivo CSV leído exitosamente: {self.file_path} "
                  f"({len(data)} registros, {len(data.columns)} columnas)")
            return data
        except Exception as e:
            print(f"❌ Error al leer el archivo CSV: {e}")
            raise
    
    def preview_data(self, n: int = 5, **kwargs) -> None:
        """
        Muestra una vista previa formateada de los datos del CSV.
        
        Utiliza tabulate para presentar los datos en formato de tabla legible
        en consola, ideal para inspección rápida de la estructura de datos.
        
        Parámetros:
        -----------
        n : int, default 5
            Número de filas a mostrar en la vista previa
        **kwargs : dict
            Argumentos adicionales para pd.read_csv()
            
        Lanza:
        ------
        Exception
            Si hay errores al leer o formatear los datos
            
        Ejemplos:
        ---------
        >>> extractor.preview_data(n=10)
        >>> extractor.preview_data(encoding='latin-1')
        """
        try:
            data = self.read_csv(**kwargs)
            print(f"\n📊 Vista previa de datos ({n} filas de {len(data)} totales):")
            print(f"📁 Archivo: {self.file_path}")
            print(f"📏 Dimensiones: {len(data)} filas × {len(data.columns)} columnas")
            print(f"🏷️ Columnas: {list(data.columns)}")
            print("\n" + "="*80)
            
            # Mostrar tabla formateada
            print(tabulate(data.head(n), headers='keys', tablefmt='grid', showindex=False))
            
            # Información adicional sobre tipos de datos
            print(f"\n📋 Tipos de datos:")
            for col, dtype in data.dtypes.head(5).items():
                print(f"  - {col}: {dtype}")
                
            if len(data.columns) > 5:
                print(f"  ... y {len(data.columns) - 5} columnas más")
                
        except Exception as e:
            print(f"❌ Error al previsualizar los datos: {e}")
            raise
    
    def tocsv(self, df: pd.DataFrame, filename: Optional[str] = None, 
              write_header: bool = True, mode: str = "replace", **kwargs) -> None:
        """
        Guarda datos en un archivo CSV usando petl para operaciones avanzadas.
        
        Soporta modos de escritura 'replace' (sobrescribir) y 'append' (añadir),
        con manejo automático de headers problemáticos y conversión entre
        pandas DataFrame y petl Table.
        
        Parámetros:
        -----------
        df : DataFrame
            DataFrame de pandas a guardar
        filename : str, opcional
            Ruta del archivo destino. Si es None, usa self.file_path
        write_header : bool, default True
            Si escribe la fila de encabezados (nombres de columnas)
        mode : str, default "replace"
            Modo de escritura:
            - "replace": Sobrescribe el archivo existente
            - "append": Añade datos al final del archivo existente
        **kwargs : dict
            Argumentos adicionales para etl.tocsv()
            
        Lanza:
        ------
        ValueError
            - Si el modo no es 'replace' o 'append'
            - Si el DataFrame está vacío
        Exception
            - Si hay errores de escritura o permisos
            
        Ejemplos:
        ---------
        >>> # Sobrescribir archivo
        >>> extractor.tocsv(df, "nuevos_datos.csv", mode="replace")
        >>> # Añadir a archivo existente
        >>> extractor.tocsv(df, "datos_existentes.csv", mode="append")
        >>> # Guardar sin headers
        >>> extractor.tocsv(df, "sin_headers.csv", write_header=False)
        """
        if filename is None:
            filename = self.file_path

        # Validar parámetros
        if mode not in ["replace", "append"]:
            raise ValueError(f"Modo '{mode}' no válido. Use 'replace' o 'append'.")
            
        if df.empty:
            print("⚠️ Advertencia: El DataFrame está vacío. Se creará un archivo vacío.")

        # Manejar headers problemáticos (columnas "Unnamed")
        if isinstance(df, pd.DataFrame):
            if df.columns.str.contains("Unnamed").any():
                print("🔄 Detectadas columnas 'Unnamed', aplicando corrección automática...")
                df.columns = df.iloc[0]  # Usar primera fila como headers
                df = df[1:]  # Eliminar primera fila (ahora son headers)
                df = df.reset_index(drop=True)  # Resetear índice

        # Convertir DataFrame a tabla petl
        table = etl.fromdataframe(df)
        
        try:
            if mode == "append" and os.path.exists(filename):
                print(f"📥 Modo append: añadiendo {len(df)} registros a '{filename}'")
                # Leer datos existentes y combinar con nuevos
                existing = etl.fromcsv(filename)
                combined = etl.cat(existing, table)
                etl.tocsv(combined, filename, write_header=write_header, **kwargs)
            else:
                # Modo replace o archivo nuevo
                action = "reemplazando" if os.path.exists(filename) else "creando"
                print(f"💾 {action.capitalize()} archivo '{filename}' con {len(df)} registros")
                etl.tocsv(table, filename, write_header=write_header, **kwargs)
            
            print(f"✅ Datos guardados exitosamente en '{filename}' "
                  f"(modo: {mode}, registros: {len(df)}).")
                  
        except Exception as e:
            print(f"❌ Error al guardar los datos en el archivo CSV: {e}")
            raise

    def get_basic_stats(self, **kwargs) -> Dict[str, Any]:
        """
        Obtiene estadísticas básicas del archivo CSV.
        
        Parámetros:
        -----------
        **kwargs : dict
            Argumentos adicionales para pd.read_csv()
            
        Retorna:
        --------
        dict
            Diccionario con estadísticas del archivo:
            {
                'file_size': tamaño en bytes,
                'rows': número de filas,
                'columns': número de columnas,
                'column_names': lista de nombres de columnas,
                'memory_usage': uso de memoria en MB
            }
        """
        try:
            df = self.read_csv(**kwargs)
            file_size = os.path.getsize(self.file_path)
            
            stats = {
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': df.columns.tolist(),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
                'data_types': df.dtypes.astype(str).to_dict()
            }
            
            print(f"📈 Estadísticas de '{self.file_path}':")
            for key, value in stats.items():
                if key != 'column_names' and key != 'data_types':
                    print(f"  - {key}: {value}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error al obtener estadísticas: {e}")
            raise

    def convert_to_petl(self, **kwargs) -> etl.Table:
        """
        Convierte el archivo CSV a una tabla petl para transformaciones ETL.
        
        Parámetros:
        -----------
        **kwargs : dict
            Argumentos adicionales para etl.fromcsv()
            
        Retorna:
        --------
        petl.Table
            Tabla petl para operaciones de transformación
            
        Ejemplos:
        ---------
        >>> table = extractor.convert_to_petl()
        >>> table = etl.cut(table, 'nombre', 'edad')
        >>> table = etl.sort(table, 'edad')
        """
        try:
            table = etl.fromcsv(self.file_path, **kwargs)
            print(f"🔄 CSV convertido a tabla petl: {self.file_path}")
            return table
        except Exception as e:
            print(f"❌ Error al convertir a tabla petl: {e}")
            raise

    def validate_csv_structure(self, required_columns: Optional[list] = None, 
                              min_rows: int = 1, **kwargs) -> tuple:
        """
        Valida la estructura básica del archivo CSV.
        
        Parámetros:
        -----------
        required_columns : list, opcional
            Lista de columnas requeridas
        min_rows : int, default 1
            Número mínimo de filas requeridas
        **kwargs : dict
            Argumentos para pd.read_csv()
            
        Retorna:
        --------
        tuple
            (bool, str): Resultado de validación y mensaje
        """
        try:
            df = self.read_csv(**kwargs)
            
            issues = []
            
            # Validar columnas requeridas
            if required_columns:
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    issues.append(f"Columnas faltantes: {missing_cols}")
            
            # Validar número de filas
            if len(df) < min_rows:
                issues.append(f"Mínimo {min_rows} filas requeridas, encontradas: {len(df)}")
            
            # Validar que no esté vacío
            if df.empty:
                issues.append("El archivo está vacío")
            
            if issues:
                return False, f"Problemas de validación: {'; '.join(issues)}"
            else:
                return True, f"CSV válido: {len(df)} filas, {len(df.columns)} columnas"
                
        except Exception as e:
            return False, f"Error en validación: {str(e)}"


# Ejemplo de uso mejorado
if __name__ == "__main__":
    ruta = r"C:\Users\rodri\Desktop\datos.csv"
    
    try:
        # Crear extractor
        extractor = CSVExtractor(ruta)
        
        # Obtener estadísticas
        stats = extractor.get_basic_stats()
        
        # Vista previa de los datos
        extractor.preview_data(n=5)
        
        # Leer datos completos
        df = extractor.read_csv()
        
        # Validar estructura
        is_valid, message = extractor.validate_csv_structure(
            required_columns=['id', 'nombre'], 
            min_rows=1
        )
        print(f"Validación: {message}")
        
        # Guardar datos (nuevo archivo)
        extractor.tocsv(df, filename=r"C:\Users\rodri\Desktop\datos_copia.csv")
        
        # Añadir datos a un archivo existente
        extractor.tocsv(df, filename=r"C:\Users\rodri\Desktop\datos_copia.csv", mode="append")
        
        # Convertir a petl para transformaciones
        petl_table = extractor.convert_to_petl()
        print(f"Tabla petl creada con {petl_table.nrows()} filas")
        
    except Exception as e:
        print(f"❌ Error en el proceso: {e}")