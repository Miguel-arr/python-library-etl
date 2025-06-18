import pandas as pd
import os

class CSV_Loader:
    def __init__(self, base_path=None):
        """
        Inicializa el cargador CSV con una ruta base opcional
        """
        self.base_path = base_path
        if self.base_path and not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
            print(f"📂 Directorio creado: {self.base_path}")

    def _get_full_path(self, filename):
        """Construye la ruta completa del archivo"""
        if self.base_path:
            return os.path.join(self.base_path, filename)
        return filename

    def load_csv(self, filename, sep=",", encoding="utf-8", **kwargs):
        """
        Carga un archivo CSV en un DataFrame
        Args:
            filename: Nombre del archivo (o ruta completa)
            sep: Separador de campos
            encoding: Codificación del archivo
            **kwargs: Argumentos adicionales para pd.read_csv()
        Returns:
            DataFrame con los datos cargados
        """
        try:
            full_path = self._get_full_path(filename)
            df = pd.read_csv(full_path, sep=sep, encoding=encoding, **kwargs)
            print(f"✅ CSV cargado exitosamente: {full_path} ({len(df)} registros)")
            return df
        except Exception as e:
            print(f"❌ Error al cargar CSV {filename}: {e}")
            raise

    def save_csv(self, df, filename, index=False, sep=",", encoding="utf-8", **kwargs):
        """
        Guarda un DataFrame en archivo CSV
        Args:
            df: DataFrame a guardar
            filename: Nombre del archivo destino
            index: Si se incluye el índice
            sep: Separador de campos
            encoding: Codificación del archivo
            **kwargs: Argumentos adicionales para df.to_csv()
        """
        try:
            full_path = self._get_full_path(filename)
            df.to_csv(full_path, index=index, sep=sep, encoding=encoding, **kwargs)
            print(f"💾 CSV guardado exitosamente: {full_path} ({len(df)} registros)")
        except Exception as e:
            print(f"❌ Error al guardar CSV {filename}: {e}")
            raise

    def merge_csvs(self, file_pattern, output_filename, how="outer", **kwargs):
        """
        Combina múltiples archivos CSV en uno solo
        Args:
            file_pattern: Patrón para encontrar archivos (ej: "data_*.csv")
            output_filename: Nombre del archivo combinado
            how: Tipo de merge (outer, inner, left, right)
            **kwargs: Argumentos adicionales para pd.concat()
        """
        try:
            if not self.base_path:
                raise ValueError("Se requiere base_path para merge_csvs")
                
            all_files = [f for f in os.listdir(self.base_path) if f.endswith('.csv') and f.startswith(file_pattern)]
            if not all_files:
                raise FileNotFoundError(f"No se encontraron archivos con patrón: {file_pattern}")
            
            dfs = [self.load_csv(f) for f in all_files]
            merged_df = pd.concat(dfs, axis=0, ignore_index=True, **kwargs)
            self.save_csv(merged_df, output_filename)
            print(f"🔀 Merge completado: {len(all_files)} archivos → {output_filename}")
            return merged_df
        except Exception as e:
            print(f"❌ Error al fusionar CSVs: {e}")
            raise

    def validate_csv(self, filename, required_columns=None):
        """
        Valida la estructura básica de un archivo CSV
        Args:
            filename: Nombre del archivo a validar
            required_columns: Lista de columnas requeridas
        Returns:
            Tuple (bool, str) con resultado y mensaje
        """
        try:
            df = self.load_csv(filename)
            
            if required_columns:
                missing = [col for col in required_columns if col not in df.columns]
                if missing:
                    return (False, f"Columnas faltantes: {missing}")
                    
            return (True, f"CSV válido: {filename} ({len(df)} registros, {len(df.columns)} columnas)")
        except Exception as e:
            return (False, f"Error de validación: {str(e)}")