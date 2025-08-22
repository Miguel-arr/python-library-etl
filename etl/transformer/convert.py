import pandas as pd
from tabulate import tabulate

import pandas as pd
from tabulate import tabulate

class ConvertOperations:
    
    @staticmethod
    def head(df, n=5, print_result=True):
        # Devuelve o imprime las primeras n filas del DataFrame.
        try:
            df_head = df.head(n)
            result = tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
            if print_result:
                print(result)
            return result
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}")
            raise
    
    @staticmethod
    def convert_column_type(df, columns, dtype, show=0):
        """
        Convierte el tipo de una o varias columnas.
        
        Args:
            df: DataFrame de pandas
            columns: str o list - Columnas a convertir
            dtype: str o dict - Tipo de dato objetivo (ej. 'int', 'float', 'str', 'datetime', 'category')
                              o diccionario con {columna: tipo}
            show: int - Mostrar las primeras filas (0: no mostrar, -1: mostrar todo, n: mostrar n filas)
        
        Returns:
            DataFrame con las columnas convertidas
        """
        try:
            # Validación de parámetros
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El primer argumento debe ser un DataFrame")
            if not isinstance(columns, (str, list)):
                raise TypeError("columns debe ser un string o una lista")
            if not isinstance(dtype, (str, dict)):
                raise TypeError("dtype debe ser un string o un diccionario")
            
            # Normalización de parámetros
            if isinstance(columns, str):
                columns = [columns]
                
            if isinstance(dtype, str):
                dtype = {col: dtype for col in columns}
            
            # Conversión de tipos
            for col, target_type in dtype.items():
                if col not in df.columns:
                    raise ValueError(f"La columna '{col}' no existe en el DataFrame")
                
                if target_type == 'datetime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif target_type == 'category':
                    df[col] = df[col].astype('category')
                elif target_type == 'str':
                    df[col] = df[col].astype(str)
                elif target_type in ['int', 'float']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(target_type)
                else:
                    df[col] = df[col].astype(target_type)
            
            # Mostrar resultados si show está habilitado
            if show > 0:
                print(ConvertOperations.head(df, show))
            elif show == -1:
                print(ConvertOperations.head(df, len(df)))
            
            return df
            
        except Exception as e:
            print(f"Error al convertir tipos de columna: {e}")
            raise
    
    @staticmethod
    def clean_numeric_columns(df, columns, show=0):
        """
        Limpia columnas numéricas eliminando símbolos no numéricos ($, %, comas, etc.)
        
        Args:
            df: DataFrame
            columns: str o list - Columnas a limpiar
            show: int - Mostrar las primeras filas (0: no mostrar, -1: mostrar todo, n: mostrar n filas)
        
        Returns:
            DataFrame con columnas numéricas limpias
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El primer argumento debe ser un DataFrame")
            if not isinstance(columns, (str, list)):
                raise TypeError("columns debe ser un string o una lista")
            
            if isinstance(columns, str):
                columns = [columns]
            
            for col in columns:
                if col not in df.columns:
                    raise ValueError(f"La columna '{col}' no existe en el DataFrame")
                
                # Elimina caracteres no numéricos excepto punto y signo negativo
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True),
                    errors='coerce'
                )
            
            # Mostrar resultados si show está habilitado
            if show > 0:
                print(ConvertOperations.head(df, show))
            elif show == -1:
                print(ConvertOperations.head(df, len(df)))
            
            return df
            
        except Exception as e:
            print(f"Error al limpiar columnas numéricas: {e}")
            raise
    
    @staticmethod
    def convert_to_ordered_category(df, column, categories, ordered=True, show=0):
        """
        Convierte una columna a categoría ordenada.
        
        Args:
            df: DataFrame
            column: str - Columna a convertir
            categories: list - Categorías en orden
            ordered: bool - Si las categorías tienen orden
            show: int - Mostrar las primeras filas (0: no mostrar, -1: mostrar todo, n: mostrar n filas)
        
        Returns:
            DataFrame con la columna convertida a categoría ordenada
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El primer argumento debe ser un DataFrame")
            if not isinstance(column, str):
                raise TypeError("column debe ser un string")
            if not isinstance(categories, list):
                raise TypeError("categories debe ser una lista")
            if not isinstance(ordered, bool):
                raise TypeError("ordered debe ser un booleano")
            
            if column not in df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame")
            
            df[column] = pd.Categorical(
                df[column],
                categories=categories,
                ordered=ordered
            )
            
            # Mostrar resultados si show está habilitado
            if show > 0:
                print(ConvertOperations.head(df, show))
            elif show == -1:
                print(ConvertOperations.head(df, len(df)))
            
            return df
            
        except Exception as e:
            print(f"Error al convertir a categoría ordenada: {e}")
            raise
    
    @staticmethod
    def extract_date_components(df, date_column, components=None, show=0):
        """
        Extrae componentes de fecha (año, mes, día) de una columna datetime.
        
        Args:
            df: DataFrame
            date_column: str - Columna con fechas
            components: list - Componentes a extraer ['year', 'month', 'day', 'weekday', 'hour', 'minute']
            show: int - Mostrar las primeras filas (0: no mostrar, -1: mostrar todo, n: mostrar n filas)
        
        Returns:
            DataFrame con nuevas columnas para cada componente
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El primer argumento debe ser un DataFrame")
            if not isinstance(date_column, str):
                raise TypeError("date_column debe ser un string")
            
            if components is None:
                components = ['year', 'month', 'day']
            elif not isinstance(components, list):
                raise TypeError("components debe ser una lista")
            
            if date_column not in df.columns:
                raise ValueError(f"La columna '{date_column}' no existe en el DataFrame")
            
            # Primero aseguramos que sea datetime
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            
            for component in components:
                if component == 'year':
                    df[f'{date_column}_year'] = df[date_column].dt.year
                elif component == 'month':
                    df[f'{date_column}_month'] = df[date_column].dt.month
                elif component == 'day':
                    df[f'{date_column}_day'] = df[date_column].dt.day
                elif component == 'weekday':
                    df[f'{date_column}_weekday'] = df[date_column].dt.weekday
                elif component == 'hour':
                    df[f'{date_column}_hour'] = df[date_column].dt.hour
                elif component == 'minute':
                    df[f'{date_column}_minute'] = df[date_column].dt.minute
                else:
                    raise ValueError(f"Componente '{component}' no reconocido")
            
            # Mostrar resultados si show está habilitado
            if show > 0:
                print(ConvertOperations.head(df, show))
            elif show == -1:
                print(ConvertOperations.head(df, len(df)))
            
            return df
            
        except Exception as e:
            print(f"Error al extraer componentes de fecha: {e}")
            raise
    
    @staticmethod
    def boolean_to_binary(df, columns, show=0):
        """
        Convierte columnas booleanas a binarias (0 y 1).
        
        Args:
            df: DataFrame
            columns: str o list - Columnas a convertir
            show: int - Mostrar las primeras filas (0: no mostrar, -1: mostrar todo, n: mostrar n filas)
        
        Returns:
            DataFrame con columnas convertidas a binario
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El primer argumento debe ser un DataFrame")
            if not isinstance(columns, (str, list)):
                raise TypeError("columns debe ser un string o una lista")
            
            if isinstance(columns, str):
                columns = [columns]
            
            for col in columns:
                if col not in df.columns:
                    raise ValueError(f"La columna '{col}' no existe en el DataFrame")
                
                df[col] = df[col].astype(int)
            
            # Mostrar resultados si show está habilitado
            if show > 0:
                print(ConvertOperations.head(df, show))
            elif show == -1:
                print(ConvertOperations.head(df, len(df)))
            
            return df
            
        except Exception as e:
            print(f"Error al convertir a binario: {e}")
            raise
    
    @staticmethod
    def split_string_column(df, column, delimiter, new_columns=None, show=0):
        """
        Divide una columna de strings en múltiples columnas.
        
        Args:
            df: DataFrame
            column: str - Columna a dividir
            delimiter: str - Delimitador para dividir los strings
            new_columns: list - Nombres para las nuevas columnas (opcional)
            show: int - Mostrar las primeras filas (0: no mostrar, -1: mostrar todo, n: mostrar n filas)
        
        Returns:
            DataFrame con las nuevas columnas añadidas
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El primer argumento debe ser un DataFrame")
            if not isinstance(column, str):
                raise TypeError("column debe ser un string")
            if not isinstance(delimiter, str):
                raise TypeError("delimiter debe ser un string")
            if new_columns is not None and not isinstance(new_columns, list):
                raise TypeError("new_columns debe ser una lista o None")
            
            if column not in df.columns:
                raise ValueError(f"La columna '{column}' no existe en el DataFrame")
            
            split_data = df[column].str.split(delimiter, expand=True)
            
            if new_columns:
                if len(new_columns) != split_data.shape[1]:
                    raise ValueError("El número de nombres de columnas no coincide con los datos divididos")
                split_data.columns = new_columns
            else:
                split_data.columns = [f"{column}_{i+1}" for i in range(split_data.shape[1])]
            
            df = pd.concat([df, split_data], axis=1)
            
            # Mostrar resultados si show está habilitado
            if show > 0:
                print(ConvertOperations.head(df, show))
            elif show == -1:
                print(ConvertOperations.head(df, len(df)))
            
            return df
            
        except Exception as e:
            print(f"Error al dividir columna de strings: {e}")
            raise                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                


def main():
    # 1. Creación de un DataFrame de ejemplo
    print("\n" + "="*60)
    print("1. CREACIÓN DEL DATAFRAME DE EJEMPLO")
    print("="*60)
    
    data = {
        'id': ['001', '002', '003', '004', '005'],
        'fecha': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-04-45'],
        'precio': ['$1,200.50ll', '$950.75j', '$2,300.00sdf', '$1,850.25fsd', '$2,850.25fsd'],
        'cantidad': ['100n', 'n150', '$$200', '175 $$', '700 $$'],
        'categoria': ['Bajo', 'Medio', 'Alto', 'Medio', 'Bajo'],
        'activo': [True, False, True, True, False],
        'nombre_completo': ['Juan Pérez', 'María García', 'Carlos López', 'Ana Martínez', 'Miguel Rodriguez']
    }

    df = pd.DataFrame(data)
    print("\nDataFrame original:")
    print(ConvertOperations.head(df, -1))  # Mostrar las 4 filas con el nuevo método head

    # 2. Limpieza de columnas numéricas (mostrando solo 2 filas)
    print("\n" + "="*60)
    print("2. LIMPIEZA DE COLUMNAS NUMÉRICAS (mostrando 2 filas)")
    print("="*60)
    
    df = ConvertOperations.clean_numeric_columns(df, ['precio', 'cantidad'], show=2)

    # 3. Conversión de tipos de columnas (mostrando todo el dataframe)
    print("\n" + "="*60)
    print("3. CONVERSIÓN DE TIPOS DE COLUMNAS (mostrando todo)")
    print("="*60)
    
    df = ConvertOperations.convert_column_type(
        df, 
        columns=['id', 'cantidad', 'precio', 'fecha', 'activo'],
        dtype={
            'id': 'int',
            'cantidad': 'int',
            'precio': 'float',
            'fecha': 'datetime',
            'activo': 'int'
        },
        show=-1  # Mostrar todo el dataframe
    )

    # 4. Conversión a categoría ordenada (mostrando 3 filas)
    print("\n" + "="*60)
    print("4. CONVERSIÓN A CATEGORÍA ORDENADA (mostrando 3 filas)")
    print("="*60)
    
    df = ConvertOperations.convert_to_ordered_category(
        df,
        'categoria',
        categories=['Bajo', 'Medio', 'Alto'],
        ordered=True,
        show=3
    )
    print("\nCategorías ordenadas:", df['categoria'].cat.categories)

    # 5. Extracción de componentes de fecha (sin mostrar)
    print("\n" + "="*60)
    print("5. EXTRACCIÓN DE COMPONENTES DE FECHA (sin mostrar)")
    print("="*60)
    
    df = ConvertOperations.extract_date_components(
        df,
        'fecha',
        components=['year', 'month', 'day', 'weekday'],
        show=0  # No mostrar resultados
    )
    # Mostramos manualmente después
    print(ConvertOperations.head(df, 2))

    # 6. División de columna de strings (mostrando todo)
    print("\n" + "="*60)
    print("6. DIVISIÓN DE COLUMNA DE STRINGS (mostrando todo)")
    print("="*60)
    
    df = ConvertOperations.split_string_column(
        df,
        'nombre_completo',
        delimiter=' ',
        new_columns=['nombre', 'apellido'],
        show=-1  # Mostrar todo
    )

    # Resultado final (mostrando todo con el método head)
    print("\n" + "="*60)
    print("RESULTADO FINAL DEL DATAFRAME TRANSFORMADO")
    print("="*60)
    print(ConvertOperations.head(df, -1))
    
    # Información de tipos de datos
    print("\n" + "="*60)
    print("INFORMACIÓN DE TIPOS DE DATOS FINALES")
    print("="*60)
    print(df.dtypes)

if __name__ == "__main__":
    main()