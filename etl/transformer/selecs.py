import pandas as pd
from tabulate import tabulate
from typing import Union, List, Set, Tuple, Callable, Any, Optional


class DataSelect:
    """
    Clase para realizar operaciones de selección y filtrado de datos en DataFrames de pandas.
    
    Esta clase proporciona métodos estáticos para filtrar, seleccionar y visualizar
    datos de manera eficiente y con validaciones incorporadas.
    
    Attributes:
        No contiene atributos de instancia ya que todos los métodos son estáticos.
    """
    
    @staticmethod
    def head2(df: pd.DataFrame, n: int = 5, print_result: bool = True) -> str:
        """
        Devuelve o imprime las primeras n filas del DataFrame con formato tabular.
        
        Args:
            df (pd.DataFrame): DataFrame del cual obtener las primeras filas.
            n (int, optional): Número de filas a mostrar. Por defecto 5.
            print_result (bool, optional): Si True, imprime el resultado. Por defecto True.
        
        Returns:
            str: Representación tabular de las primeras n filas.
        
        Raises:
            TypeError: Si df no es un DataFrame de pandas.
            ValueError: Si n no es un entero positivo.
            Exception: Para cualquier otro error durante la operación.
        
        Examples:
            >>> result = DataSelect.head2(df, 3)
            >>> print(result)
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")
            if not isinstance(n, int) or n <= 0:
                raise ValueError("El parámetro 'n' debe ser un entero positivo")
            
            df_head = df.head(n)
            result = tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
            
            if print_result:
                print(result)
            
            return result
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}")
            raise

    @staticmethod
    def _validate_common(df: pd.DataFrame, field: str, complement: bool, show: int) -> None:
        """
        Valida parámetros comunes para los métodos de filtrado.
        
        Args:
            df (pd.DataFrame): DataFrame a validar.
            field (str): Nombre de la columna a validar.
            complement (bool): Indica si se debe invertir la selección.
            show (int): Número de filas a mostrar después del filtrado.
        
        Raises:
            TypeError: Si los tipos de parámetros son incorrectos.
            ValueError: Si los valores de parámetros son inválidos.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")
        if not isinstance(field, str):
            raise TypeError("El parámetro 'field' debe ser un string")
        if not isinstance(complement, bool):
            raise TypeError("El parámetro 'complement' debe ser un booleano")
        if not isinstance(show, int) or show < -1:
            raise ValueError("El parámetro 'show' debe ser un entero mayor o igual a -1")
        if field not in df.columns:
            raise ValueError(f"El campo '{field}' no existe en el DataFrame")

    @staticmethod
    def filter_by_operation(df: pd.DataFrame, field: str, value: Any, 
                           op: Callable[[Any, Any], bool], complement: bool = False, 
                           show: int = 0) -> pd.DataFrame:
        """
        Filtra filas aplicando una operación lógica personalizada.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna sobre la cual aplicar la operación.
            value (Any): Valor con el cual se compara.
            op (Callable): Función que toma dos argumentos y devuelve True/False.
            complement (bool, optional): Si True, devuelve filas donde la condición NO se cumple.
            show (int, optional): Número de filas a mostrar (0 = no mostrar).
        
        Returns:
            pd.DataFrame: DataFrame filtrado según la operación especificada.
        
        Raises:
            TypeError: Si los parámetros tienen tipos incorrectos.
            ValueError: Si el campo no existe en el DataFrame.
            Exception: Para cualquier otro error durante la operación.
        
        Examples:
            >>> # Filtrar donde la edad es mayor a 30
            >>> filtered = DataSelect.filter_by_operation(df, 'age', 30, lambda x, y: x > y)
        """
        try:
            DataSelect._validate_common(df, field, complement, show)
            if not callable(op):
                raise TypeError("El parámetro 'op' debe ser una función callable")

            mask = df[field].apply(lambda x: op(x, value))
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows_to_show = show if show > 0 else len(result)
                DataSelect.head2(result, rows_to_show)

            return result.copy()  # Retornar copia para evitar modificar el original
        except Exception as e:
            print(f"Error en filter_by_operation: {e}")
            raise

    @staticmethod
    def filter_equal(df: pd.DataFrame, field: str, value: Any, 
                    complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor en 'field' es igual a 'value'.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            value (Any): Valor de comparación.
            complement (bool, optional): Si True, devuelve filas donde NO son iguales.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Filtrar usuarios con género 'Femenino'
            >>> mujeres = DataSelect.filter_equal(df, 'gender', 'Femenino')
        """
        try:
            DataSelect._validate_common(df, field, complement, show)

            mask = df[field] == value
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows_to_show = show if show > 0 else len(result)
                DataSelect.head2(result, rows_to_show)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_equal: {e}")
            raise

    @staticmethod
    def filter_not_equal(df: pd.DataFrame, field: str, value: Any, 
                        complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor en 'field' NO es igual a 'value'.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            value (Any): Valor de comparación.
            complement (bool, optional): Si True, devuelve filas donde SON iguales.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Excluir usuarios con edad 25
            >>> sin_25 = DataSelect.filter_not_equal(df, 'age', 25)
        """
        return DataSelect.filter_equal(df, field, value, not complement, show)

    @staticmethod
    def filter_in_range(df: pd.DataFrame, field: str, minv: Union[int, float], 
                       maxv: Union[int, float], complement: bool = False, 
                       show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor del campo está dentro del rango [minv, maxv].
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            minv (int|float): Valor mínimo del rango (inclusive).
            maxv (int|float): Valor máximo del rango (inclusive).
            complement (bool, optional): Si True, devuelve filas FUERA del rango.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Filtrar edades entre 18 y 65 años
            >>> adultos = DataSelect.filter_in_range(df, 'age', 18, 65)
        """
        try:
            DataSelect._validate_common(df, field, complement, show)
            
            if not (isinstance(minv, (int, float)) and isinstance(maxv, (int, float))):
                raise TypeError("Los parámetros 'minv' y 'maxv' deben ser numéricos")
            if minv > maxv:
                raise ValueError("'minv' no puede ser mayor que 'maxv'")

            mask = (df[field] >= minv) & (df[field] <= maxv)
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows_to_show = show if show > 0 else len(result)
                DataSelect.head2(result, rows_to_show)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_in_range: {e}")
            raise

    @staticmethod
    def filter_contains(df: pd.DataFrame, field: str, value: str, 
                       complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor de 'field' contiene la cadena 'value'.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            value (str): Subcadena que debe contener el valor del campo.
            complement (bool, optional): Si True, devuelve filas que NO contienen el valor.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Filtrar emails que contienen '@gmail.com'
            >>> gmail_users = DataSelect.filter_contains(df, 'email', '@gmail.com')
        """
        try:
            DataSelect._validate_common(df, field, complement, show)
            if not isinstance(value, str):
                raise TypeError("El parámetro 'value' debe ser un string")

            mask = df[field].astype(str).str.contains(value, na=False, regex=False)
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows_to_show = show if show > 0 else len(result)
                DataSelect.head2(result, rows_to_show)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_contains: {e}")
            raise

    @staticmethod
    def filter_in_list(df: pd.DataFrame, field: str, values: Union[List, Set, Tuple], 
                      complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor en 'field' está en la lista de valores.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            values (list|set|tuple): Colección de valores aceptados.
            complement (bool, optional): Si True, devuelve filas que NO están en la lista.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Filtrar por múltiples categorías
            >>> categorias = ['Electrónicos', 'Ropa', 'Hogar']
            >>> filtrado = DataSelect.filter_in_list(df, 'category', categorias)
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")
            if not isinstance(values, (list, set, tuple)):
                raise ValueError("El parámetro 'values' debe ser una lista, conjunto o tupla")

            mask = df[field].isin(values)
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows_to_show = show if show > 0 else len(result)
                DataSelect.head2(result, rows_to_show)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_in_list: {e}")
            raise

    @staticmethod
    def filter_is_null(df: pd.DataFrame, field: str, 
                      complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor del campo es NaN o None.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            complement (bool, optional): Si True, devuelve filas que NO son nulas.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Encontrar registros con email nulo
            >>> sin_email = DataSelect.filter_is_null(df, 'email')
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")

            mask = df[field].isna()
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows_to_show = show if show > 0 else len(result)
                DataSelect.head2(result, rows_to_show)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_is_null: {e}")
            raise

    @staticmethod
    def filter_not_null(df: pd.DataFrame, field: str, 
                       complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Filtra filas donde el valor del campo NO es NaN o None.
        
        Args:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna a evaluar.
            complement (bool, optional): Si True, devuelve filas que SON nulas.
            show (int, optional): Número de filas a mostrar.
        
        Returns:
            pd.DataFrame: DataFrame filtrado.
        
        Examples:
            >>> # Filtrar registros con email válido
            >>> con_email = DataSelect.filter_not_null(df, 'email')
        """
        return DataSelect.filter_is_null(df, field, not complement, show)

    @staticmethod
    def unique_values(df: pd.DataFrame, field: str, 
                     show: bool = False) -> List[Any]:
        """
        Devuelve una lista con valores únicos del campo indicado.
        
        Args:
            df (pd.DataFrame): DataFrame de origen.
            field (str): Columna de la cual obtener valores únicos.
            show (bool, optional): Si True, imprime todos los valores únicos.
        
        Returns:
            List[Any]: Lista con los valores únicos (sin NaN).
        
        Examples:
            >>> # Obtener categorías únicas
            >>> categorias = DataSelect.unique_values(df, 'category', show=True)
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")
            if not isinstance(field, str):
                raise TypeError("El parámetro 'field' debe ser un string")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")

            unique_vals = df[field].dropna().unique().tolist()

            if show:
                unique_df = pd.DataFrame({field: unique_vals})
                DataSelect.head2(unique_df, len(unique_df))

            return unique_vals
        except Exception as e:
            print(f"Error al obtener valores únicos: {e}")
            raise

    @staticmethod
    def select_columns(df: pd.DataFrame, *columns: str, 
                      complement: bool = False, show: int = 0) -> pd.DataFrame:
        """
        Selecciona o excluye columnas específicas del DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame de origen.
            *columns (str): Nombres de columnas a seleccionar o excluir.
            complement (bool, optional): Si True, excluye las columnas indicadas.
            show (int, optional): Número de filas a mostrar del resultado.
        
        Returns:
            pd.DataFrame: DataFrame con columnas seleccionadas o excluidas.
        
        Examples:
            >>> # Seleccionar solo nombre y edad
            >>> reducido = DataSelect.select_columns(df, 'name', 'age')
            
            >>> # Excluir columnas sensibles
            >>> seguro = DataSelect.select_columns(df, 'password', 'ssn', complement=True)
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")

            # Validar que las columnas existan
            for col in columns:
                if col not in df.columns:
                    raise ValueError(f"La columna '{col}' no existe en el DataFrame")

            if complement:
                result_df = df.drop(columns=list(columns))
            else:
                result_df = df[list(columns)]

            if show != 0:
                rows_to_show = show if show > 0 else len(result_df)
                DataSelect.head2(result_df, rows_to_show)

            return result_df.copy()
        except Exception as e:
            print(f"Error al seleccionar columnas: {e}")
            raise


# Ejemplo de uso y documentación adicional
if __name__ == "__main__":
    """
    Ejemplo de uso de la clase DataSelect:
    
    # Crear DataFrame de ejemplo
    data = {
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'email': ['alice@email.com', 'bob@mail.com', None, 'david@email.com', 'eve@mail.com'],
        'department': ['IT', 'HR', 'IT', 'Finance', 'HR']
    }
    df = pd.DataFrame(data)
    
    # Ejemplos de uso:
    print("Primeras 3 filas:")
    DataSelect.head2(df, 3)
    
    print("\\nEmpleados de IT:")
    it_employees = DataSelect.filter_equal(df, 'department', 'IT', show=5)
    
    print("\\nEmpleados entre 30 y 40 años:")
    age_range = DataSelect.filter_in_range(df, 'age', 30, 40, show=5)
    
    print("\\nEmails que contienen 'email':")
    email_filter = DataSelect.filter_contains(df, 'email', 'email', show=5)
    
    print("\\nValores únicos de department:")
    depts = DataSelect.unique_values(df, 'department', show=True)
    """
    pass