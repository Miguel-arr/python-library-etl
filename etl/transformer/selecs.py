import pandas as pd
from tabulate import tabulate
from typing import Union, List, Set, Tuple, Callable, Any, Optional

class DataSelect:
    """
    Clase para realizar operaciones de selección y filtrado de datos en DataFrames de pandas.
    Todos los métodos son estáticos, con validaciones internas, manejo de errores
    y opción de visualización controlada.
    """

    # ======================================================
    #  VISUALIZACIÓN
    # ======================================================
    @staticmethod
    def head(df, n=5, print_result=True):
        """Muestra las primeras n filas del DataFrame con formato tabular."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(n, int) or n <= 0:
                raise ValueError("n debe ser un entero positivo")

            df_head = df.head(n)
            result = tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
            if print_result:
                print(result)
            return result
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}")
            raise

    # ======================================================
    #  FILTROS GENERALES
    # ======================================================
    @staticmethod
    def filter_by_operation(df, field, value, op, complement=False, show=0):
        """Filtra filas aplicando una operación lógica personalizada."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")
            if not callable(op):
                raise TypeError("op debe ser una función callable")

            mask = df[field].apply(lambda x: op(x, value))
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_by_operation: {e}")
            raise

    @staticmethod
    def filter_equal(df, field, value, complement=False, show=0):
        """Filtra filas donde el valor del campo es igual al valor dado."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")

            mask = df[field] == value
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_equal: {e}")
            raise

    @staticmethod
    def filter_not_equal(df, field, value, complement=False, show=0):
        """Filtra filas donde el valor del campo NO es igual al valor dado."""
        try:
            return DataSelect.filter_equal(df, field, value, not complement, show)
        except Exception as e:
            print(f"Error en filter_not_equal: {e}")
            raise

    @staticmethod
    def filter_in_range(df, field, minv, maxv, complement=False, show=0):
        """Filtra filas donde el valor del campo está dentro del rango [minv, maxv]."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")
            if not (isinstance(minv, (int, float)) and isinstance(maxv, (int, float))):
                raise TypeError("minv y maxv deben ser numéricos")
            if minv > maxv:
                raise ValueError("minv no puede ser mayor que maxv")

            mask = (df[field] >= minv) & (df[field] <= maxv)
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_in_range: {e}")
            raise

    @staticmethod
    def filter_contains(df, field, value, complement=False, show=0):
        """Filtra filas donde el campo contiene la subcadena dada."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")
            if not isinstance(value, str):
                raise TypeError("value debe ser un string")

            mask = df[field].astype(str).str.contains(value, na=False, regex=False)
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_contains: {e}")
            raise

    @staticmethod
    def filter_in_list(df, field, values, complement=False, show=0):
        """Filtra filas donde el valor del campo está en una lista o conjunto."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")
            if not isinstance(values, (list, set, tuple)):
                raise TypeError("values debe ser una lista, conjunto o tupla")

            mask = df[field].isin(values)
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_in_list: {e}")
            raise

    @staticmethod
    def filter_is_null(df, field, complement=False, show=0):
        """Filtra filas donde el campo es nulo (NaN o None)."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")

            mask = df[field].isna()
            result = df[~mask] if complement else df[mask]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error en filter_is_null: {e}")
            raise



    # ======================================================
    #  SELECCIÓN Y VALORES ÚNICOS
    # ======================================================
    @staticmethod
    def unique_values(df, field, show=False):
        """Devuelve los valores únicos del campo indicado."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame")

            unique_vals = df[field].dropna().unique().tolist()

            if show:
                unique_df = pd.DataFrame({field: unique_vals})
                DataSelect.head(unique_df, len(unique_df))

            return unique_vals
        except Exception as e:
            print(f"Error al obtener valores únicos: {e}")
            raise

    @staticmethod
    def select_columns(df, *columns, complement=False, show=0):
        """Selecciona o excluye columnas del DataFrame."""
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            for col in columns:
                if col not in df.columns:
                    raise ValueError(f"La columna '{col}' no existe en el DataFrame")

            result = df.drop(columns=list(columns)) if complement else df[list(columns)]

            if show != 0:
                rows = show if show > 0 else len(result)
                DataSelect.head(result, rows)

            return result.copy()
        except Exception as e:
            print(f"Error al seleccionar columnas: {e}")
            raise

    @staticmethod
    def select_not_none(df, field, complement=False, show=0):
        """Filtra filas donde el campo NO es nulo."""
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            mask = df[field].notna()
            result = df[~mask] if complement else df[mask]

            if show:
                rows = show if show > 0 else len(result)
                DataSelect.head2(result, rows)

            return result
        except Exception as e:
            print(f"Error en select_not_none: {e}")
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
    DataSelect.head(df, 3)
    
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