import pandas as pd
from tabulate import tabulate
from etl.transformer.basics_data_transformer import BasicsTransformOperations


class DataSelect:
    # Asignamos la clase BasicsTransformOperations para usar sus operaciones si se requiere
    hd = BasicsTransformOperations

    @staticmethod
    def head2(df, n=5, print_result=True):
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
    def _validate_common(df, field, complement, show):
        """
        Función auxiliar para validar parámetros comunes en los filtros.
        Parámetros:
            df (pd.DataFrame): DataFrame a validar.
            field (str): Nombre de la columna a validar.
            complement (bool): Si se debe invertir la selección.
            show (int): Número de filas a mostrar después del filtrado.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df debe ser un DataFrame de pandas")
        if not isinstance(field, str):
            raise TypeError("field debe ser un string")
        if not isinstance(complement, bool):
            raise TypeError("complement debe ser un booleano")
        if not isinstance(show, int) or show < -1:
            raise ValueError("show debe ser un entero mayor o igual a -1")
        if field not in df.columns:
            raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

    @staticmethod
    def filter_by_operation(df, field, value, op, complement=False, show=0):
        """
        Filtra filas aplicando una operación lógica personalizada.
        Parámetros:
            df (pd.DataFrame): DataFrame a filtrar.
            field (str): Columna del DataFrame sobre la cual aplicar la operación.
            value (cualquier): Valor con el cual se compara.
            op (callable): Función que toma dos argumentos y devuelve True/False.
            complement (bool): Si True, devuelve filas donde la condición NO se cumple.
            show (int): Cuántas filas mostrar del resultado (0 = no mostrar).
        Retorna:
            pd.DataFrame: DataFrame filtrado.
        """
        try:
            # Validamos los parámetros comunes y que op sea callable
            DataSelect._validate_common(df, field, complement, show)
            if not callable(op):
                raise TypeError("op debe ser una función callable")

            # Aplicamos la función op fila por fila
            mask = df[field].apply(lambda x: op(x, value))
            # Si complement es True, invertimos la máscara
            result = df[~mask] if complement else df[mask]

            # Si show es distinto de 0, mostramos las primeras filas del resultado
            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_op: {e}")
            raise

    @staticmethod
    def filter_equal(df, field, value, complement=False, show=0):
        """
        Filtra filas donde el valor en 'field' es igual a 'value'.
        Parámetros y retornos similares a filter_by_operation.
        """
        try:
            DataSelect._validate_common(df, field, complement, show)

            # Creamos máscara booleana donde el campo es igual al valor dado
            mask = df[field] == value
            # Invertimos si complement es True
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_eq: {e}")
            raise

    @staticmethod
    def filter_not_equal(df, field, value, complement=False, show=0):
        """
        Filtra filas donde el valor en 'field' NO es igual a 'value'.
        Aquí simplemente usamos filter_equal pero invertimos el parámetro complement.
        """
        # Complementamos complement para invertir la selección en filter_equal
        return DataSelect.filter_equal(df, field, value, not complement, show)

    @staticmethod
    def filter_in_range(df, field, minv, maxv, complement=False, show=0):
        """
        Filtra filas donde el valor del campo está dentro del rango [minv, maxv].
        Parámetros:
            minv (int|float): Valor mínimo del rango.
            maxv (int|float): Valor máximo del rango.
        """
        try:
            DataSelect._validate_common(df, field, complement, show)
            # Validamos que minv y maxv sean numéricos
            if not (isinstance(minv, (int, float)) and isinstance(maxv, (int, float))):
                raise TypeError("minv y maxv deben ser numéricos")

            # Máscara donde los valores están dentro del rango (inclusive)
            mask = (df[field] >= minv) & (df[field] <= maxv)
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_range_open: {e}")
            raise

    @staticmethod
    def filter_contains(df, field, value, complement=False, show=0):
        """
        Filtra filas donde el valor de 'field' contiene la cadena 'value'.
        Parámetros:
            value (str): Subcadena que debe contener el valor del campo.
        """
        try:
            DataSelect._validate_common(df, field, complement, show)
            if not isinstance(value, str):
                raise TypeError("value debe ser un string")

            # Convertimos todo a string para evitar errores y aplicamos contains
            mask = df[field].astype(str).str.contains(value, na=False)
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_contains: {e}")
            raise

    @staticmethod
    def filter_in_list(df, field, values, complement=False, show=0):
        """
        Filtra filas donde el valor en 'field' está dentro de una lista (o set, tupla) de valores.
        Parámetros:
            values (list|set|tuple): Colección de valores aceptados.
        """
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")
            if not isinstance(values, (list, set, tuple)):
                raise ValueError("El parámetro 'values' debe ser una lista, conjunto o tupla.")

            mask = df[field].isin(values)
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_in: {e}")
            raise

    @staticmethod
    def filter_is_null(df, field, complement=False, show=0):
        """
        Filtra filas donde el valor del campo es NaN o None.
        """
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            mask = df[field].isna()
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_none: {e}")
            raise

    @staticmethod
    def select_not_none(df, field, complement=False, show=0):
        """
        Filtra filas donde el valor del campo NO es NaN o None.
        """
        try:
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            mask = df[field].notna()
            result = df[~mask] if complement else df[mask]

            if show:
                print(DataSelect.head2(result, show if show > 0 else len(result)))

            return result
        except Exception as e:
            print(f"Error en select_not_none: {e}")
            raise

    @staticmethod
    def unique_values(df, field, show=False):
        """
        Devuelve una lista con valores únicos del campo indicado.
        Parámetros:
            show (bool): Si es True, imprime todos los valores únicos.
        Retorna:
            list: Lista con los valores únicos (sin NaN).
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(field, str):
                raise TypeError("field debe ser un string")
            if field not in df.columns:
                raise ValueError(f"El campo '{field}' no existe en el DataFrame.")

            unique_vals_df = pd.DataFrame({field: df[field].dropna().unique()})

            if show:
                print(DataSelect.head2(unique_vals_df, len(unique_vals_df)))

            return unique_vals_df[field].tolist()
        except Exception as e:
            print(f"Error al obtener valores únicos: {e}")
            raise

    @staticmethod
    def select_columns(df, *columns, complement=False, show=0):
        """
        Selecciona columnas específicas del DataFrame o excluye si complement es True.
        Parámetros:
            *columns (str): Nombres de columnas a seleccionar o excluir.
            complement (bool): Si True, se excluyen las columnas indicadas.
            show (int): Cantidad de filas a mostrar del DataFrame resultante.
        Retorna:
            pd.DataFrame: DataFrame con columnas seleccionadas o excluidas.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")

            # Seleccionamos o excluimos columnas según complement
            if complement:
                result_df = df[[col for col in df.columns if col not in columns]]
            else:
                result_df = df[list(columns)]

            if show:
                print(DataSelect.head2(result_df, show if show > 0 else len(result_df)))

            return result_df
        except Exception as e:
            print(f"Error al seleccionar/reordenar columnas: {e}")
            raise
