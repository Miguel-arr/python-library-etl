import pandas as pd
from tabulate import tabulate

class TransformOperations:
    """
    Clase con métodos estáticos para realizar operaciones comunes de transformación y combinación
    en DataFrames de pandas, con soporte para mostrar resultados tabulados.
    """

    @staticmethod
    def head(df, n=5):
        """
        Muestra las primeras n filas de un DataFrame formateadas como tabla.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - n (int, opcional): Número de filas a mostrar (por defecto 5).

        Retorna:
        - str: Tabla en formato texto con las primeras n filas.

        Lanza:
        - TypeError si df no es DataFrame.
        - ValueError si n no es entero positivo.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(n, int) or n < 0:
                raise ValueError("n debe ser un entero positivo")
            df_head = df.head(n)
            return tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}")
            raise

    @staticmethod
    def left_join(df1, df2, on, show = 0):
        """
        Realiza una unión LEFT JOIN entre dos DataFrames según las columnas indicadas.

        Parámetros:
        - df1 (pd.DataFrame): DataFrame izquierdo.
        - df2 (pd.DataFrame): DataFrame derecho.
        - on (str, list o tupla): Columna(s) clave para hacer el join.
          Si es tupla, debe ser (columna_df1, columna_df2) para claves distintas.
        - show (int, opcional): Cantidad de filas a mostrar del resultado.
          0 = no mostrar, -1 = mostrar todo, >0 mostrar primeras n filas.

        Retorna:
        - pd.DataFrame: DataFrame resultante de la unión LEFT JOIN.

        Lanza:
        - TypeError si los parámetros no tienen tipos adecuados.
        """
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not (isinstance(on, str) or isinstance(on, list) or isinstance(on, tuple)):
                raise TypeError("on debe ser una cadena, lista o tupla")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            if isinstance(on, tuple):
                # Join con claves diferentes para df1 y df2
                result_df = pd.merge(df1, df2, how='left', left_on=on[0], right_on=on[1])
            else:
                # Join con claves iguales
                result_df = pd.merge(df1, df2, how='left', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df
        except Exception as e:
            print(f"Error al realizar el left join: {e}")
            raise

    @staticmethod
    def right_join(df1, df2, on, show=0):
        """
        Realiza una unión RIGHT JOIN entre dos DataFrames según las columnas indicadas.

        Parámetros:
        - df1, df2 (pd.DataFrame): DataFrames a unir.
        - on (str o list): Columnas clave para la unión.
        - show (int, opcional): Filas a mostrar (0=ninguna, -1=todo, >0 primeras n filas).

        Retorna:
        - pd.DataFrame con la unión RIGHT JOIN.

        Lanza errores si los tipos no coinciden.
        """
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not (isinstance(on, str) or isinstance(on, list)):
                raise TypeError("on debe ser una cadena o lista")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            result_df = pd.merge(df1, df2, how='right', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df
        except Exception as e:
            print(f"Error al realizar el right join: {e}")
            raise

    @staticmethod
    def inner_join(df1, df2, on, show=0):
        """
        Realiza una unión INNER JOIN entre dos DataFrames según las columnas indicadas.

        Parámetros:
        - df1, df2 (pd.DataFrame): DataFrames de entrada.
        - on (str o list): Columnas clave para la unión.
        - show (int): Control de filas mostradas.

        Retorna:
        - pd.DataFrame con las filas que coinciden en ambos DataFrames.
        """
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not (isinstance(on, str) or isinstance(on, list)):
                raise TypeError("on debe ser una cadena o lista")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            result_df = pd.merge(df1, df2, how='inner', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df
        except Exception as e:
            print(f"Error al realizar el inner join: {e}")
            raise

    @staticmethod
    def outer_join(df1, df2, on, show=0):
        """
        Realiza una unión OUTER JOIN (unión completa) entre dos DataFrames.

        Parámetros:
        - df1, df2 (pd.DataFrame): DataFrames a unir.
        - on (str o list): Columnas clave.
        - show (int): Control de visualización.

        Retorna:
        - pd.DataFrame con la unión completa.
        """
        try:
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not (isinstance(on, str) or isinstance(on, list)):
                raise TypeError("on debe ser una cadena o lista")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            result_df = pd.merge(df1, df2, how='outer', on=on)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df
        except Exception as e:
            print(f"Error al realizar el outer join: {e}")
            raise

    @staticmethod
    def group_by_sum(df, by, column, show=0):
        """
        Agrupa el DataFrame por una o varias columnas y suma los valores de una columna específica.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - by (str o list): Columnas para agrupar.
        - column (str): Columna cuyos valores se suman.
        - show (int): Control de visualización de resultados.

        Retorna:
        - pd.DataFrame agrupado con la suma por grupo.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not (isinstance(by, str) or isinstance(by, list)):
                raise TypeError("by debe ser una cadena o lista")
            if not isinstance(column, str):
                raise TypeError("column debe ser una cadena")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            grouped_df = df.groupby(by)[column].sum().reset_index()

            if show > 0:
                print(TransformOperations.head(grouped_df, show))
            elif show == -1:
                print(TransformOperations.head(grouped_df, len(grouped_df)))

            return grouped_df
        except Exception as e:
            print(f"Error al agrupar y sumar: {e}")
            raise

    @staticmethod
    def apply_to_column(df, column, func, show=0):
        """
        Aplica una función a una columna específica del DataFrame.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - column (str): Nombre de la columna sobre la que aplicar la función.
        - func (callable): Función a aplicar.
        - show (int): Control de visualización.

        Retorna:
        - pd.DataFrame modificado.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(column, str):
                raise TypeError("column debe ser una cadena")
            if not callable(func):
                raise TypeError("func debe ser una función")

            df[column] = df[column].apply(func)

            if show > 0:
                print(TransformOperations.head(df, show))
            elif show == -1:
                print(TransformOperations.head(df, len(df)))

            return df
        except Exception as e:
            print(f"Error al aplicar función a la columna: {e}")
            raise

    @staticmethod
    def sort_by(df, columns, ascending=True, show=0):
        """
        Ordena un DataFrame por una o varias columnas.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada a ordenar.
        - columns (str o list): Nombre(s) de columna(s) para ordenar.
        - ascending (bool o list, opcional): Orden ascendente (True) o descendente (False).
          Puede ser un booleano único o una lista que corresponda a cada columna.
          Por defecto True.
        - show (int, opcional): Control de impresión:
          0 = no imprimir,
          >0 imprimir las primeras n filas,
          -1 imprimir todo.

        Retorna:
        - pd.DataFrame ordenado según las columnas indicadas.

        Lanza:
        - TypeError si los argumentos no tienen el tipo esperado.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not (isinstance(columns, str) or isinstance(columns, list)):
                raise TypeError("columns debe ser una cadena o una lista de cadenas")
            if not isinstance(ascending, (bool, list)):
                raise TypeError("ascending debe ser un booleano o lista de booleanos")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            sorted_df = df.sort_values(by=columns, ascending=ascending)

            if show > 0:
                print(TransformOperations.head(sorted_df, show))
            elif show == -1:
                print(TransformOperations.head(sorted_df, len(sorted_df)))

            return sorted_df

        except Exception as e:
            print(f"Error al ordenar las filas: {e}")
            raise

    @staticmethod
    def drop_duplicates(df, subset=None, show=0):
        """
        Elimina filas duplicadas en un DataFrame.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - subset (str, list o None): Columnas para identificar duplicados.
          Si es None (por defecto), se usan todas las columnas.
        - show (int): Control de impresión (igual que en sort_by).

        Retorna:
        - pd.DataFrame sin filas duplicadas según el subset indicado.

        Lanza:
        - TypeError si los tipos de parámetros no son correctos.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if subset is not None and not (isinstance(subset, str) or isinstance(subset, list)):
                raise TypeError("subset debe ser None, una cadena o una lista")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            df_no_duplicates = df.drop_duplicates(subset=subset)

            if show > 0:
                print(TransformOperations.head(df_no_duplicates, show))
            elif show == -1:
                print(TransformOperations.head(df_no_duplicates, len(df_no_duplicates)))

            return df_no_duplicates

        except Exception as e:
            print(f"Error al eliminar duplicados: {e}")
            raise

    @staticmethod
    def replace_values(df, column, old_value, new_value, show=0):
        """
        Reemplaza valores en una columna específica del DataFrame.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - column (str): Nombre de la columna donde se reemplazan valores.
        - old_value: Valor o lista de valores a reemplazar.
        - new_value: Nuevo valor o lista de valores con los que se reemplaza.
        - show (int): Control de impresión.

        Retorna:
        - pd.DataFrame con los valores reemplazados en la columna indicada.

        Lanza:
        - TypeError si los tipos de argumentos no son los esperados.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(column, str):
                raise TypeError("column debe ser una cadena")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            df[column] = df[column].replace(old_value, new_value)

            if show > 0:
                print(TransformOperations.head(df, show))
            elif show == -1:
                print(TransformOperations.head(df, len(df)))

            return df

        except Exception as e:
            print(f"Error al reemplazar valores en la columna: {e}")
            raise

    @staticmethod
    def union_all(dfs, show=0):
        """
        Une (concatena) una lista de DataFrames verticalmente.

        Parámetros:
        - dfs (list de pd.DataFrame): Lista de DataFrames a unir.
        - show (int): Control de impresión.

        Retorna:
        - pd.DataFrame resultante de concatenar todos los DataFrames de la lista.

        Lanza:
        - TypeError si dfs no es lista o si alguno no es DataFrame.
        """
        try:
            if not isinstance(dfs, list) or not all(isinstance(df, pd.DataFrame) for df in dfs):
                raise TypeError("dfs debe ser una lista de DataFrames")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            result_df = pd.concat(dfs, ignore_index=True)

            if show > 0:
                print(TransformOperations.head(result_df, show))
            elif show == -1:
                print(TransformOperations.head(result_df, len(result_df)))

            return result_df

        except Exception as e:
            print(f"Error al realizar el union all: {e}")
            raise

    @staticmethod
    def group_by_count(df, by, show=0):
        """
        Agrupa el DataFrame por una o más columnas y cuenta filas por grupo.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - by (str o list): Columna(s) para agrupar.
        - show (int): Control de impresión.

        Retorna:
        - pd.DataFrame con las columnas de agrupación y una columna 'conteo'
          con el número de filas por grupo.

        Lanza:
        - TypeError si los tipos no son correctos.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not (isinstance(by, str) or isinstance(by, list)):
                raise TypeError("by debe ser una cadena o lista de cadenas")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            grouped_df = df.groupby(by).size().reset_index(name='conteo')

            if show > 0:
                print(TransformOperations.head(grouped_df, show))
            elif show == -1:
                print(TransformOperations.head(grouped_df, len(grouped_df)))

            return grouped_df

        except Exception as e:
            print(f"Error al agrupar y contar: {e}")
            raise


    @staticmethod
    def group_by_mean(df, by, column, show=0):
        """
        Agrupa el DataFrame por una o varias columnas y calcula el promedio de una columna específica.

        Parámetros:
        - df (pd.DataFrame): DataFrame de entrada.
        - by (str o list): Columnas para agrupar.
        - column (str): Columna cuyos valores se promedian.
        - show (int): Control de visualización de resultados.

        Retorna:
        - pd.DataFrame agrupado con el promedio por grupo.
        """
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not (isinstance(by, str) or isinstance(by, list)):
                raise TypeError("by debe ser una cadena o lista")
            if not isinstance(column, str):
                raise TypeError("column debe ser una cadena")
            if not isinstance(show, int):
                raise TypeError("show debe ser un número entero")

            grouped_df = df.groupby(by)[column].mean().reset_index()

            if show > 0:
                print(TransformOperations.head(grouped_df, show))
            elif show == -1:
                print(TransformOperations.head(grouped_df, len(grouped_df)))

            return grouped_df
        except Exception as e:
            print(f"Error al agrupar y promediar: {e}")
            raise

