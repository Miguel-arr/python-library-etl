import pandas as pd
from tabulate import tabulate

class TransformOperations:
    """
    Clase con métodos estáticos para realizar operaciones comunes de transformación y combinación
    en DataFrames de pandas, con soporte para mostrar resultados tabulados.
    
    Esta clase proporciona una interfaz simplificada para operaciones comunes de manipulación
    de datos, incluyendo joins, agrupaciones, ordenamientos y transformaciones de columnas.
    Todos los métodos incluyen manejo de errores y opciones de visualización.
    """

    @staticmethod
    def head(df, n=5):
        """
        Muestra las primeras n filas de un DataFrame formateadas como tabla.
        
        Este método es útil para obtener una vista previa rápida de los datos
        en un formato legible y tabulado.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de pandas del cual se mostrarán las primeras filas.
        n : int, opcional
            Número de filas a mostrar. Por defecto es 5.

        Retorna:
        --------
        str
            Representación en formato de tabla de las primeras n filas.

        Lanza:
        ------
        TypeError
            Si df no es un DataFrame de pandas.
        ValueError
            Si n no es un entero positivo.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        >>> print(TransformOperations.head(df, 2))
        ╒═════╤═════╕
        │   A │   B │
        ╞═════╪═════╡
        │   1 │   4 │
        ├─────┼─────┤
        │   2 │   5 │
        ╘═════╧═════╛
        """
        try:
            # Validación de tipos de entrada
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El parámetro 'df' debe ser un DataFrame de pandas")
            if not isinstance(n, int) or n < 0:
                raise ValueError("El parámetro 'n' debe ser un entero positivo")
            
            # Obtener las primeras n filas del DataFrame
            df_head = df.head(n)
            
            # Formatear y retornar la tabla
            return tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
        
        except Exception as e:
            print(f"Error al obtener las primeras {n} filas: {e}")
            raise

    @staticmethod
    def left_join(df1, df2, on, show=0):
        """
        Realiza una unión LEFT JOIN entre dos DataFrames según las columnas indicadas.
        
        Un LEFT JOIN mantiene todas las filas del DataFrame izquierdo (df1) y 
        combina con las filas coincidentes del DataFrame derecho (df2). Donde no
        hay coincidencias, se llenan con valores NaN.

        Parámetros:
        -----------
        df1 : pd.DataFrame
            DataFrame izquierdo (se mantienen todas sus filas).
        df2 : pd.DataFrame
            DataFrame derecho (solo filas coincidentes).
        on : str, list o tuple
            - Si es str: Nombre de la columna común para hacer el join.
            - Si es list: Lista de nombres de columnas comunes.
            - Si es tuple: (columna_df1, columna_df2) para claves con nombres diferentes.
        show : int, opcional
            Control de visualización del resultado:
            - 0: No mostrar resultado (por defecto)
            - >0: Mostrar las primeras n filas
            - -1: Mostrar todas las filas

        Retorna:
        --------
        pd.DataFrame
            DataFrame resultante de la unión LEFT JOIN.

        Lanza:
        ------
        TypeError
            Si los parámetros no tienen los tipos adecuados.

        Ejemplo:
        --------
        >>> df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
        >>> df2 = pd.DataFrame({'key': ['A', 'B'], 'value2': [10, 20]})
        >>> result = TransformOperations.left_join(df1, df2, on='key', show=2)
        ╒═══════╤══════════╤══════════╕
        │ key   │   value1 │   value2 │
        ╞═══════╪══════════╪══════════╡
        │ A     │        1 │       10 │
        ├───────┼──────────┼──────────┤
        │ B     │        2 │       20 │
        ╘═══════╧══════════╧══════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not isinstance(on, (str, list, tuple)):
                raise TypeError("'on' debe ser una cadena, lista o tupla")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Realizar el join según el tipo de parámetro 'on'
            if isinstance(on, tuple):
                # Join con claves diferentes para df1 y df2
                result_df = pd.merge(df1, df2, how='left', left_on=on[0], right_on=on[1])
            else:
                # Join con claves iguales
                result_df = pd.merge(df1, df2, how='left', on=on)

            # Mostrar resultado si se solicita
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
        
        Un RIGHT JOIN mantiene todas las filas del DataFrame derecho (df2) y 
        combina con las filas coincidentes del DataFrame izquierdo (df1).

        Parámetros:
        -----------
        df1 : pd.DataFrame
            DataFrame izquierdo (solo filas coincidentes).
        df2 : pd.DataFrame
            DataFrame derecho (se mantienen todas sus filas).
        on : str o list
            Nombre(s) de la(s) columna(s) común(es) para hacer el join.
        show : int, opcional
            Control de visualización (0=ninguna, -1=todo, >0 primeras n filas).

        Retorna:
        --------
        pd.DataFrame
            DataFrame con la unión RIGHT JOIN.

        Lanza:
        ------
        TypeError
            Si los tipos de parámetros no son correctos.

        Ejemplo:
        --------
        >>> df1 = pd.DataFrame({'key': ['A', 'B'], 'value1': [1, 2]})
        >>> df2 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value2': [10, 20, 30]})
        >>> result = TransformOperations.right_join(df1, df2, on='key', show=1)
        """
        try:
            # Validaciones de tipos
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not isinstance(on, (str, list)):
                raise TypeError("'on' debe ser una cadena o lista")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Realizar RIGHT JOIN
            result_df = pd.merge(df1, df2, how='right', on=on)

            # Mostrar resultado si se solicita
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
        
        Un INNER JOIN mantiene solo las filas que tienen coincidencias en ambos DataFrames.

        Parámetros:
        -----------
        df1, df2 : pd.DataFrame
            DataFrames de entrada para la unión.
        on : str o list
            Nombre(s) de la(s) columna(s) común(es) para hacer el join.
        show : int
            Control de visualización de filas.

        Retorna:
        --------
        pd.DataFrame
            DataFrame con las filas que coinciden en ambos DataFrames.

        Ejemplo:
        --------
        >>> df1 = pd.DataFrame({'key': ['A', 'B', 'C'], 'value1': [1, 2, 3]})
        >>> df2 = pd.DataFrame({'key': ['A', 'B', 'D'], 'value2': [10, 20, 40]})
        >>> result = TransformOperations.inner_join(df1, df2, on='key', show=-1)
        """
        try:
            # Validaciones de tipos
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not isinstance(on, (str, list)):
                raise TypeError("'on' debe ser una cadena o lista")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Realizar INNER JOIN
            result_df = pd.merge(df1, df2, how='inner', on=on)

            # Mostrar resultado si se solicita
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
        
        Un OUTER JOIN mantiene todas las filas de ambos DataFrames, combinando
        donde hay coincidencias y llenando con NaN donde no las hay.

        Parámetros:
        -----------
        df1, df2 : pd.DataFrame
            DataFrames a unir.
        on : str o list
            Nombre(s) de la(s) columna(s) común(es) para la unión.
        show : int
            Control de visualización.

        Retorna:
        --------
        pd.DataFrame
            DataFrame con la unión completa de ambos DataFrames.

        Ejemplo:
        --------
        >>> df1 = pd.DataFrame({'key': ['A', 'B'], 'value1': [1, 2]})
        >>> df2 = pd.DataFrame({'key': ['B', 'C'], 'value2': [20, 30]})
        >>> result = TransformOperations.outer_join(df1, df2, on='key', show=0)
        """
        try:
            # Validaciones de tipos
            if not isinstance(df1, pd.DataFrame):
                raise TypeError("df1 debe ser un DataFrame de pandas")
            if not isinstance(df2, pd.DataFrame):
                raise TypeError("df2 debe ser un DataFrame de pandas")
            if not isinstance(on, (str, list)):
                raise TypeError("'on' debe ser una cadena o lista")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Realizar OUTER JOIN
            result_df = pd.merge(df1, df2, how='outer', on=on)

            # Mostrar resultado si se solicita
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
        
        Esta operación es útil para obtener totales por categorías o grupos.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada para agrupar.
        by : str o list
            Columna(s) por las cuales agrupar los datos.
        column : str
            Columna cuyos valores numéricos se sumarán.
        show : int
            Control de visualización de resultados.

        Retorna:
        --------
        pd.DataFrame
            DataFrame agrupado con la suma por grupo.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({
        ...     'categoria': ['A', 'A', 'B', 'B', 'A'],
        ...     'ventas': [100, 200, 150, 50, 300]
        ... })
        >>> result = TransformOperations.group_by_sum(df, 'categoria', 'ventas', show=-1)
        ╒═════════════╤══════════╕
        │ categoria   │   ventas │
        ╞═════════════╪══════════╡
        │ A           │      600 │
        ├─────────────┼──────────┤
        │ B           │      200 │
        ╘═════════════╧══════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(by, (str, list)):
                raise TypeError("'by' debe ser una cadena o lista")
            if not isinstance(column, str):
                raise TypeError("'column' debe ser una cadena")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Agrupar y sumar
            grouped_df = df.groupby(by)[column].sum().reset_index()

            # Mostrar resultado si se solicita
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
        
        Permite transformar los valores de una columna usando cualquier función
        personalizada o built-in de Python.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada.
        column : str
            Nombre de la columna sobre la que aplicar la función.
        func : callable
            Función a aplicar a cada elemento de la columna.
        show : int
            Control de visualización del resultado.

        Retorna:
        --------
        pd.DataFrame
            DataFrame modificado con la función aplicada.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({'texto': ['HOLA', 'mundo', 'PYTHON']})
        >>> result = TransformOperations.apply_to_column(df, 'texto', str.lower, show=2)
        ╒══════════╕
        │ texto    │
        ╞══════════╡
        │ hola     │
        ├──────────┤
        │ mundo    │
        ╘══════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(column, str):
                raise TypeError("'column' debe ser una cadena")
            if not callable(func):
                raise TypeError("'func' debe ser una función callable")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Aplicar función a la columna
            df[column] = df[column].apply(func)

            # Mostrar resultado si se solicita
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
        
        Permite ordenar los datos de forma ascendente o descendente según
        una o múltiples columnas.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada a ordenar.
        columns : str o list
            Nombre(s) de columna(s) para ordenar.
        ascending : bool o list, opcional
            - bool: True para ascendente, False para descendente (aplica a todas las columnas)
            - list: Lista de booleanos que especifica el orden para cada columna individualmente
            Por defecto True (orden ascendente).
        show : int, opcional
            Control de impresión:
            - 0: no imprimir
            - >0: imprimir las primeras n filas
            - -1: imprimir todo

        Retorna:
        --------
        pd.DataFrame
            DataFrame ordenado según las columnas indicadas.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({
        ...     'nombre': ['Carlos', 'Ana', 'Juan'],
        ...     'edad': [25, 30, 20]
        ... })
        >>> result = TransformOperations.sort_by(df, 'edad', ascending=False, show=2)
        ╒═════════╤════════╕
        │ nombre  │   edad │
        ╞═════════╪════════╡
        │ Ana     │     30 │
        ├─────────┼────────┤
        │ Carlos  │     25 │
        ╘═════════╧════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(columns, (str, list)):
                raise TypeError("'columns' debe ser una cadena o una lista de cadenas")
            if not isinstance(ascending, (bool, list)):
                raise TypeError("'ascending' debe ser un booleano o lista de booleanos")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Ordenar DataFrame
            sorted_df = df.sort_values(by=columns, ascending=ascending)

            # Mostrar resultado si se solicita
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
        
        Identifica y remueve filas duplicadas basándose en un subconjunto
        de columnas o en todas las columnas.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada.
        subset : str, list o None, opcional
            Columnas para identificar duplicados.
            - Si es None: usa todas las columnas (por defecto)
            - Si es str: usa una sola columna
            - Si es list: usa múltiples columnas
        show : int
            Control de impresión.

        Retorna:
        --------
        pd.DataFrame
            DataFrame sin filas duplicadas según el subset indicado.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({
        ...     'id': [1, 2, 2, 3],
        ...     'nombre': ['A', 'B', 'B', 'C']
        ... })
        >>> result = TransformOperations.drop_duplicates(df, subset='id', show=-1)
        ╒═══════╤══════════╕
        │   id │ nombre   │
        ╞═══════╪══════════╡
        │     1 │ A        │
        ├───────┼──────────┤
        │     2 │ B        │
        ├───────┼──────────┤
        │     3 │ C        │
        ╘═══════╧══════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if subset is not None and not isinstance(subset, (str, list)):
                raise TypeError("'subset' debe ser None, una cadena o una lista")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Eliminar duplicados
            df_no_duplicates = df.drop_duplicates(subset=subset)

            # Mostrar resultado si se solicita
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
        
        Permite reemplazar valores específicos por nuevos valores en una columna.
        Soporta reemplazo de valores simples o múltiples.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada.
        column : str
            Nombre de la columna donde se reemplazan valores.
        old_value : cualquier tipo o lista
            Valor o lista de valores a reemplazar.
        new_value : cualquier tipo o lista
            Nuevo valor o lista de valores con los que se reemplaza.
        show : int
            Control de impresión.

        Retorna:
        --------
        pd.DataFrame
            DataFrame con los valores reemplazados en la columna indicada.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({'status': ['A', 'B', 'A', 'C']})
        >>> result = TransformOperations.replace_values(df, 'status', 'A', 'Active', show=2)
        ╒══════════╕
        │ status   │
        ╞══════════╡
        │ Active   │
        ├──────────┤
        │ B        │
        ╘══════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(column, str):
                raise TypeError("'column' debe ser una cadena")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Reemplazar valores
            df[column] = df[column].replace(old_value, new_value)

            # Mostrar resultado si se solicita
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
        
        Combina múltiples DataFrames apilándolos verticalmente. Todos los
        DataFrames deben tener las mismas columnas.

        Parámetros:
        -----------
        dfs : list of pd.DataFrame
            Lista de DataFrames a unir verticalmente.
        show : int
            Control de impresión.

        Retorna:
        --------
        pd.DataFrame
            DataFrame resultante de concatenar todos los DataFrames.

        Ejemplo:
        --------
        >>> df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        >>> df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        >>> result = TransformOperations.union_all([df1, df2], show=3)
        ╒═════╤═════╕
        │   A │   B │
        ╞═════╪═════╡
        │   1 │   3 │
        ├─────┼─────┤
        │   2 │   4 │
        ├─────┼─────┤
        │   5 │   7 │
        ╘═════╧═════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(dfs, list) or not all(isinstance(df, pd.DataFrame) for df in dfs):
                raise TypeError("'dfs' debe ser una lista de DataFrames")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Concatenar DataFrames
            result_df = pd.concat(dfs, ignore_index=True)

            # Mostrar resultado si se solicita
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
        
        Calcula el número de filas (conteo) para cada combinación única de
        valores en las columnas de agrupación.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada.
        by : str o list
            Columna(s) para agrupar.
        show : int
            Control de impresión.

        Retorna:
        --------
        pd.DataFrame
            DataFrame con las columnas de agrupación y una columna 'conteo'
            con el número de filas por grupo.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({
        ...     'departamento': ['IT', 'IT', 'HR', 'HR', 'IT'],
        ...     'empleado': ['A', 'B', 'C', 'D', 'E']
        ... })
        >>> result = TransformOperations.group_by_count(df, 'departamento', show=-1)
        ╒═════════════════╤══════════╕
        │ departamento   │   conteo │
        ╞═════════════════╪══════════╡
        │ HR             │        2 │
        ├─────────────────┼──────────┤
        │ IT             │        3 │
        ╘═════════════════╧══════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(by, (str, list)):
                raise TypeError("'by' debe ser una cadena o lista de cadenas")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Agrupar y contar
            grouped_df = df.groupby(by).size().reset_index(name='conteo')

            # Mostrar resultado si se solicita
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
        
        Calcula el valor promedio para una columna numérica dentro de cada grupo
        definido por las columnas de agrupación.

        Parámetros:
        -----------
        df : pd.DataFrame
            DataFrame de entrada.
        by : str o list
            Columnas para agrupar.
        column : str
            Columna cuyos valores numéricos se promedian.
        show : int
            Control de visualización de resultados.

        Retorna:
        --------
        pd.DataFrame
            DataFrame agrupado con el promedio por grupo.

        Ejemplo:
        --------
        >>> df = pd.DataFrame({
        ...     'grupo': ['A', 'A', 'B', 'B', 'A'],
        ...     'puntuacion': [85, 90, 75, 80, 95]
        ... })
        >>> result = TransformOperations.group_by_mean(df, 'grupo', 'puntuacion', show=-1)
        ╒══════════╤═════════════════╕
        │ grupo    │   puntuacion    │
        ╞══════════╪═════════════════╡
        │ A        │            90.0 │
        ├──────────┼─────────────────┤
        │ B        │            77.5 │
        ╘══════════╧═════════════════╛
        """
        try:
            # Validaciones de tipos
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(by, (str, list)):
                raise TypeError("'by' debe ser una cadena o lista")
            if not isinstance(column, str):
                raise TypeError("'column' debe ser una cadena")
            if not isinstance(show, int):
                raise TypeError("'show' debe ser un número entero")

            # Agrupar y calcular promedio
            grouped_df = df.groupby(by)[column].mean().reset_index()

            # Mostrar resultado si se solicita
            if show > 0:
                print(TransformOperations.head(grouped_df, show))
            elif show == -1:
                print(TransformOperations.head(grouped_df, len(grouped_df)))

            return grouped_df
        
        except Exception as e:
            print(f"Error al agrupar y promediar: {e}")
            raise