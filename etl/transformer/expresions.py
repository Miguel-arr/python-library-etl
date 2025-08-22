import pandas as pd
from tabulate import tabulate
from etl.transformer.basics_data_transformer import BasicsTransformOperations


btf = BasicsTransformOperations

class DataExpresion:
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
    def search_in_column(df, field, pattern, show=0, complement=False):
        #Busca filas donde el campo contenga el patrón dado.
        try:
            if not isinstance(field, str):
                raise TypeError("field debe ser un string")
            if not isinstance(pattern, str):
                raise TypeError("pattern debe ser un string")
            if not isinstance(complement, bool):
                raise TypeError("complement debe ser un booleano")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")
            
            if complement:
                result = df[~df[field].astype(str).str.contains(pattern, na=False)]
            else:
                result = df[df[field].astype(str).str.contains(pattern, na=False)]
            
            if show:
                print(btf.show_head(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en search: {e}")
            raise

    @staticmethod
    def search_in_table(df, pattern, show=0,  complement=False):
        #Busca filas donde cualquier campo contenga el patrón dado.
        try:
            if not isinstance(pattern, str):
                raise TypeError("pattern debe ser un string")
            if not isinstance(complement, bool):
                raise TypeError("complement debe ser un booleano")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")
            
            mask = df.astype(str).apply(lambda row: row.str.contains(pattern, na=False)).any(axis=1)
            result = df[~mask] if complement else df[mask]
            
            if show:
                print(btf.show_head(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en search_any: {e}")
            raise

    @staticmethod
    def split_column_into_rows(df, field, delimiter, show=0):
        #Divide un campo en varias filas mediante un delimitador.
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(field, str) or field not in df.columns:
                raise ValueError(f"field debe ser un nombre de columna válido: {df.columns.tolist()}")
            if not isinstance(delimiter, str):
                raise TypeError("delimiter debe ser un string")
            if not isinstance(show, int) or show < -1:
                raise ValueError("show debe ser un entero mayor o igual a -1")
            
            df_exploded = df.assign(**{field: df[field].astype(str).str.split(delimiter)}).explode(field)
            result = df_exploded.reset_index(drop=True)
            
            if show:
                print(btf.show_head(result, show if show > 0 else len(result)))
            
            return result
        except Exception as e:
            print(f"Error en splitdown: {e}")
            raise
        