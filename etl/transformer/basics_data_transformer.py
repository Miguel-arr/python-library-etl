
import pandas as pd
from tabulate import tabulate
from functools import wraps


def validate_params(df_type=False, columns_type=False, lambda_type=False, n_type=False):

    #Decorador para validar parámetros en las funciones de transformación de DataFrames.
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                if df_type:
                    try:
                        if not isinstance(args[0], pd.DataFrame):
                            raise TypeError("El primer argumento debe ser un DataFrame de pandas")
                    except IndexError:
                        raise ValueError("Falta el argumento del DataFrame")

                if columns_type:
                    try:
                        if not isinstance(args[1], (str, list, dict, type(None))):
                            raise TypeError("El segundo argumento debe ser un string, lista, diccionario o None")
                    except IndexError:
                        pass  

                if lambda_type:
                    try:
                        if not (callable(args[2]) or isinstance(args[2], str) or callable(args[1]) or isinstance(args[1], str)):
                            raise TypeError("El argumento debe ser una función lambda válida o un string")
                    except IndexError:
                        pass  

                if n_type:
                    try:
                        if not (isinstance(args[3], int) or isinstance(args[2], int)):
                            raise ValueError("El ultimo argumento debe ser un entero")
                    except IndexError:
                        pass  

                return func(*args, **kwargs)
            except Exception as e:
                raise ValueError(f"Error en la validación de parámetros: {e}")

        return wrapper
    return decorator


class BasicsTransformOperations:

    @staticmethod
    def show_head(df, n=5, print_result=True):
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
    def show_tail(df, n=5, print_result=False):
        #Devuelve las últimas n filas del DataFrame.
        try:
            df_head = df.tail(n)
            result = tabulate(df_head, headers="keys", tablefmt="fancy_grid", showindex=False)
            if print_result:
                print(result)
            return result
        except Exception as e:
            print(f"Error al obtener las últimas {n} filas: {e}")
            raise
    
  

    @staticmethod
    @validate_params(df_type=True, columns_type=True, lambda_type=True, n_type=True)
    def add_new_column(df, new_column_name, lambda_func, show=0):
        #Agrega una nueva columna al DataFrame usando una función lambda.
        try:
            if callable(lambda_func):
                df[new_column_name] = df.apply(lambda_func, axis=1)
            else:
                raise ValueError("lambda_func debe ser una función callable")
                
            if show > 0:
                print(BasicsTransformOperations.show_head(df, show))
            elif show == -1:
                print(BasicsTransformOperations.show_head(df, len(df)))
            return df
        except Exception as e:
            print(f"Error al agregar la columna: {e}")
            raise

   

    @staticmethod
    @validate_params(df_type=True, columns_type=True, n_type=True)
    def remove_columns(df, columns_to_drop, show=0, extra_param=None):
        #Elimina columnas del DataFrame.
        try:
            result_df = df.drop(columns=columns_to_drop)
            if show > 0:
                print(BasicsTransformOperations.show_head(result_df, show))
            elif show == -1:
                print(BasicsTransformOperations.show_head(result_df, len(result_df)))
            return result_df
        except Exception as e:
            print(f"Error al eliminar las columnas: {e}")
            raise


    @staticmethod
    @validate_params(df_type=True, columns_type=True, n_type=True)
    def select_columns(df, *columns, show=0):
        #Selecciona columnas específicas del DataFrame.
        try:
            result_df = df[list(columns)]
            if show > 0:
                print(BasicsTransformOperations.show_head(result_df, show))
            elif show == -1:
                print(BasicsTransformOperations.show_head(result_df, len(result_df)))
            return result_df
        except Exception as e:
            print(f"Error al seleccionar/reordenar columnas: {e}")
            raise

    @staticmethod
    @validate_params(df_type=True, columns_type=True, lambda_type=True, n_type=True)
    def transform_column(df, column, func, show=0):
        #Aplica una función LAMBDA a una columna específica del DataFrame.
        try:
            df[column] = df[column].apply(func)
            if show > 0:
                print(BasicsTransformOperations.show_head(df, show))
            elif show == -1:
                print(BasicsTransformOperations.show_head(df, len(df)))
            return df
        except Exception as e:
            print(f"Error al aplicar la función a la columna '{column}': {e}")
            raise

    @staticmethod
    @validate_params(df_type=True, lambda_type=True, n_type=True)
    def filter_by_condition(df, lambda_func, show=0):
        #Filtra filas de un DataFrame según una función lambda.
        try:
            filtered_df = df[df.apply(lambda_func, axis=1)]
            if show > 0:
                print(BasicsTransformOperations.show_head(filtered_df, show))
            elif show == -1:
                print(BasicsTransformOperations.show_head(filtered_df, len(filtered_df)))
            return filtered_df
        except Exception as e:
            print(f"Error al filtrar las filas: {e}")
            raise

    @staticmethod
    def sort_columns(df, columns, ascending=True):
        try:
            # Verificar si la columna existe en el DataFrame
            if isinstance(columns, str):  # Si es un solo nombre de columna
                columns = [columns]  # Convertirlo en una lista
            
            missing_columns = [col for col in columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Las siguientes columnas no existen en el DataFrame: {missing_columns}")
            # Ordenar el DataFrame
            sorted_df = df.sort_values(by=columns, ascending=ascending)
            return sorted_df
        
        except Exception as e:
            print(f"Error al ordenar las filas: {e}")
            raise
    
    

