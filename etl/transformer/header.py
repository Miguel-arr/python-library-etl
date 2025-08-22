import pandas as pd
from tabulate import tabulate

class HeaderOperations:
    
    

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
    def replace_all_headers(df, new_headers, show=0): 
        #Reemplaza completamente la fila de encabezado con una nueva lista de nombres de columnas.
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            if not isinstance(new_headers, list):
                raise TypeError("new_headers debe ser una lista de nombres de columnas")
            if len(new_headers) != len(df.columns):
                raise ValueError("La cantidad de nuevos nombres debe coincidir con el número de columnas del DataFrame")
            
            df.columns = new_headers  
            # Cambia los nombres de las columnas

            if show > 0:
                print(HeaderOperations.head(df, show))
            elif show == -1:
                print(HeaderOperations.head(df, len(df)))

            return df
        except Exception as e:
            print(f"Error al reemplazar el encabezado: {e}")
            raise

    @staticmethod
    def rename_columns(df, *args, show=0, **kwargs): 
        # Renombra uno o más valores en la fila de encabezado del DataFrame y muestra opcionalmente el resultado.

        try:
            # Si pasamos un diccionario de renombrado, usamos el método rename de pandas
            if isinstance(args[0], dict):
                df = df.rename(columns=args[0])
            else:
                # Si se pasan argumentos individuales, renombramos una sola columna
                for old_name, new_name in kwargs.items():
                    df = df.rename(columns={old_name: new_name})

            # Si se solicita mostrar la tabla, usamos el método head de esta clase
            if show > 0:
                print(HeaderOperations.head(df, show))
            elif show == -1:  # Si show es -1, mostramos todo el DataFrame
                print(HeaderOperations.head(df, len(df)))

            return df  # Retornar el DataFrame para seguir trabajando con él
        except Exception as e:
            print(f"Error al renombrar columnas: {e}")
            raise

    @staticmethod
        

    @staticmethod
    def drop_header(df, columns_to_drop, show=0): # ELIMINAR ESA EN BASICS TRANSFORMS
        #Elimina una o más columnas del DataFrame.
        
        try:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("df debe ser un DataFrame de pandas")
            
            if isinstance(columns_to_drop, str):  # Si se pasa un solo nombre como string, convertirlo en lista
                columns_to_drop = [columns_to_drop]
            
            if not isinstance(columns_to_drop, list):
                raise TypeError("columns_to_drop debe ser un string o una lista de nombres de columnas")

            df = df.drop(columns=columns_to_drop, errors='ignore')  # Elimina las columnas sin error si no existen

            if show > 0:
                print(HeaderOperations.head(df, show))
            elif show == -1:
                print(HeaderOperations.head(df, len(df)))

            return df  # Retorna el DataFrame modificado

        except Exception as e:
            print(f"Error al eliminar columnas: {e}")
            raise

    @staticmethod
    def prefix_header(df, prefix, show=0): #ante pone un prefijo
        try:
            df = df.add_prefix(prefix)
            if show > 0:
                print(HeaderOperations.head(df, show))
            elif show == -1:
                print(HeaderOperations.head(df, len(df)))
            return df
        except Exception as e:
            print(f"Error al agregar prefijo a los encabezados: {e}")
            raise

    @staticmethod
    def suffix_header(df, suffix, show=0):#dobrepone pone un prefijo
        try:
            df = df.add_suffix(suffix)
            if show > 0:
                print(HeaderOperations.head(df, show))
            elif show == -1:
                print(HeaderOperations.head(df, len(df)))
            return df
        except Exception as e:
            print(f"Error al agregar sufijo a los encabezados: {e}")
            raise