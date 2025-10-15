import pandas as pd
from itertools import combinations
from collections import Counter
import pandas as pd
from itertools import combinations
from collections import Counter
from etl import BasicsTransformOperations, DB_Extractor, DB_Loader, HeaderOperations, TransformOperations


def create_db_connection():
    """Crea y retorna la conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "colombia_saludable"   
    }
    return DB_Extractor(**db_params)

def db_connection_loader():
    """Crea y retorna una conexión a la base de datos"""
    db_params = {
        "db_type": "postgresql",
        "user": "postgres",
        "password": "admin",           
        "database": "carga_colombia"   
    }
    return DB_Extractor(**db_params)

db_connected = create_db_connection()
db_loader = db_connection_loader()
db_connected.connect()
db_loader.connect()
loader_carga_colombia = DB_Loader(engine=db_loader.engine)

print("Extrayendo datos de la tabla formulas medicas...")

fomulas = db_connected.get_table("formulas_medicas")




# 1. DATOS ORIGINALES (simulando tu tabla)


print("=== DATOS ORIGINALES ===")
h = print(BasicsTransformOperations.show_head(fomulas))
print(h)

# 2. NORMALIZACIÓN: Convertir string a lista
df_original = BasicsTransformOperations.convert_column_to_list(fomulas, 'medicamentos_recetados', nueva_columna="medicamentos_lista", show=0)
df_original["medicamentos_recetados"] = df_original["medicamentos_lista"]
#df_original['medicamentos_lista'] = df_original['medicamentos_recetados'].str.split(',')

print("=== DESPUÉS DE CONVERTIR A LISTA ===")
print(BasicsTransformOperations.show_head(df_original))
print("\n")

# 3. EXPLODE: Crear una fila por cada medicamento
#df_normalizado = df_original.explode('medicamentos_lista')
df_normalizado = BasicsTransformOperations.explode_column_list(df_original, 'medicamentos_lista', show=0)
df_normalizado2 = BasicsTransformOperations.normalize_delimited_column(df_normalizado, 'medicamentos_recetados', ",", show=0)

print("=== DESPUÉS DE EXPLODE (UNA FILA POR MEDICAMENTO) ===")
print(BasicsTransformOperations.show_head(df_normalizado))
print(BasicsTransformOperations.show_head(df_normalizado2))
print("\n")

# 4. LIMPIEZA: Crear columna limpia
df_normalizado['codigo_medicamento'] = df_normalizado['medicamentos_lista'].str.strip()

# 5. ELIMINAR COLUMNAS TEMPORALES
df_normalizado = df_normalizado.drop([ 'medicamentos_lista'], axis=1)

print("=== DATOS NORMALIZADOS Y LIMPIOS ===")
print(BasicsTransformOperations.show_head(df_normalizado))
print("\n")

# 6. RESETEO DE ÍNDICE
df_normalizado = df_normalizado.reset_index(drop=True)
df_normalizado.index.name = 'id_detalle'
df_normalizado = df_normalizado.reset_index()

print("=== DATOS FINALES CON ID_DETALLE ===")
print(BasicsTransformOperations.show_head(df_normalizado))
print("\n")

# 7. ANÁLISIS: MEDICAMENTOS INDIVIDUALES MÁS FRECUENTES
print("=== TOP MEDICAMENTOS INDIVIDUALES ===")
top_medicamentos =  TransformOperations.filtrar_por_longitud_lista(df_normalizado, "medicamentos_recetados", 1)
print(BasicsTransformOperations.show_head(top_medicamentos))
top_medicamentos =  df_normalizado['codigo_medicamento'].value_counts().reset_index()
top_medicamentos.columns = ['codigo_medicamento', 'frecuencia']
print(BasicsTransformOperations.show_head(top_medicamentos))
print("\n")

# 8. ANÁLISIS: MEDICAMENTOS QUE SE FORMULAN JUNTOS

# Agrupar por fórmula para obtener listas de medicamentos
formulas_agrupadas = df_normalizado.groupby('codigo_formula')['codigo_medicamento'].apply(list)

print("=== MEDICAMENTOS AGRUPADOS POR FÓRMULA ===")
print(formulas_agrupadas)
print("\n")

# Contar combinaciones de pares
contador_combinaciones = Counter()

for medicamentos in formulas_agrupadas:
    if len(medicamentos) >= 2:
        for combo in combinations(medicamentos, 2):
            # Ordenar para que (A,B) sea igual a (B,A)
            combo_ordenado = tuple(sorted(combo))
            contador_combinaciones[combo_ordenado] += 1

print("=== CONTADOR DE COMBINACIONES ===")
#print(contador_combinaciones)
print("\n")

# Convertir a DataFrame
combinaciones_frecuentes = []
for combo, freq in contador_combinaciones.items():
    combinaciones_frecuentes.append({
        'medicamento_a': combo[0],
        'medicamento_b': combo[1],
        'frecuencia_conjunta': freq
    })

df_combinaciones = pd.DataFrame(combinaciones_frecuentes)
df_combinaciones = df_combinaciones.sort_values('frecuencia_conjunta', ascending=False)

print("=== COMBINACIONES MÁS FRECUENTES ===")
print(df_combinaciones)
print("\n")

# 9. ANÁLISIS ADICIONAL: TAMAÑO DE LAS FÓRMULAS
print("=== DISTRIBUCIÓN DE TAMAÑO DE FÓRMULAS ===")
tamaño_formulas = df_normalizado.groupby('codigo_formula').size().value_counts().sort_index()
print(tamaño_formulas)
print("\n")

# 10. ESTADÍSTICAS GENERALES
print("=== ESTADÍSTICAS GENERALES ===")
print(f"Total de fórmulas: {df_normalizado['codigo_formula'].nunique()}")
print(f"Total de medicamentos únicos: {df_normalizado['codigo_medicamento'].nunique()}")
print(f"Promedio de medicamentos por fórmula: {df_normalizado.groupby('codigo_formula').size().mean():.2f}")