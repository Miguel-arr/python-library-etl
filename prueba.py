foreign_keys_map = {
    "id_producto": ("dim_productos", "producto_id"),
    "id_cliente": ("dim_clientes", "cliente_id"),
    "id_tienda": ("dim_tiendas", "tienda_id")
}
print("-------------------------------------------------------------------------------")
# Recorrer las claves y valores del diccionario
for clave, valor in foreign_keys_map.items():
    print(f"Clave: {clave}, Valor: {valor}")
    # También puedes acceder a los elementos de la tupla valor
    tabla = valor[0]
    columna = valor[1]
    print(f"Tabla: {tabla}, Columna: {columna}")

print("-------------------------------------------------------------------------------")
for clave in foreign_keys_map.keys():
        print(f"Clave: {clave}")    
print("-------------------------------------------------------------------------------")
for valor in foreign_keys_map.values():
    print(f"Valor: {valor}")
    tabla, columna = valor  # Desempaquetar la tupla
    print(f"Tabla: {tabla}, Columna: {columna}")

print("-------------------------------------------------------------------------------")
for i, (clave, valor) in enumerate(foreign_keys_map.items()): 
    print(f"Índice: {i}, Clave: {clave}, Valor: {valor}")