import happybase
import pandas as pd

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase en la dirección IP de tu servidor
    connection = happybase.Connection('192.168.1.11')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla 'restaurants' con las familias de columnas
    table_name = 'restaurants'
    families = {
        'address': dict(),     # Información de la dirección del restaurante
        'grades': dict(),      # Calificaciones y fechas de evaluación
        'info': dict()         # Información general del restaurante
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'restaurants' creada exitosamente")

    # 3. Cargar datos del CSV
    csv_file_path = '/mnt/Datos/restaurants.csv'
    car_data = pd.read_csv(csv_file_path)

    # Iterar sobre el DataFrame usando el índice
    for index, row in car_data.iterrows():
        # Generar row key basado en el índice
        row_key = f'restaurant_{index}'.encode()

        # Organizar los datos en familias de columnas
        data = {
            b'address:building': str(row['address.building']).encode(),
            b'address:coord_x': str(row['address.coord[0]']).encode(),
            b'address:coord_y': str(row['address.coord[1]']).encode(),
            b'address:street': str(row['address.street']).encode(),
            b'address:zipcode': str(row['address.zipcode']).encode(),
            b'info:borough': str(row['borough']).encode(),
            b'info:cuisine': str(row['cuisine']).encode(),
            b'info:name': str(row['name']).encode(),
            b'info:restaurant_id': str(row['restaurant_id']).encode()
        }

        # Cargar todas las calificaciones y puntuaciones
        for i in range(9):  # Ajustar si el número de calificaciones varía
            date_col = f'grades[{i}].date'
            grade_col = f'grades[{i}].grade'
            score_col = f'grades[{i}].score'

            if date_col in row and pd.notna(row[date_col]):
                data[f'grades:date_{i+1}'.encode()] = str(row[date_col]).encode()
            if grade_col in row and pd.notna(row[grade_col]):
                data[f'grades:grade_{i+1}'.encode()] = str(row[grade_col]).encode()
            if score_col in row and pd.notna(row[score_col]):
                try:
                    score_value = float(row[score_col])
                    data[f'grades:score_{i+1}'.encode()] = str(score_value).encode()
                except ValueError as e:
                    print(f"Error al convertir el valor de la calificación: {row[score_col]}. Error: {e}")

        # Insertar datos en HBase
        table.put(row_key, data)

    print("Datos cargados exitosamente")

    # 4. Consultas y análisis de datos
    print("\n=== Primeros restaurantes en la base de datos===")
    count = 0
    for key, data in table.scan():
        if count < 3:  # Mostrar solo los primeros 3
            print(f"\nRestaurante ID: {key.decode()}")
            print(f"Nombre: {data[b'info:name'].decode()}")
            print(f"Borough: {data[b'info:borough'].decode()}")
            print(f"Cocina: {data[b'info:cuisine'].decode()}")
            count += 1

    print("\n=== Top 10 restaurantes de cocina india fuera de Manhattan ===")
    indian_restaurants = [
        (key.decode(), data)
        for key, data in table.scan()
        if data.get(b'info:cuisine', b'').decode().lower() == 'indian' and
           data.get(b'info:borough', b'').decode().lower() != 'manhattan' and
           b'grades:score_1' in data
    ]
    top_indian_restaurants = sorted(
        indian_restaurants,
        key=lambda x: float(x[1].get(b'grades:score_1', b'0').decode()),
        reverse=True
    )[:10]
    for restaurant in top_indian_restaurants:
        key, data = restaurant
        print(f"\nRestaurante ID: {key}")
        print(f"Nombre: {data[b'info:name'].decode()}")
        print(f"Borough: {data[b'info:borough'].decode()}")
        print(f"Calificación: {data[b'grades:score_1'].decode()}")

    print("\n=== Contar la cantidad de restaurantes según categoría ===")
    cuisine_count = {}
    for key, data in table.scan():
        cuisine = data.get(b'info:cuisine', b'').decode()
        cuisine_count[cuisine] = cuisine_count.get(cuisine, 0) + 1
    for cuisine, count in cuisine_count.items():
        print(f"Cocina: {cuisine}, Cantidad: {count}")

    print("\n=== Obtener los 5 restaurantes con mayor calificación ===")
    top_restaurants = sorted(
        [(key.decode(), float(data.get(b'grades:score_1', b'0').decode()))
         for key, data in table.scan() if b'grades:score_1' in data],
        key=lambda x: x[1], reverse=True
    )[:5]
    for restaurant in top_restaurants:
        print(f"Restaurante ID: {restaurant[0]}, Calificación: {restaurant[1]}")

    print("\n=== Realizando una actualización de datos ===")
    
    # Mostrar los valores actuales del restaurante antes de la actualización
    example_key = b'restaurant_0'

    print("\n=== Estado del restaurante antes de la actualización ===")
    row = table.row(example_key)  # Obtener los datos del restaurante
    if row:
        for key, value in row.items():
            print(f"{key.decode()}: {value.decode()}")
    else:
        print(f"Restaurante con ID {example_key.decode()} no encontrado.")

    # Realizar la actualización de datos
    print("\n=== Realizando una actualización de datos ===")
    table.put(example_key, {b'info:name': b'Nueva Ciudad Restaurant'})
    print(f"Restaurante actualizado: {example_key.decode()} con nuevo nombre.")

    # Mostrar los valores del restaurante después de la actualización
    print("\n=== Estado del restaurante después de la actualización ===")
    row = table.row(example_key)  # Obtener nuevamente los datos del restaurante
    if row:
        for key, value in row.items():
            print(f"{key.decode()}: {value.decode()}")
    else:
        print(f"Restaurante con ID {example_key.decode()} no encontrado.")

    print("\n=== Eliminando un restaurante ===")
    example_key_to_delete = b'restaurant_1'
    table.delete(example_key_to_delete)
    print(f"Restaurante eliminado: {example_key_to_delete.decode()}.")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Cerrar la conexión
    connection.close()
