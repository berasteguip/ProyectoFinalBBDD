import pymysql
import pymongo
import json
from datetime import datetime
import configuracion 

def create_mysql_tables(cursor):
    """
    Crea la base de datos y las tablas necesarias en MySQL (Users, Products, Reviews) si no existen.
    """
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {configuracion.MYSQL_DB}")
    cursor.execute(f"USE {configuracion.MYSQL_DB}")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Users (
          user_id INT AUTO_INCREMENT PRIMARY KEY,
          reviewerID VARCHAR(50) UNIQUE,
          reviewerName VARCHAR(255)
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products (
          product_id INT AUTO_INCREMENT PRIMARY KEY,
          asin VARCHAR(50) UNIQUE,
          category VARCHAR(50)
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Reviews (
          review_id INT AUTO_INCREMENT PRIMARY KEY,
          user_id INT,
          product_id INT,
          overall FLOAT,
          review_date DATE,
          unixTime BIGINT,
          FOREIGN KEY (user_id) REFERENCES Users(user_id),
          FOREIGN KEY (product_id) REFERENCES Products(product_id)
        );
    """)

def connect_mysql():
    """
    Establece una conexión con la base de datos MySQL utilizando los parámetros de configuración.
    Devuelve el objeto de conexión.
    """
    conexion = pymysql.connect(
        host=configuracion.MYSQL_HOST,
        port=configuracion.MYSQL_PORT,
        user=configuracion.MYSQL_USER,
        password=configuracion.MYSQL_PASS,
        charset='utf8mb4'
    )
    return conexion

def connect_mongo():
    """
    Establece una conexión con la base de datos MongoDB y devuelve la colección especificada.
    """
    mongo_client = pymongo.MongoClient(configuracion.MONGO_HOST, configuracion.MONGO_PORT)
    db = mongo_client[configuracion.MONGO_DB]
    collection = db[configuracion.MONGO_COLLECTION]
    return collection

def parse_review_time(review_time_str):
    """
    Convierte una cadena de fecha de revisión en formato 'MM DD, YYYY' a un objeto de fecha de Python.
    Devuelve None si la conversión falla.
    """
    try:
        return datetime.strptime(review_time_str, "%m %d, %Y").date()
    except:
        return None

def get_or_insert_user(reviewer_id, reviewer_name, cursor):
    """
    Verifica si un usuario existe en la tabla Users mediante el reviewerID.
    Si no existe, inserta el usuario en la tabla.
    Devuelve el user_id del usuario existente o recién insertado.
    """
    cursor.execute("SELECT user_id FROM Users WHERE reviewerID = %s", (reviewer_id,))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute(
            "INSERT INTO Users(reviewerID, reviewerName) VALUES (%s, %s)",
            (reviewer_id, reviewer_name)
        )
        return cursor.lastrowid

def get_or_insert_product(asin, category, cursor):
    """
    Verifica si un producto existe en la tabla Products mediante el asin.
    Si no existe, inserta el producto en la tabla.
    Devuelve el product_id del producto existente o recién insertado.
    """
    cursor.execute("SELECT product_id FROM Products WHERE asin = %s", (asin,))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute(
            "INSERT INTO Products(asin, category) VALUES (%s, %s)",
            (asin, category)
        )
        return cursor.lastrowid

def insert_review_mysql(user_id, product_id, overall, rdate, unix_time, cursor):
    """
    Inserta una revisión en la tabla Reviews de MySQL.
    Devuelve el review_id de la revisión recién insertada.
    """
    cursor.execute("""
        INSERT INTO Reviews(user_id, product_id, overall, review_date, unixTime)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_id, product_id, overall, rdate, unix_time))
    return cursor.lastrowid

def insert_review_mongo(review_id, review, mongo_collection):
    """
    Inserta un documento de revisión en la colección de MongoDB.
    El documento incluye el texto de la revisión, el resumen y los datos de utilidad.
    """
    doc = {
        "_id": review_id,
        "reviewText": review.get("reviewText", ""),
        "summary": review.get("summary", ""),
        "helpful": review.get("helpful", [])
    }
    mongo_collection.insert_one(doc)

def process_json_file(filepath, category, mysql_cursor, mysql_conexion, mongo_collection):
    """
    Procesa un archivo JSON que contiene revisiones.
    Para cada revisión:
    - Inserta o recupera el usuario y el producto en MySQL.
    - Inserta la revisión en MySQL y MongoDB.
    Realiza un commit en MySQL después de procesar el archivo.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            review = json.loads(line)

            # Procesamiento en MySQL:
            reviewer_id = review.get("reviewerID")
            reviewer_name = review.get("reviewerName", None)
            user_id = get_or_insert_user(reviewer_id, reviewer_name, mysql_cursor)

            asin = review.get("asin")
            product_id = get_or_insert_product(asin, category, mysql_cursor)

            overall = float(review.get("overall", 0))
            rdate = parse_review_time(review.get("reviewTime", ""))
            unix_time = int(review.get("unixReviewTime", 0))

            review_id = insert_review_mysql(user_id, product_id, overall, rdate, unix_time, mysql_cursor)

            # Procesamiento en MongoDB (se llama a la función separada)
            insert_review_mongo(review_id, review, mongo_collection)
        
        mysql_conexion.commit()

def main():
    """
    Función principal que:
    - Conecta a MySQL y MongoDB.
    - Crea las tablas necesarias en MySQL.
    - Procesa archivos JSON para diferentes categorías de productos.
    - Cierra las conexiones a las bases de datos después de procesar.
    """
    # Conexión y creación de tablas en MySQL
    print('Realizando conexión...')
    conexion_mysql = connect_mysql()
    cur_mysql = conexion_mysql.cursor()
    print('Conexión exitosa. Creando tablas...')
    create_mysql_tables(cur_mysql)
    conexion_mysql.commit()
    print('Tablas creadas existosamente.')

    # Conexión a MongoDB (opcional: limpiar la colección)
    mongo_collection = connect_mongo()
    mongo_collection.drop()

    # Procesar cada fichero JSON (se asume que las rutas están en configuracion.py)
    print('Procesando Toys_and_Games...')
    process_json_file(configuracion.TOYS_FILE, "Toys_and_Games", cur_mysql, conexion_mysql, mongo_collection)
    print(f'Toys_and_Games completado. Procesando Video_Games...')
    process_json_file(configuracion.VIDEOGAMES_FILE, "Video_Games", cur_mysql, conexion_mysql, mongo_collection)
    print(f'Video_Games completado. Procesando Digital_Music...')
    process_json_file(configuracion.MUSIC_FILE, "Digital_Music", cur_mysql, conexion_mysql, mongo_collection)
    print(f'Digital_Music completado. Procesando Musical_Instruments...')
    process_json_file(configuracion.INSTRUMENTS_FILE, "Musical_Instruments", cur_mysql, conexion_mysql, mongo_collection)
    print('Musical_Instruments completado. Cerrando conexión...')
    cur_mysql.close()
    conexion_mysql.close()
    print("Carga completada.")

if __name__ == "__main__":
    main()
