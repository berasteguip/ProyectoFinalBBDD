#!/usr/bin/env python
# neo4JProyecto.py

from neo4j import GraphDatabase
import pymysql
import math
import configuracion  # Archivo de configuración con credenciales y parámetros


# Parámetros MySQL (para extraer datos de reviews)
def connect_mysql():
    return pymysql.connect(
        host=configuracion.MYSQL_HOST,
        port=configuracion.MYSQL_PORT,
        user=configuracion.MYSQL_USER,
        password=configuracion.MYSQL_PASS,
        db=configuracion.MYSQL_DB,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# Función para calcular la correlación de Pearson entre dos usuarios.
def pearson_correlation(reviews1, reviews2):
    # reviewsX es un diccionario {asin: rating} de los reviews de un usuario.
    # Se calcula la correlación sobre los artículos en común.
    common_asins = set(reviews1.keys()) & set(reviews2.keys())
    n = len(common_asins)
    if n == 0:
        return 0  # Sin reviews en común, no se define la similitud.
    
    sum1 = sum(reviews1[a] for a in common_asins)
    sum2 = sum(reviews2[a] for a in common_asins)
    
    sum1Sq = sum(pow(reviews1[a],2) for a in common_asins)
    sum2Sq = sum(pow(reviews2[a],2) for a in common_asins)
    
    pSum = sum(reviews1[a] * reviews2[a] for a in common_asins)
    
    num = pSum - (sum1 * sum2 / n)
    den = math.sqrt((sum1Sq - pow(sum1,2) / n) * (sum2Sq - pow(sum2,2) / n))
    if den == 0:
        return 0
    return num/den

# Función para extraer de MySQL la información de reviews de un usuario.
def get_reviews_by_user(user_id, connection):
    reviews = {}
    with connection.cursor() as cursor:
        sql = "SELECT p.asin, r.overall FROM Reviews r JOIN Products p ON r.product_id = p.product_id WHERE r.user_id = %s"
        cursor.execute(sql, (user_id,))
        for row in cursor.fetchall():
            reviews[row["asin"]] = float(row["overall"])
    return reviews

# Función para obtener los N usuarios con más reviews
def get_top_users(n, connection):
    with connection.cursor() as cursor:
        sql = "SELECT user_id, reviewerID FROM Users u JOIN Reviews r ON u.user_id = r.user_id GROUP BY u.user_id ORDER BY COUNT(r.review_id) DESC LIMIT %s"
        cursor.execute(sql, (n,))
        return cursor.fetchall()  # Lista de dicts con user_id y reviewerID

# Función para calcular la matriz de similitud entre los usuarios top.
def compute_similarity_matrix(users, connection):
    # Para cada usuario obtenemos su diccionario de reviews
    user_reviews = {}
    for user in users:
        user_id = user["user_id"]
        user_reviews[user_id] = get_reviews_by_user(user_id, connection)
    
    similarities = {}
    for i in range(len(users)):
        for j in range(i+1, len(users)):
            u1 = users[i]["user_id"]
            u2 = users[j]["user_id"]
            sim = pearson_correlation(user_reviews[u1], user_reviews[u2])
            if sim > 0:  # solo consideramos relaciones con similitud positiva
                similarities[(u1, u2)] = sim
    return similarities

# Funciones para cargar los datos en Neo4J

def clear_neo4j_graph(driver):
    # Limpia el grafo
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("Neo4J: Grafo limpio.")

def create_users_nodes(driver, users):
    with driver.session() as session:
        for user in users:
            session.run(
                "CREATE (u:User {user_id: $user_id, reviewerID: $reviewerID})",
                user_id=user["user_id"],
                reviewerID=user["reviewerID"]
            )
    print("Neo4J: Nodos de usuarios creados.")

def create_similarity_relationships(driver, similarities):
    with driver.session() as session:
        for (u1, u2), sim in similarities.items():
            session.run(
                """
                MATCH (a:User {user_id: $u1}), (b:User {user_id: $u2})
                CREATE (a)-[:SIMILAR {similarity: $sim}]->(b)
                CREATE (b)-[:SIMILAR {similarity: $sim}]->(a)
                """,
                u1=u1, u2=u2, sim=sim
            )
    print("Neo4J: Relaciones de similitud creadas.")

# Función para cargar enlaces entre usuarios y artículos en Neo4J.
def load_user_article_relationships(driver, article_type, num_articles, connection):
    # Seleccionar aleatoriamente 'num_articles' artículos del tipo indicado.
    with connection.cursor() as cursor:
        sql = "SELECT p.product_id, p.asin FROM Products p WHERE p.category = %s ORDER BY RAND() LIMIT %s"
        cursor.execute(sql, (article_type, num_articles))
        articles = cursor.fetchall()
    
    # Para cada artículo, obtener los usuarios que lo han revisado.
    with connection.cursor() as cursor:
        for article in articles:
            sql = "SELECT DISTINCT r.user_id, r.overall, r.review_date FROM Reviews r WHERE r.product_id = %s"
            cursor.execute(sql, (article["product_id"],))
            reviews = cursor.fetchall()
            # Crear nodo del artículo y relaciones con los usuarios.
            with driver.session() as session:
                session.run(
                    """
                    MERGE (a:Article {asin: $asin, category: $category})
                    """,
                    asin=article["asin"], category=article_type
                )
                for review in reviews:
                    session.run(
                        """
                        MATCH (u:User {user_id: $user_id}), (a:Article {asin: $asin})
                        MERGE (u)-[r:REVIEWED]->(a)
                        SET r.overall = $overall, r.review_date = $review_date
                        """,
                        user_id=review["user_id"],
                        asin=article["asin"],
                        overall=review["overall"],
                        review_date=str(review["review_date"])
                    )
    print("Neo4J: Enlaces entre usuarios y artículos cargados.")

# Función para cargar usuarios que han revisado artículos de distintos tipos.
def load_users_multiple_categories(driver, connection):
    # Seleccionar los primeros 400 usuarios ordenados alfabéticamente por reviewerID
    with connection.cursor() as cursor:
        sql = "SELECT u.user_id, u.reviewerID FROM Users u ORDER BY u.reviewerID LIMIT 400"
        cursor.execute(sql)
        users = cursor.fetchall()
    
    # Para cada usuario, obtener las categorías de los artículos que han revisado.
    user_categories = {}
    with connection.cursor() as cursor:
        for user in users:
            sql = """
                SELECT DISTINCT p.category FROM Reviews r
                JOIN Products p ON r.product_id = p.product_id
                WHERE r.user_id = %s
            """
            cursor.execute(sql, (user["user_id"],))
            cats = [row["category"] for row in cursor.fetchall()]
            if len(cats) > 1:
                user_categories[user["user_id"]] = cats
    
    # Cargar en Neo4J nodos para usuarios y para cada categoría revisada, creando relaciones
    with driver.session() as session:
        for user_id, categories in user_categories.items():
            # Aseguramos que el nodo del usuario existe:
            session.run("MERGE (u:User {user_id: $user_id})", user_id=user_id)
            for cat in categories:
                session.run(
                    """
                    MERGE (c:Category {name: $cat})
                    WITH c
                    MATCH (u:User {user_id: $user_id})
                    MERGE (u)-[:REVIEWED_CATEGORY {count: 1}]->(c)
                    ON CREATE SET u.categories = [$cat]
                    ON MATCH SET u.categories = coalesce(u.categories, []) + $cat
                    """,
                    cat=cat, user_id=user_id
                )
    print("Neo4J: Usuarios con múltiples categorías cargados.")

# Función para cargar los 5 artículos populares con menos de 40 reviews y enlazar usuarios
def load_popular_articles(driver, connection):
    with connection.cursor() as cursor:
        # Seleccionar 5 artículos con menos de 40 reviews ordenados por número de reviews (descendente)
        sql = """
            SELECT p.product_id, p.asin, COUNT(r.review_id) as review_count
            FROM Products p
            JOIN Reviews r ON p.product_id = r.product_id
            GROUP BY p.product_id
            HAVING review_count < 40
            ORDER BY review_count DESC
            LIMIT 5
        """
        cursor.execute(sql)
        articles = cursor.fetchall()
    
    with driver.session() as session:
        for article in articles:
            # Crear nodo para el artículo
            session.run(
                "MERGE (a:Article {asin: $asin}) SET a.review_count = $count",
                asin=article["asin"], count=article["review_count"]
            )
            # Obtener usuarios que han revisado el artículo
            with connection.cursor() as cursor2:
                sql = "SELECT DISTINCT user_id FROM Reviews WHERE product_id = %s"
                cursor2.execute(sql, (article["product_id"],))
                users = [row["user_id"] for row in cursor2.fetchall()]
            # Crear relaciones de revisión entre esos usuarios y el artículo
            for user_id in users:
                session.run(
                    """
                    MATCH (u:User {user_id: $user_id}), (a:Article {asin: $asin})
                    MERGE (u)-[:REVIEWED_ARTICLE]->(a)
                    """,
                    user_id=user_id, asin=article["asin"]
                )
            # Además, crear relaciones entre usuarios basadas en número de artículos en común
            for i in range(len(users)):
                for j in range(i+1, len(users)):
                    session.run(
                        """
                        MATCH (u1:User {user_id: $u1}), (u2:User {user_id: $u2})
                        MERGE (u1)-[r:COMMON_REVIEWS]-(u2)
                        ON CREATE SET r.count = 1
                        ON MATCH SET r.count = r.count + 1
                        """,
                        u1=users[i], u2=users[j]
                    )
    print("Neo4J: Artículos populares y relaciones entre usuarios creados.")

# Menú principal para la aplicación Neo4J
def main_menu():
    # Conexión a Neo4J
    driver = GraphDatabase.driver(configuracion.NEO4J_URI, auth=(configuracion.NEO4J_USER, configuracion.NEO4J_PASS))
    # Conexión a MySQL para extraer información
    mysql_conn = connect_mysql()
    
    # Limpieza inicial del grafo (si se desea)
    clear_neo4j_graph(driver)
    
    while True:
        print("\nMenú Neo4J Proyecto Bases de Datos")
        print("1. Cargar similitudes entre usuarios en Neo4J")
        print("2. Cargar enlaces entre usuarios y artículos")
        print("3. Cargar usuarios que han revisado artículos de múltiples categorías")
        print("4. Cargar 5 artículos populares (menos de 40 reviews) y relaciones entre usuarios")
        print("5. Salir")
        opcion = input("Seleccione una opción: ")
        
        if opcion == "1":
            # Obtener top 30 usuarios
            with mysql_conn.cursor() as cursor:
                top_users = get_top_users(30, mysql_conn)
            similarities = compute_similarity_matrix(top_users, mysql_conn)
            create_users_nodes(driver, top_users)
            create_similarity_relationships(driver, similarities)
            print("Funcionalidad 1 completada.")
        elif opcion == "2":
            article_type = input("Ingrese el tipo de artículo (ej. Video_Games, Toys_and_Games, etc.): ")
            num_articles = int(input("Número de artículos a seleccionar aleatoriamente: "))
            load_user_article_relationships(driver, article_type, num_articles, mysql_conn)
            print("Funcionalidad 2 completada.")
        elif opcion == "3":
            load_users_multiple_categories(driver, mysql_conn)
            print("Funcionalidad 3 completada.")
        elif opcion == "4":
            load_popular_articles(driver, mysql_conn)
            print("Funcionalidad 4 completada.")
        elif opcion == "5":
            print("Saliendo...")
            break
        else:
            print("Opción no válida. Intente de nuevo.")
    
    mysql_conn.close()
    driver.close()

if __name__ == "__main__":
    main_menu()
