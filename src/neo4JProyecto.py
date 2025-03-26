#!/usr/bin/env python
# neo4JProyecto.py

from neo4j import GraphDatabase
import pymysql
import math
import configuracion  # Archivo de configuración con credenciales y parámetros

# Parámetros Neo4J (se pueden definir en configuracion.py)
NEO4J_URI = configuracion.NEO4J_URI       # ej: "bolt://localhost:7687"
NEO4J_USER = configuracion.NEO4J_USER     # ej: "neo4j"
NEO4J_PASS = configuracion.NEO4J_PASS     # ej: "password"

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
        cursor.execute(sql,
