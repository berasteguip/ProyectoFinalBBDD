# configuracion.py

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASS = "5QyePb9T"
MYSQL_DB   = "ProyectoAmazon"

MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB   = "ProyectoBBDD"
MONGO_COLLECTION = "ReviewsText"

# Parámetros Neo4J (se pueden definir en configuracion.py)
NEO4J_URI = "bolt://localhost:7687"  # URI de conexión a Neo4J
NEO4J_USER = "neo4j"                # Usuario de Neo4J
NEO4J_PASS = "password"             # Contraseña de Neo4J

# Rutas de los ficheros JSON
TOYS_FILE       = "data/Toys_and_Games_5.json"
VIDEOGAMES_FILE = "data/Video_Games_5.json"
MUSIC_FILE      = "data/Digital_Music_5.json"
INSTRUMENTS_FILE= "data/Musical_Instruments_5.json"
