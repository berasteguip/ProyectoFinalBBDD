# menu_visualizacion.py

import matplotlib.pyplot as plt
import seaborn as sns
# + cualquier librería de conexión (MySQL, MongoDB, pandas, etc)

def mostrar_menu():
    while True:
        print("\n--- MENÚ DE VISUALIZACIÓN ---")
        print("1. Evolución de reviews por año")
        print("2. Popularidad de productos")
        print("3. Histograma de notas")
        print("4. Evolución temporal de reviews")
        print("5. Reviews por usuario")
        print("6. Nube de palabras")
        print("7. Otra gráfica libre")
        print("8. Salir")

        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            evolucion_reviews()
        elif opcion == "2":
            popularidad_productos()
        elif opcion == "3":
            histograma_notas()
        elif opcion == "4":
            evolucion_temporal()
        elif opcion == "5":
            histograma_reviews_usuario()
        elif opcion == "6":
            nube_de_palabras()
        elif opcion == "7":
            grafico_extra()
        elif opcion == "8":
            break
        else:
            print("Opción no válida.")


def evolucion_reviews():
    
    query = """
            SELECT COUNT(*), YEAR(review_date) as review_year
            FROM reviews
            GROUP BY review_year
            ORDER BY review_year DESC;
            """




def popularidad_productos():
    pass # TODO
def histograma_notas():
    pass # TODO
def evolucion_temporal():
    pass # TODO
def histograma_reviews_usuario():
    pass # TODO
def nube_de_palabras():
    pass # TODO
def grafico_extra():
    pass # TODO


# Cada una conectará a MySQL o MongoDB, obtiene datos y genera una gráfica

if __name__ == "__main__":
    mostrar_menu()
