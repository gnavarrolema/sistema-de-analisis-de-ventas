import pytest
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env para la conexión a BD
load_dotenv()

# --- Fixture para la conexión a la base de datos ---
@pytest.fixture(scope="module") # la conexión se hace una vez por archivo de prueba
def db_connection():
    """
    Establece y devuelve una conexión a la base de datos.
    Se cierra automáticamente al final de las pruebas del módulo.
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME") 
        )
        print("\nConexión a MySQL exitosa para pruebas de integridad.")
        yield connection # Proporciona la conexión a las pruebas
    except Error as e:
        pytest.fail(f"Error al conectar a MySQL: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nConexión a MySQL cerrada.")

# --- Pruebas de Integridad ---
def test_conteo_paises(db_connection):
    """
    Verifica que la tabla 'countries' tenga el número esperado de filas.
    """
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM countries;")
    resultado = cursor.fetchone()
    numero_de_paises_en_db = resultado[0]

    numero_esperado_de_paises = 206 

    assert numero_de_paises_en_db == numero_esperado_de_paises, \
        f"Se esperaban {numero_esperado_de_paises} países, pero se encontraron {numero_de_paises_en_db}."
    cursor.close()

def test_conteo_ventas(db_connection):
    """
    Verifica que la tabla 'sales' tenga 50,000 filas.
    """
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM sales;")
    resultado = cursor.fetchone()
    numero_de_ventas_en_db = resultado[0]
    numero_esperado_de_ventas = 50000

    assert numero_de_ventas_en_db == numero_esperado_de_ventas, \
        f"Se esperaban {numero_esperado_de_ventas} ventas, pero se encontraron {numero_de_ventas_en_db}."
    cursor.close()