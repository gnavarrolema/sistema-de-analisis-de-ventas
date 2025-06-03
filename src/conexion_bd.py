import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
import pandas as pd

class ConexionBD:
    __instancia = None
    __motor = None
    __sesion = None

    def __new__(cls):
        if cls.__instancia is None:
            cls.__instancia = super(ConexionBD, cls).__new__(cls)
            cls.__instancia.__inicializar_conexion()
        return cls.__instancia

    def __inicializar_conexion(self):
        """
        Inicializa la conexión a la base de datos utilizando SQLAlchemy.
        Las credenciales se cargan desde el archivo .env.
        """
        load_dotenv() # Carga las variables del archivo .env

        usuario = os.getenv("DB_USER")
        contrasena = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        puerto = os.getenv("DB_PORT", "3306") # Puerto por defecto de MySQL
        nombre_bd = os.getenv("DB_NAME")

        if not all([usuario, contrasena, host, nombre_bd]):
            raise ValueError("Faltan variables de entorno para la conexión a la base de datos.")

        try:
            # Cadena de conexión: mysql+pymysql://usuario:contraseña@host:puerto/basedatos
            cadena_conexion = f"mysql+pymysql://{usuario}:{contrasena}@{host}:{puerto}/{nombre_bd}"
            self.__motor = create_engine(cadena_conexion)
            
            # Crear una sesión
            Session = sessionmaker(bind=self.__motor)
            self.__sesion = Session()
            print("Conexión a la base de datos establecida exitosamente.")

        except Exception as e:
            print(f"Error al conectar con la base de datos: {e}")
            self.__motor = None
            self.__sesion = None
                      
    def obtener_motor(self):
        """
        Retorna el motor de SQLAlchemy.
        """
        return self.__motor

    def obtener_sesion(self):
        """
        Retorna la sesión de SQLAlchemy.
        """
        if self.__sesion is None and self.__motor is not None:            
            Session = sessionmaker(bind=self.__motor)
            self.__sesion = Session()
        return self.__sesion

    def cerrar_sesion(self):
        """
        Cierra la sesión de SQLAlchemy si está abierta.
        """
        if self.__sesion:
            self.__sesion.close()
            print("Sesión de base de datos cerrada.")
            self.__sesion = None # Importante para poder reabrir si es necesario

    def ejecutar_consulta(self, consulta_sql: str, params: dict = None) -> pd.DataFrame | None:
        """
        Ejecuta una consulta SQL y retorna los resultados como un DataFrame de pandas.

        Args:
            consulta_sql (str): La consulta SQL a ejecutar.
            params (dict, optional): Parámetros para la consulta SQL. Defaults to None.

        Returns:
            pd.DataFrame | None: Un DataFrame con los resultados o None si hay un error.
        """
        if self.__motor is None:
            print("Error: No hay conexión establecida con la base de datos.")
            return None
        
        try:
            df = pd.read_sql_query(consulta_sql, self.__motor, params=params)
            return df
        except Exception as e:
            print(f"Error al ejecutar la consulta: {e}")
            return None

# ¿Cómo usar la clase ConexionBD?
if __name__ == "__main__":
#Obtener instancia Singleton
     conector1 = ConexionBD()
     conector2 = ConexionBD()

     print(f"Motor 1: {conector1.obtener_motor()}")
     print(f"Motor 2: {conector2.obtener_motor()}")
     print(f"Son la misma instancia? {conector1 is conector2}")

     if conector1.obtener_motor():
         # Ejemplo de consulta
         df_empleados = conector1.ejecutar_consulta("SELECT * FROM employees LIMIT 5;")
         if df_empleados is not None:
             print("\nPrimeros 5 empleados:")
             print(df_empleados)
        
         # Cerrar la sesión cuando ya no se necesite 
         conector1.cerrar_sesion()