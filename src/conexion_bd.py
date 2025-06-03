import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import psutil  # Para métricas de sistema

# Importar nuestro sistema de logging
from src.utils.sistema_logging import (
    ConfiguradorLogging, 
    registrar_operacion, 
    registrar_consulta_sql,
    RegistradorEstadisticas,
    ManejadorErrores
)


class ConexionBD:
    """
    Clase Singleton para manejar la conexión a la base de datos MySQL.
    Incluye sistema de logging profesional y manejo de errores robusto.
    """
    
    __instancia = None
    __motor = None
    __sesion = None
    __contador_consultas = 0
    
    def __new__(cls):
        if cls.__instancia is None:
            cls.__instancia = super(ConexionBD, cls).__new__(cls)
            # Configurar logging específico para esta clase
            cls.__instancia.logger = ConfiguradorLogging.obtener_logger("ConexionBD")
            cls.__instancia.estadisticas = RegistradorEstadisticas()
            cls.__instancia.manejador_errores = ManejadorErrores("ConexionBD")
            cls.__instancia.__inicializar_conexion()
        return cls.__instancia

    @registrar_operacion("inicialización de conexión a base de datos")
    def __inicializar_conexion(self):
        """
        Inicializa la conexión a la base de datos utilizando SQLAlchemy.
        Las credenciales se cargan desde el archivo .env.
        """
        try:
            load_dotenv()  # Carga las variables del archivo .env
            self.logger.debug("Variables de entorno cargadas desde .env")

            # Obtener credenciales de las variables de entorno
            usuario = os.getenv("DB_USER")
            contrasena = os.getenv("DB_PASSWORD")
            host = os.getenv("DB_HOST")
            puerto = os.getenv("DB_PORT", "3306")
            nombre_bd = os.getenv("DB_NAME")
            
            self.logger.debug(f"Configuración BD - Host: {host}:{puerto}, BD: {nombre_bd}, Usuario: {usuario}")

            # Validar que todas las credenciales estén presentes
            if not all([usuario, contrasena, host, nombre_bd]):
                credenciales_faltantes = [
                    var for var, val in [
                        ("DB_USER", usuario), ("DB_PASSWORD", contrasena),
                        ("DB_HOST", host), ("DB_NAME", nombre_bd)
                    ] if not val
                ]
                raise ValueError(f"Faltan variables de entorno: {', '.join(credenciales_faltantes)}")

            # Construir cadena de conexión
            cadena_conexion = f"mysql+pymysql://{usuario}:{contrasena}@{host}:{puerto}/{nombre_bd}"
            
            # Crear motor con configuraciones optimizadas
            self.__motor = create_engine(
                cadena_conexion,
                echo=False,  # El logging lo manejamos nosotros
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verificar conexiones antes de usar
                pool_recycle=3600,   # Reciclar conexiones cada hora
                connect_args={
                    "charset": "utf8mb4",
                    "use_unicode": True,
                    "autocommit": True
                }
            )
            
            # Probar la conexión
            with self.__motor.connect() as conexion_prueba:
                resultado_prueba = conexion_prueba.execute("SELECT 1 as prueba")
                self.logger.debug("Prueba de conexión exitosa")
            
            # Crear una sesión
            Session = sessionmaker(bind=self.__motor)
            self.__sesion = Session()
            
            self.logger.info("Conexión a la base de datos establecida exitosamente")
            self.__registrar_metricas_iniciales()

        except Exception as e:
            self.manejador_errores.manejar_error_critico(e)
            self.__motor = None
            self.__sesion = None
            raise

    def __registrar_metricas_iniciales(self):
        """Registra métricas iniciales del sistema."""
        try:
            # Métricas de memoria
            proceso = psutil.Process()
            memoria_mb = proceso.memory_info().rss / 1024 / 1024
            self.estadisticas.registrar_uso_memoria(memoria_mb)
            
            # Métricas de BD
            self.estadisticas.registrar_metricas_base_datos(
                conexiones_activas=1,
                consultas_ejecutadas=self.__contador_consultas
            )
        except Exception as e:
            self.logger.warning(f"No se pudieron registrar métricas iniciales: {e}")
                      
    def obtener_motor(self):
        """
        Retorna el motor de SQLAlchemy.
        
        Returns:
            sqlalchemy.Engine: Motor de conexión a la BD o None si falló la conexión
        """
        if self.__motor is None:
            self.logger.warning("Motor de BD no está disponible - conexión falló durante inicialización")
        return self.__motor

    def obtener_sesion(self):
        """
        Retorna la sesión de SQLAlchemy.
        
        Returns:
            sqlalchemy.orm.Session: Sesión activa o None si no está disponible
        """
        if self.__sesion is None and self.__motor is not None:            
            try:
                Session = sessionmaker(bind=self.__motor)
                self.__sesion = Session()
                self.logger.debug("Nueva sesión de BD creada")
            except Exception as e:
                self.manejador_errores.manejar_error_bd(e)
                return None
        return self.__sesion

    @registrar_operacion("cierre de sesión de base de datos")
    def cerrar_sesion(self):
        """
        Cierra la sesión de SQLAlchemy si está abierta.
        """
        if self.__sesion:
            try:
                self.__sesion.close()
                self.logger.info("Sesión de base de datos cerrada correctamente")
                self.__sesion = None
            except Exception as e:
                self.manejador_errores.manejar_error_bd(e)
                self.__sesion = None  # Forzar reset en caso de error

    @registrar_consulta_sql
    def ejecutar_consulta(self, consulta_sql: str, params: dict = None) -> pd.DataFrame | None:
        """
        Ejecuta una consulta SQL y retorna los resultados como un DataFrame de pandas.
        Incluye logging detallado y manejo de errores robusto.

        Args:
            consulta_sql (str): La consulta SQL a ejecutar.
            params (dict, optional): Parámetros para la consulta SQL. Defaults to None.

        Returns:
            pd.DataFrame | None: Un DataFrame con los resultados o None si hay un error.
        """
        if self.__motor is None:
            self.logger.error("No hay conexión establecida con la base de datos")
            return None
        
        inicio_consulta = datetime.now()
        tipo_consulta = consulta_sql.strip().split()[0].upper()  # SELECT, INSERT, etc.
        
        try:
            # Incrementar contador de consultas
            self.__contador_consultas += 1
            
            # Ejecutar consulta
            df_resultado = pd.read_sql_query(consulta_sql, self.__motor, params=params)
            
            # Calcular métricas
            duracion = (datetime.now() - inicio_consulta).total_seconds()
            num_filas = len(df_resultado)
            
            # Registrar métricas de rendimiento
            self.estadisticas.registrar_rendimiento_consulta(tipo_consulta, duracion, num_filas)
            
            # Registrar métricas del sistema periódicamente
            if self.__contador_consultas % 10 == 0:
                self.__registrar_metricas_sistema()
            
            return df_resultado
            
        except Exception as e:
            duracion = (datetime.now() - inicio_consulta).total_seconds()
            self.manejador_errores.manejar_error_bd(e, consulta_sql)
            
            # Registrar consulta fallida
            self.estadisticas.registrar_rendimiento_consulta(f"{tipo_consulta}_ERROR", duracion, 0)
            
            return None

    def __registrar_metricas_sistema(self):
        """Registra métricas del sistema periódicamente."""
        try:
            # Métricas de memoria
            proceso = psutil.Process()
            memoria_mb = proceso.memory_info().rss / 1024 / 1024
            self.estadisticas.registrar_uso_memoria(memoria_mb)
            
            # Métricas de BD
            conexiones_activas = self.__motor.pool.size() if self.__motor else 0
            self.estadisticas.registrar_metricas_base_datos(
                conexiones_activas=conexiones_activas,
                consultas_ejecutadas=self.__contador_consultas
            )
            
        except Exception as e:
            self.logger.debug(f"Error registrando métricas: {e}")

    @registrar_operacion("validación de salud de conexión")
    def validar_salud_conexion(self) -> bool:
        """
        Valida que la conexión a la BD esté funcionando correctamente.
        
        Returns:
            bool: True si la conexión está saludable, False en caso contrario
        """
        if self.__motor is None:
            self.logger.warning("Motor de BD no disponible para validación de salud")
            return False
        
        try:
            # Ejecutar consulta simple de prueba
            with self.__motor.connect() as conexion:
                resultado = conexion.execute("SELECT 1 as salud_bd")
                valor = resultado.fetchone()[0]
                
                if valor == 1:
                    self.logger.debug("Validación de salud exitosa")
                    return True
                else:
                    self.logger.error("Validación de salud falló - resultado inesperado")
                    return False
                    
        except Exception as e:
            self.manejador_errores.manejar_error_bd(e)
            return False

    def obtener_estadisticas_conexion(self) -> dict:
        """
        Retorna estadísticas de la conexión actual.
        
        Returns:
            dict: Diccionario con estadísticas de la conexión
        """
        try:
            estadisticas = {
                'consultas_ejecutadas': self.__contador_consultas,
                'motor_disponible': self.__motor is not None,
                'sesion_activa': self.__sesion is not None,
                'pool_size': self.__motor.pool.size() if self.__motor else 0,
                'conexiones_activas': self.__motor.pool.checkedout() if self.__motor else 0
            }
            
            # Métricas de memoria del proceso
            try:
                proceso = psutil.Process()
                estadisticas['memoria_mb'] = round(proceso.memory_info().rss / 1024 / 1024, 2)
                estadisticas['cpu_percent'] = round(proceso.cpu_percent(), 2)
            except:
                estadisticas['memoria_mb'] = 'N/A'
                estadisticas['cpu_percent'] = 'N/A'
            
            self.logger.debug(f"Estadísticas de conexión: {estadisticas}")
            return estadisticas
            
        except Exception as e:
            self.logger.error(f"Error obteniendo estadísticas: {e}")
            return {}

    def __del__(self):
        """Destructor para limpiar recursos."""
        try:
            if self.__sesion:
                self.__sesion.close()
            self.logger.debug("Recursos de ConexionBD liberados")
        except:
            pass  # Ignorar errores en destructor


# Ejemplo de uso con logging
if __name__ == "__main__":
    from src.utils.sistema_logging import log_inicio_aplicacion, log_fin_aplicacion
    
    # Inicializar logging
    log_inicio_aplicacion()
    
    try:
        # Obtener instancias Singleton
        conector1 = ConexionBD()
        conector2 = ConexionBD()

        print(f"Motor 1: {conector1.obtener_motor()}")
        print(f"Motor 2: {conector2.obtener_motor()}")
        print(f"Son la misma instancia? {conector1 is conector2}")

        if conector1.obtener_motor():
            # Validar salud
            salud = conector1.validar_salud_conexion()
            print(f"Salud de conexión: {salud}")
            
            # Ejemplo de consulta
            df_empleados = conector1.ejecutar_consulta("SELECT * FROM employees LIMIT 5;")
            if df_empleados is not None:
                print(f"\nPrimeros 5 empleados: {len(df_empleados)} filas")
                print(df_empleados)
            
            # Mostrar estadísticas
            stats = conector1.obtener_estadisticas_conexion()
            print(f"\nEstadísticas: {stats}")
        
            # Cerrar la sesión cuando ya no se necesite 
            conector1.cerrar_sesion()
            
    except Exception as e:
        print(f"Error en ejemplo: {e}")
    
    finally:
        log_fin_aplicacion()