import pymysql
from sqlalchemy import create_engine, text 
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import psutil

from src.utils.sistema_logging import (
    ConfiguradorLogging,
    registrar_operacion,
    registrar_consulta_sql,
    RegistradorEstadisticas,
    ManejadorErrores
)
from src.utils.cache_consultas import GestorCacheConsultas, cache_consulta


class ConexionBD:
    """
    Clase Singleton para manejar la conexión a la base de datos MySQL.
    Incluye sistema de logging profesional, cache de consultas y manejo de errores robusto.
    """
    __instancia = None
    __motor = None
    __sesion = None
    __contador_consultas = 0

    def __new__(cls):
        if cls.__instancia is None:
            cls.__instancia = super(ConexionBD, cls).__new__(cls)
            cls.__instancia.logger = ConfiguradorLogging.obtener_logger("ConexionBD")
            cls.__instancia.estadisticas = RegistradorEstadisticas()
            cls.__instancia.manejador_errores = ManejadorErrores("ConexionBD")
            cls.__instancia.gestor_cache = GestorCacheConsultas()
            cls.__instancia.__inicializar_conexion()
        return cls.__instancia

    @registrar_operacion("inicialización de conexión a base de datos")
    def __inicializar_conexion(self):
        try:
            load_dotenv()
            self.logger.debug("Variables de entorno cargadas desde .env")

            usuario = os.getenv("DB_USER")
            contrasena = os.getenv("DB_PASSWORD")
            host = os.getenv("DB_HOST")
            puerto = os.getenv("DB_PORT", "3306")
            nombre_bd = os.getenv("DB_NAME")
            self.logger.debug(f"Configuración BD - Host: {host}:{puerto}, BD: {nombre_bd}, Usuario: {usuario}")

            if not all([usuario, contrasena, host, nombre_bd]):
                credenciales_faltantes = [
                    var for var, val in [
                        ("DB_USER", usuario), ("DB_PASSWORD", contrasena),
                        ("DB_HOST", host), ("DB_NAME", nombre_bd)
                    ] if not val
                ]
                raise ValueError(f"Faltan variables de entorno para BD: {', '.join(credenciales_faltantes)}")

            pool_size = int(os.getenv('DB_POOL_SIZE', '5'))
            max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '10'))
            query_timeout_seconds = int(os.getenv('DB_QUERY_TIMEOUT', '30'))

            cadena_conexion = f"mysql+pymysql://{usuario}:{contrasena}@{host}:{puerto}/{nombre_bd}"
            connect_args_dict = {
                "charset": "utf8mb4",
                "use_unicode": True,
                "autocommit": True
            }

            self.__motor = create_engine(
                cadena_conexion,
                echo=False,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args=connect_args_dict
            )

            # Probar la conexión usando text()
            with self.__motor.connect() as conexion_prueba:
                conexion_prueba.execute(text("SELECT 1")) 
            self.logger.debug("Prueba de conexión directa al motor exitosa.")

            Session = sessionmaker(bind=self.__motor)
            self.__sesion = Session()

            self.logger.info("Conexión a la base de datos establecida exitosamente.")
            self.logger.info(f"Pool configurado: size={pool_size}, overflow={max_overflow}, query_timeout_config={query_timeout_seconds}s")
            self.__registrar_metricas_iniciales()

        except Exception as e:
            self.manejador_errores.manejar_error_critico(e)
            self.__motor = None
            self.__sesion = None
            raise

    def __registrar_metricas_iniciales(self):
        try:
            proceso = psutil.Process()
            memoria_mb = proceso.memory_info().rss / (1024 * 1024)
            self.estadisticas.registrar_uso_memoria(memoria_mb)
            self.estadisticas.registrar_metricas_base_datos(
                conexiones_activas=1,
                consultas_ejecutadas=self.__contador_consultas
            )
            stats_cache = self.gestor_cache.obtener_estadisticas_cache()
            self.logger.info(f"Cache inicializado - Habilitado: {stats_cache.get('cache_habilitado', 'N/A')}")
        except Exception as e:
            self.logger.warning(f"No se pudieron registrar métricas iniciales completas: {e}")

    def obtener_motor(self):
        if self.__motor is None:
            self.logger.warning("Motor de BD no está disponible.")
        return self.__motor

    def obtener_sesion(self):
        if self.__sesion is None and self.__motor is not None:
            try:
                Session = sessionmaker(bind=self.__motor)
                self.__sesion = Session()
                self.logger.debug("Nueva sesión de BD creada.")
            except Exception as e:
                self.manejador_errores.manejar_error_bd(e)
                return None
        elif self.__sesion and not self.__sesion.is_active and self.__motor:
            self.logger.warning("Sesión de BD detectada como inactiva, reabriendo.")
            try:
                Session = sessionmaker(bind=self.__motor)
                self.__sesion = Session()
            except Exception as e:
                self.manejador_errores.manejar_error_bd(e, "Intentando reabrir sesión inactiva")
                return None
        return self.__sesion

    @registrar_operacion("cierre de sesión de base de datos")
    def cerrar_sesion(self):
        if self.__sesion:
            try:
                self.__sesion.close()
                self.logger.info("Sesión de base de datos cerrada correctamente.")
            except Exception as e:
                self.manejador_errores.manejar_error_bd(e)
            finally:
                self.__sesion = None

    @registrar_consulta_sql
    def ejecutar_consulta(self, consulta_sql: str, params: dict = None, usar_cache: bool = True) -> pd.DataFrame | None:
        if self.__motor is None:
            self.logger.error("No hay conexión (motor) establecida con la base de datos.")
            return None

        inicio_operacion = datetime.now()
        tipo_consulta_simple = consulta_sql.strip().split(None, 1)[0].upper()

        if usar_cache and tipo_consulta_simple == 'SELECT':
            resultado_cache = self.gestor_cache.obtener_desde_cache(consulta_sql, params)
            if resultado_cache is not None:
                duracion_total = (datetime.now() - inicio_operacion).total_seconds()
                self.logger.info(f"HIT DE CACHE para consulta: {consulta_sql[:60]}... ({duracion_total:.4f}s)")
                return resultado_cache

        df_resultado = None
        try:
            self.__contador_consultas += 1
            df_resultado = pd.read_sql_query(consulta_sql, self.__motor, params=params)

            if usar_cache and tipo_consulta_simple == 'SELECT' and df_resultado is not None and not df_resultado.empty:
                self.gestor_cache.guardar_en_cache(consulta_sql, df_resultado, params)

            if self.__contador_consultas % 20 == 0:
                self.__registrar_metricas_sistema()
            return df_resultado
        except Exception as e:
            self.manejador_errores.manejar_error_bd(e, consulta_sql)
            return None

    @cache_consulta(duracion_minutos=30)
    def ejecutar_consulta_analisis(self, consulta_sql: str, params: dict = None) -> pd.DataFrame | None:
        self.logger.info(f"Ejecutando consulta de análisis (cache manejado por decorador): {consulta_sql[:60]}...")
        return self.ejecutar_consulta(consulta_sql, params, usar_cache=False)

    def ejecutar_consulta_sin_cache(self, consulta_sql: str, params: dict = None) -> pd.DataFrame | None:
        self.logger.debug(f"Ejecutando consulta explícitamente sin cache: {consulta_sql[:60]}...")
        return self.ejecutar_consulta(consulta_sql, params, usar_cache=False)

    def invalidar_cache_tabla(self, nombre_tabla: str):
        self.logger.info(f"Solicitando invalidación de cache para la tabla: {nombre_tabla}")
        self.gestor_cache.invalidar_cache(patron=nombre_tabla.lower())

    def __registrar_metricas_sistema(self):
        try:
            proceso = psutil.Process()
            memoria_mb = proceso.memory_info().rss / (1024 * 1024)
            limite_memoria_alerta = int(os.getenv('MEMORIA_LIMITE_ALERTA', '500'))
            if memoria_mb > limite_memoria_alerta:
                self.logger.warning(f"Uso de memoria ALTO: {memoria_mb:.1f}MB (Límite de alerta: {limite_memoria_alerta}MB)")
            self.estadisticas.registrar_uso_memoria(memoria_mb)

            conexiones_pool = self.__motor.pool.size() if self.__motor and hasattr(self.__motor.pool, 'size') else 0
            self.estadisticas.registrar_metricas_base_datos(
                conexiones_activas=conexiones_pool,
                consultas_ejecutadas=self.__contador_consultas
            )
            stats_cache = self.gestor_cache.obtener_estadisticas_cache()
            if stats_cache.get('ratio_hit_porcentaje', 100) < 50 and self.__contador_consultas > 50 :
                self.logger.warning(f"Ratio de HIT del cache es BAJO: {stats_cache.get('ratio_hit_porcentaje', 0):.1f}% "
                                    f"tras {self.__contador_consultas} consultas.")
        except Exception as e:
            self.logger.debug(f"Error menor registrando métricas del sistema: {e}")

    @registrar_operacion("validación de salud de conexión")
    def validar_salud_conexion(self) -> bool:
        if self.__motor is None:
            self.logger.warning("Motor de BD no disponible para validación de salud.")
            return False
        try:
            sesion_prueba = self.obtener_sesion()
            if sesion_prueba and sesion_prueba.is_active:
                resultado_df = self.ejecutar_consulta_sin_cache("SELECT 1 AS salud")
                if resultado_df is not None and not resultado_df.empty and resultado_df.iloc[0]['salud'] == 1:
                    self.logger.debug("Validación de salud de conexión (con sesión y consulta) exitosa.")
                    return True
            self.logger.error("Validación de salud de conexión falló (sesión inactiva o consulta fallida).")
            return False
        except Exception as e:
            self.manejador_errores.manejar_error_bd(e, "SELECT 1 (Validación de salud)")
            return False

    def obtener_estadisticas_completas(self) -> dict:
        try:
            estadisticas_bd = {
                'consultas_ejecutadas_bd': self.__contador_consultas,
                'motor_disponible': self.__motor is not None,
                'sesion_disponible': self.__sesion is not None and self.__sesion.is_active,
            }
            if self.__motor and hasattr(self.__motor, 'pool'):
                pool = self.__motor.pool
                estadisticas_bd.update({
                    'pool_status': pool.status(),
                    'pool_size_config': pool.size(),
                    'pool_checkedout': pool.checkedout(),
                    'pool_overflow': pool.overflow(),
                })

            try:
                proceso = psutil.Process()
                estadisticas_bd['memoria_proceso_mb'] = round(proceso.memory_info().rss / (1024 * 1024), 2)
                estadisticas_bd['cpu_proceso_percent'] = round(proceso.cpu_percent(interval=0.1), 2)
            except Exception as e_psutil:
                self.logger.debug(f"No se pudo obtener info de psutil: {e_psutil}")
                estadisticas_bd['memoria_proceso_mb'] = 'N/A'
                estadisticas_bd['cpu_proceso_percent'] = 'N/A'

            estadisticas_cache = self.gestor_cache.obtener_estadisticas_cache()
            todas_las_estadisticas = {
                'timestamp_actual': datetime.now().isoformat(),
                'info_base_datos': estadisticas_bd,
                'info_cache_consultas': estadisticas_cache,
            }
            self.logger.debug("Estadísticas completas del sistema generadas.")
            return todas_las_estadisticas
        except Exception as e:
            self.logger.error(f"Error crítico al obtener estadísticas completas: {e}", exc_info=True)
            return {'error_estadisticas': str(e)}

    @registrar_operacion("optimización de rendimiento programada")
    def optimizar_rendimiento(self):
        self.logger.info("Iniciando tareas de optimización de rendimiento...")
        try:
            self.gestor_cache.limpiar_cache_expirado()
            if not self.validar_salud_conexion():
                self.logger.warning("Conexión a BD no saludable detectada durante optimización.")
            stats_post_opt = self.obtener_estadisticas_completas()
            self.logger.info(f"Optimización completada. Memoria actual: {stats_post_opt.get('info_base_datos', {}).get('memoria_proceso_mb', 'N/A')}MB. "
                             f"Entradas cache memoria: {stats_post_opt.get('info_cache_consultas', {}).get('entradas_en_memoria', 'N/A')}.")
        except Exception as e:
            self.logger.error(f"Error durante la optimización de rendimiento: {e}", exc_info=True)

    def __del__(self):
        try:
            if self.__sesion:
                self.cerrar_sesion()
            self.logger.debug("Recursos de ConexionBD liberados en __del__ (sesión cerrada).")
        except:
            pass

if __name__ == "__main__":
    # Ejecuta como módulo: python -m src.conexion_bd desde la raíz del proyecto.
    try:
        from src.utils.sistema_logging import log_inicio_aplicacion, log_fin_aplicacion
        log_inicio_aplicacion()
    except ImportError:
        import logging as py_logging # Renombrar para evitar conflicto con variable logging en otros lados
        py_logging.basicConfig(level=py_logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        py_logging.info("Logging básico configurado para prueba de ConexionBD.")

    try:
        conector_bd = ConexionBD()
        if conector_bd.obtener_motor():
            print(f"Salud de la conexión inicial: {conector_bd.validar_salud_conexion()}")
            consulta_prueba_cache = "SELECT EmployeeID, FirstName, LastName FROM employees LIMIT 2"
            print("\n--- Ejecutando consulta por primera vez (espera MISS de cache) ---")
            df_resultado1 = conector_bd.ejecutar_consulta(consulta_prueba_cache)
            if df_resultado1 is not None:
                print("Resultado 1 (desde BD):")
                print(df_resultado1.head())

            print("\n--- Ejecutando MISMA consulta por segunda vez (espera HIT de cache) ---")
            df_resultado2 = conector_bd.ejecutar_consulta(consulta_prueba_cache)
            if df_resultado2 is not None:
                print("Resultado 2 (desde Cache):")
                print(df_resultado2.head())

            consulta_analisis_test = "SELECT CategoryID, COUNT(*) AS TotalProducts FROM products GROUP BY CategoryID LIMIT 3"
            print("\n--- Ejecutando consulta de ANÁLISIS (usa decorador con cache diferente) ---")
            df_analisis1 = conector_bd.ejecutar_consulta_analisis(consulta_analisis_test)
            if df_analisis1 is not None:
                print("Resultado Análisis 1 (desde BD, cacheado por decorador):")
                print(df_analisis1.head())

            print("\n--- Ejecutando MISMA consulta de ANÁLISIS (espera HIT de cache del decorador) ---")
            df_analisis2 = conector_bd.ejecutar_consulta_analisis(consulta_analisis_test)
            if df_analisis2 is not None:
                print("Resultado Análisis 2 (desde Cache del decorador):")
                print(df_analisis2.head())

            print("\n--- Obteniendo estadísticas completas ---")
            estadisticas = conector_bd.obtener_estadisticas_completas()
            import json
            print(json.dumps(estadisticas, indent=2, default=str))

            print("\n--- Realizando optimización ---")
            conector_bd.optimizar_rendimiento()
            conector_bd.cerrar_sesion()
            print("\nSesión cerrada.")
    except ValueError as ve:
        print(f"ERROR DE CONFIGURACIÓN: {ve}. Asegúrate de tener .env configurado.")
    except Exception as e_main:
        print(f"ERROR en la demostración de ConexionBD: {e_main}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            log_fin_aplicacion()
        except NameError: # Si log_fin_aplicacion no fue importado
            if 'py_logging' in locals():
                 py_logging.info("Fin de prueba de ConexionBD.")
            else: # Último recurso
                 print("Fin de prueba de ConexionBD.")