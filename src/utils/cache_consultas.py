import hashlib
import pickle
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
import pandas as pd
from functools import wraps
import threading
import logging

# Importar sistema de logging
# Se asume que sistema_logging.py está en src/utils/
try:
    from .sistema_logging import (
        ConfiguradorLogging,
        registrar_operacion,
        RegistradorEstadisticas
    )
except ImportError:
    # Fallback para si se ejecuta el script directamente y el import relativo falla
    # (Esto podría necesitar ajuste dependiendo de cómo se ejecute)
    from sistema_logging import (
        ConfiguradorLogging,
        registrar_operacion,
        RegistradorEstadisticas
    )


class GestorCacheConsultas:
    """
    Gestor de cache para consultas SQL que mejora el rendimiento
    del sistema almacenando resultados de consultas frecuentes.

    Implementa el patrón Singleton para mantener un cache único
    y consistente en toda la aplicación.
    """

    _instancia: Optional['GestorCacheConsultas'] = None
    _lock = threading.Lock()

    def __new__(cls) -> 'GestorCacheConsultas':
        if cls._instancia is None:
            with cls._lock:
                if cls._instancia is None:
                    cls._instancia = super(GestorCacheConsultas, cls).__new__(cls)
                    cls._instancia._inicializado = False
        return cls._instancia

    def __init__(self):
        if not getattr(self, '_inicializado', False):
            self._inicializar_cache()
            self._inicializado = True

    def _inicializar_cache(self):
        """Inicializa el sistema de cache."""
        self.logger = ConfiguradorLogging.obtener_logger("GestorCache")
        self.estadisticas = RegistradorEstadisticas()

        # Configuración del cache
        self.cache_habilitado = os.getenv('CACHE_CONSULTAS_HABILITADO', 'True').lower() == 'true'
        self.duracion_cache_minutos = int(os.getenv('CACHE_DURACION_MINUTOS', '15'))
        self.tamaño_maximo_cache = int(os.getenv('CACHE_TAMAÑO_MAXIMO', '100')) # Default 100 si no está en .env

        # Directorio para cache persistente
        self.directorio_cache = Path("cache") # Relativo al directorio de ejecución
        self.directorio_cache.mkdir(exist_ok=True)

        # Cache en memoria (más rápido)
        self._cache_memoria: Dict[str, Tuple[pd.DataFrame, datetime, int]] = {}

        # Estadísticas
        self.hits_cache = 0
        self.misses_cache = 0
        self.consultas_cacheadas = 0
        self.ultimo_cleanup = datetime.now()

        # Configurar limpieza automática cada hora
        self._programar_limpieza_automatica()

        self.logger.info(f"Cache de consultas inicializado - Habilitado: {self.cache_habilitado}")
        self.logger.info(f"Configuración: duración={self.duracion_cache_minutos}min, tamaño_max={self.tamaño_maximo_cache}")

    def _generar_clave_cache(self, consulta_sql: str, parametros: dict = None) -> str:
        """
        Genera una clave única para la consulta y sus parámetros.
        Args:
            consulta_sql (str): La consulta SQL
            parametros (dict): Parámetros de la consulta
        Returns:
            str: Clave hash única para el cache
        """
        consulta_normalizada = ' '.join(consulta_sql.lower().split())
        contenido_cache = consulta_normalizada
        if parametros:
            params_ordenados = sorted(parametros.items())
            contenido_cache += str(params_ordenados)

        hash_objeto = hashlib.sha256(contenido_cache.encode('utf-8'))
        clave_cache = hash_objeto.hexdigest()[:16] # Usar solo primeros 16 caracteres
        self.logger.debug(f"Clave de cache generada: {clave_cache} para consulta: {consulta_sql[:50]}...")
        return clave_cache

    def _es_consulta_cacheable(self, consulta_sql: str) -> bool:
        """
        Determina si una consulta puede ser cacheada.
        Args:
            consulta_sql (str): La consulta SQL
        Returns:
            bool: True si la consulta puede ser cacheada
        """
        consulta_upper = consulta_sql.upper().strip()
        if not consulta_upper.startswith('SELECT'):
            return False
        funciones_tiempo = ['NOW()', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME', 'RAND()']
        if any(funcion in consulta_upper for funcion in funciones_tiempo):
            self.logger.debug("Consulta no cacheable: contiene funciones de tiempo/aleatorias")
            return False
        if len(consulta_sql) > 5000: # Evitar cachear consultas SQL extremadamente largas
            self.logger.debug("Consulta no cacheable: demasiado grande")
            return False
        return True

    @registrar_operacion("búsqueda en cache de consulta")
    def obtener_desde_cache(self, consulta_sql: str, parametros: dict = None) -> Optional[pd.DataFrame]:
        """
        Intenta obtener el resultado de una consulta desde el cache.
        """
        if not self.cache_habilitado:
            return None
        if not self._es_consulta_cacheable(consulta_sql):
            return None

        clave_cache = self._generar_clave_cache(consulta_sql, parametros)

        # Buscar en cache de memoria primero
        if clave_cache in self._cache_memoria:
            resultado, timestamp, _ = self._cache_memoria[clave_cache]
            if datetime.now() - timestamp < timedelta(minutes=self.duracion_cache_minutos):
                self.hits_cache += 1
                self.logger.debug(f"HIT de cache en memoria: {clave_cache}")
                self.estadisticas.registrar_rendimiento_consulta("CACHE_HIT_MEMORIA", 0.001, len(resultado))
                return resultado.copy() # Retornar copia
            else:
                del self._cache_memoria[clave_cache]
                self.logger.debug(f"Cache en memoria expirado eliminado: {clave_cache}")

        # Buscar en cache persistente
        archivo_cache = self.directorio_cache / f"{clave_cache}.pkl"
        if archivo_cache.exists():
            try:
                tiempo_archivo = datetime.fromtimestamp(archivo_cache.stat().st_mtime)
                if datetime.now() - tiempo_archivo < timedelta(minutes=self.duracion_cache_minutos):
                    with open(archivo_cache, 'rb') as f:
                        resultado_disco = pickle.load(f)
                    # Agregar de vuelta al cache de memoria
                    tamaño_bytes_disco = archivo_cache.stat().st_size
                    self._cache_memoria[clave_cache] = (resultado_disco, datetime.now(), tamaño_bytes_disco)
                    self.hits_cache += 1
                    self.logger.debug(f"HIT de cache persistente y cargado a memoria: {clave_cache}")
                    self.estadisticas.registrar_rendimiento_consulta("CACHE_HIT_DISCO", 0.005, len(resultado_disco))
                    return resultado_disco.copy()
                else:
                    archivo_cache.unlink()
                    self.logger.debug(f"Archivo de cache persistente expirado eliminado: {archivo_cache}")
            except Exception as e:
                self.logger.warning(f"Error leyendo cache persistente {clave_cache}: {e}")
                try:
                    archivo_cache.unlink(missing_ok=True) # Python 3.8+
                except FileNotFoundError: # Para Python < 3.8
                    if archivo_cache.exists(): archivo_cache.unlink()


        self.misses_cache += 1
        return None

    @registrar_operacion("almacenamiento en cache de consulta")
    def guardar_en_cache(self, consulta_sql: str, resultado: pd.DataFrame, parametros: dict = None):
        """
        Guarda el resultado de una consulta en el cache.
        """
        if not self.cache_habilitado or not self._es_consulta_cacheable(consulta_sql) or resultado is None or resultado.empty:
            if resultado is None or resultado.empty:
                 self.logger.debug("No se cachea resultado vacío o None.")
            return

        clave_cache = self._generar_clave_cache(consulta_sql, parametros)
        try:
            datos_serializados = pickle.dumps(resultado)
            tamaño_bytes = len(datos_serializados)
            limite_tamaño_mb = int(os.getenv('CACHE_LIMITE_TAMAÑO_MB', '50')) # Default 50MB si no está en .env
            if tamaño_bytes > limite_tamaño_mb * 1024 * 1024:
                self.logger.debug(f"Resultado demasiado grande para cache: {tamaño_bytes / (1024*1024):.1f}MB")
                return

            timestamp = datetime.now()
            self._cache_memoria[clave_cache] = (resultado.copy(), timestamp, tamaño_bytes)

            archivo_cache = self.directorio_cache / f"{clave_cache}.pkl"
            with open(archivo_cache, 'wb') as f:
                f.write(datos_serializados)

            self.consultas_cacheadas += 1
            self.logger.debug(f"Resultado cacheado: {clave_cache} ({tamaño_bytes / 1024:.1f}KB, {len(resultado)} filas)")

            if len(self._cache_memoria) > self.tamaño_maximo_cache:
                self._limpiar_cache_exceso()
        except Exception as e:
            self.logger.warning(f"Error guardando en cache {clave_cache}: {e}")

    def _limpiar_cache_exceso(self):
        """Limpia entradas más antiguas del cache cuando excede el tamaño máximo."""
        if len(self._cache_memoria) <= self.tamaño_maximo_cache:
            return
        self.logger.info(f"Limpiando cache por exceso de tamaño. Actual: {len(self._cache_memoria)}, Máximo: {self.tamaño_maximo_cache}")
        items_ordenados = sorted(self._cache_memoria.items(), key=lambda x: x[1][1]) # x[1][1] es el timestamp
        
        # Eliminar 20% de las entradas más antiguas o al menos el exceso.
        cantidad_a_eliminar = max(
            len(self._cache_memoria) - self.tamaño_maximo_cache,
            int(len(self._cache_memoria) * 0.2)
        )
        cantidad_a_eliminar = min(cantidad_a_eliminar, len(items_ordenados)) # No eliminar más de lo que hay


        eliminados_count = 0
        for i in range(cantidad_a_eliminar):
            clave_antigua = items_ordenados[i][0]
            if clave_antigua in self._cache_memoria:
                del self._cache_memoria[clave_antigua]
                eliminados_count +=1
                archivo_cache = self.directorio_cache / f"{clave_antigua}.pkl"
                try:
                    archivo_cache.unlink(missing_ok=True) # Python 3.8+
                except FileNotFoundError: # Para Python < 3.8
                    if archivo_cache.exists(): archivo_cache.unlink()
                except Exception as e_unlink:
                    self.logger.warning(f"Error eliminando archivo de cache {archivo_cache} durante limpieza por exceso: {e_unlink}")


        if eliminados_count > 0:
            self.logger.info(f"Cache limpiado por exceso: eliminadas {eliminados_count} entradas más antiguas.")


    def _programar_limpieza_automatica(self):
        """Programa limpieza automática del cache."""
        def limpieza_periodica():
            try:
                self.limpiar_cache_expirado()
                # Reprogramar para la próxima hora, asegurando que sea un hilo daemon
                timer = threading.Timer(3600, limpieza_periodica)
                timer.daemon = True # Para que no impida que el programa termine
                timer.start()
            except Exception as e:
                self.logger.error(f"Error en limpieza automática de cache: {e}")

        # Iniciar timer para limpieza cada hora
        timer_inicial = threading.Timer(3600, limpieza_periodica)
        timer_inicial.daemon = True
        timer_inicial.start()
        self.logger.debug("Limpieza automática de cache programada cada hora.")

    @registrar_operacion("limpieza de cache expirado")
    def limpiar_cache_expirado(self):
        """Limpia todas las entradas expiradas del cache."""
        ahora = datetime.now()
        limite_expiracion = timedelta(minutes=self.duracion_cache_minutos)
        claves_expiradas_memoria = [
            clave for clave, (_, timestamp, _) in self._cache_memoria.items()
            if ahora - timestamp > limite_expiracion
        ]
        for clave in claves_expiradas_memoria:
            if clave in self._cache_memoria: # Doble check por si fue eliminado por otra operación
                del self._cache_memoria[clave]

        archivos_eliminados_disco = 0
        for archivo_cache in self.directorio_cache.glob("*.pkl"):
            try:
                tiempo_archivo = datetime.fromtimestamp(archivo_cache.stat().st_mtime)
                if ahora - tiempo_archivo > limite_expiracion:
                    archivo_cache.unlink()
                    archivos_eliminados_disco += 1
            except Exception as e:
                self.logger.warning(f"Error eliminando archivo de cache expirado {archivo_cache}: {e}")
        self.ultimo_cleanup = ahora
        self.logger.info(f"Limpieza de cache expirado completada: {len(claves_expiradas_memoria)} en memoria, {archivos_eliminados_disco} en disco.")

    def obtener_estadisticas_cache(self) -> Dict[str, Any]:
        """
        Retorna estadísticas del cache.
        """
        total_consultas_intentadas = self.hits_cache + self.misses_cache
        ratio_hit = (self.hits_cache / total_consultas_intentadas * 100) if total_consultas_intentadas > 0 else 0
        tamaño_total_memoria_bytes = sum(tamaño for _, _, tamaño in self._cache_memoria.values())
        tamaño_total_memoria_mb = tamaño_total_memoria_bytes / (1024 * 1024)
        archivos_cache_persistente = len(list(self.directorio_cache.glob("*.pkl")))

        return {
            'cache_habilitado': self.cache_habilitado,
            'hits_cache': self.hits_cache,
            'misses_cache': self.misses_cache,
            'ratio_hit_porcentaje': round(ratio_hit, 2),
            'consultas_cacheadas_exitosamente': self.consultas_cacheadas,
            'entradas_en_memoria': len(self._cache_memoria),
            'archivos_en_disco': archivos_cache_persistente,
            'tamaño_total_en_memoria_mb': round(tamaño_total_memoria_mb, 2),
            'ultimo_cleanup_expirados': self.ultimo_cleanup.isoformat(),
            'configuracion': {
                'duracion_minutos': self.duracion_cache_minutos,
                'tamaño_maximo_entradas_memoria': self.tamaño_maximo_cache,
                'limite_tamaño_individual_mb': int(os.getenv('CACHE_LIMITE_TAMAÑO_MB', '50')),
                'directorio_cache_disco': str(self.directorio_cache.resolve())
            }
        }

    def invalidar_cache(self, patron: str = None):
        """
        Invalida entradas del cache.
        Args:
            patron (str): Si se provee, invalida claves que contengan este patrón (case-insensitive).
                          Si es None, invalida todo el cache.
        """
        with self._lock: # Asegurar operación atómica en el diccionario de memoria
            if patron:
                patron_lower = patron.lower()
                claves_a_eliminar_memoria = [
                    clave for clave in self._cache_memoria.keys()
                    if patron_lower in self._generar_clave_cache(clave.split('_#_')[0] if '_#_' in clave else clave).lower() # Asume que la clave original es parte de la consulta
                                                                                                                            # o una heurística más simple: if patron_lower in clave.lower()
                ]
                # Para archivos, la clave es el hash. Invalidar por patrón de consulta es más complejo
                # aquí se invalidará si el patrón está en el nombre del archivo (que es el hash).
                # Lo ideal sería guardar metadatos consulta->hash para invalidar por consulta.
                # Por ahora, si hay patrón, limpiamos memoria y dejamos que el disco expire o se limpie después.
                # Una implementación más robusta requeriría un índice inverso.
                # O, si la clave del caché es el hash de la consulta, podemos re-generar hashes de consultas conocidas que matcheen el patron.
                # Simplificación: por ahora, patrón solo afecta memoria de forma precisa. Para disco, es más complejo
                # sin un mapeo. La forma más segura es limpiar todo si el patrón es crítico.

                for clave in claves_a_eliminar_memoria:
                    if clave in self._cache_memoria:
                        del self._cache_memoria[clave]
                    archivo_cache_a_eliminar = self.directorio_cache / f"{clave}.pkl"
                    try:
                        archivo_cache_a_eliminar.unlink(missing_ok=True)
                    except Exception as e_unlink:
                         self.logger.warning(f"Error al intentar eliminar archivo de cache {archivo_cache_a_eliminar} durante invalidación por patrón: {e_unlink}")


                self.logger.info(f"Cache de memoria invalidado para patrón '{patron}': {len(claves_a_eliminar_memoria)} entradas. Revisar cache en disco manualmente si es necesario.")
            else:
                self._cache_memoria.clear()
                archivos_eliminados_disco_total = 0
                for archivo_cache in self.directorio_cache.glob("*.pkl"):
                    try:
                        archivo_cache.unlink()
                        archivos_eliminados_disco_total +=1
                    except Exception as e:
                        self.logger.warning(f"Error eliminando archivo de cache {archivo_cache} durante invalidación total: {e}")
                self.logger.info(f"Todo el cache ha sido invalidado (memoria y {archivos_eliminados_disco_total} archivos en disco).")


def cache_consulta(duracion_minutos: int = None): # El default vendrá de la config del gestor
    """
    Decorador para aplicar cache automáticamente a métodos que ejecutan consultas.
    """
    def decorador(func):
        @wraps(func)
        def wrapper(self_obj, consulta_sql: str, parametros: dict = None, *args, **kwargs):
            # self_obj es la instancia de la clase donde se aplica el decorador (ej. ConexionBD)
            gestor_cache = GestorCacheConsultas() # Obtiene el Singleton

            # Usar duración específica del decorador si se provee, sino la global del gestor
            duracion_cache_actual = duracion_minutos if duracion_minutos is not None else gestor_cache.duracion_cache_minutos

            # Para que la expiración sea respetada, el gestor debería poder manejar duraciones variables por clave
            # o el decorador manejar su propio mini-cache o no usar el del gestor si las duraciones difieren.
            # La implementación actual de GestorCache usa una duración global para la expiración.
            # Para simplicidad, el decorador usará el gestor global. Si `duracion_minutos` es
            # diferente, implicaría una lógica más compleja en el gestor o que este decorador
            # no use el gestor global sino su propio mecanismo simple (menos ideal).

            # Solución simple: el decorador no anula la duración global del gestor, solo decide si cachear o no.
            # O, el gestor necesitaría un método obtener_desde_cache_con_duracion_especifica.
            # Por ahora, el decorador usará la lógica del gestor.

            resultado_cache = gestor_cache.obtener_desde_cache(consulta_sql, parametros) # Usa la duración global del gestor
            if resultado_cache is not None:
                # self_obj.logger.debug(f"Resultado para '{consulta_sql[:30]}...' servido desde cache por decorador.")
                return resultado_cache

            resultado_real = func(self_obj, consulta_sql, parametros, *args, **kwargs)

            if resultado_real is not None and not resultado_real.empty:
                # self_obj.logger.debug(f"Resultado para '{consulta_sql[:30]}...' obtenido de BD y guardado en cache por decorador.")
                gestor_cache.guardar_en_cache(consulta_sql, resultado_real, parametros) # Usa la duración global del gestor

            return resultado_real
        return wrapper
    return decorador

if __name__ == "__main__":
    # Para ejecutar este script directamente, necesitas que 'sistema_logging.py' sea importable.
    # Esto usualmente significa que el directorio 'src/utils' debe estar en PYTHONPATH
    # o que ejecutes python -m src.utils.cache_consultas desde el directorio raíz del proyecto.
    try:
        from sistema_logging import log_inicio_aplicacion, log_fin_aplicacion
        log_inicio_aplicacion()
    except ImportError:
        logging.basicConfig(level=logging.DEBUG)
        logging.info("Logging básico configurado para prueba de cache_consultas.")


    cache_manager = GestorCacheConsultas()

    consulta_test_1 = "SELECT * FROM tabla_test WHERE id = 1"
    params_test_1 = {'id_param': 1}
    df_test_1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

    print("--- Prueba Cache ---")
    print("1. Obtener (miss):")
    res = cache_manager.obtener_desde_cache(consulta_test_1, params_test_1)
    print(f"Resultado: {'HIT' if res is not None else 'MISS'}")

    print("\n2. Guardar:")
    cache_manager.guardar_en_cache(consulta_test_1, df_test_1, params_test_1)

    print("\n3. Obtener (hit en memoria):")
    res = cache_manager.obtener_desde_cache(consulta_test_1, params_test_1)
    print(f"Resultado: {'HIT' if res is not None else 'MISS'}")
    if res is not None:
        print(res)

    # Simular expiración del caché en memoria para forzar lectura de disco
    print("\n4. Forzar 'expiración' de caché en memoria y leer de disco:")
    clave_generada = cache_manager._generar_clave_cache(consulta_test_1, params_test_1)
    if clave_generada in cache_manager._cache_memoria:
        del cache_manager._cache_memoria[clave_generada]
        print(f"   Entrada '{clave_generada}' eliminada del caché en memoria.")

    res_disco = cache_manager.obtener_desde_cache(consulta_test_1, params_test_1)
    print(f"Resultado (debería ser HIT de disco y recargado a memoria): {'HIT' if res_disco is not None else 'MISS'}")
    if res_disco is not None:
        print(res_disco)


    print(f"\nEstadísticas Pre-limpieza: {cache_manager.obtener_estadisticas_cache()['entradas_en_memoria']} en memoria.")
    cache_manager.limpiar_cache_expirado() # Asumiendo que la duración por defecto es corta para la prueba o ajustar.
    print(f"Estadísticas Post-limpieza: {cache_manager.obtener_estadisticas_cache()['entradas_en_memoria']} en memoria.")


    # Ejemplo con decorador (requeriría una clase con un logger)
    class DBMock:
        def __init__(self):
            self.logger = ConfiguradorLogging.obtener_logger("DBMock") # Asume que ConfiguradorLogging está disponible

        @cache_consulta()
        def get_data(self, query, params=None):
            self.logger.info(f"DBMock: Ejecutando consulta original para: {query}")
            time.sleep(0.1) # Simular latencia de BD
            return pd.DataFrame({'datos_mock': [query, str(params)]})

    print("\n--- Prueba Decorador ---")
    db_mock = DBMock() # Necesita ConfiguradorLogging para funcionar
    print("Llamada 1 decorada (MISS):")
    df_deco1 = db_mock.get_data("SELECT * FROM decorado WHERE id=100")
    print(df_deco1.head())
    print("\nLlamada 2 decorada (HIT):")
    df_deco2 = db_mock.get_data("SELECT * FROM decorado WHERE id=100")
    print(df_deco2.head())


    print("\n--- Estadísticas Finales del Cache ---")
    stats_final = cache_manager.obtener_estadisticas_cache()
    for key, value in stats_final.items():
        if isinstance(value, dict):
            print(f"  {key}:")
            for sub_key, sub_value in value.items():
                print(f"    {sub_key}: {sub_value}")
        else:
            print(f"  {key}: {value}")

    try:
        log_fin_aplicacion()
    except NameError:
        logging.info("Fin de prueba de cache_consultas.")