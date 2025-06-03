import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from functools import wraps
import traceback


class ConfiguradorLogging:
    """
    Configurador centralizado del sistema de logging para el proyecto.
    Implementa el patrón Singleton para asegurar configuración única.
    """
    
    _instancia: Optional['ConfiguradorLogging'] = None
    _configurado: bool = False
    
    def __new__(cls) -> 'ConfiguradorLogging':
        if cls._instancia is None:
            cls._instancia = super(ConfiguradorLogging, cls).__new__(cls)
        return cls._instancia
    
    def __init__(self):
        if not self._configurado:
            self._configurar_logging()
            ConfiguradorLogging._configurado = True
    
    def _configurar_logging(self):
        """
        Configura el sistema de logging con múltiples handlers y formatos.
        """
        # Crear directorio de logs si no existe
        directorio_logs = Path("logs")
        directorio_logs.mkdir(exist_ok=True)
        
        # Configuración del nivel base
        nivel_logging = os.getenv('NIVEL_LOGGING', 'INFO').upper()
        nivel_numerico = getattr(logging, nivel_logging, logging.INFO)
        
        # Configurar el logger raíz del proyecto
        logger_raiz = logging.getLogger('sistema_ventas')
        logger_raiz.setLevel(nivel_numerico)
        
        # Limpiar handlers existentes para evitar duplicados
        logger_raiz.handlers.clear()
        
        # === HANDLER 1: Archivo rotativo para logs generales ===
        archivo_general = directorio_logs / "sistema_ventas.log"
        handler_archivo = logging.handlers.RotatingFileHandler(
            archivo_general,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        handler_archivo.setLevel(logging.INFO)
        
        # === HANDLER 2: Archivo específico para errores ===
        archivo_errores = directorio_logs / "errores.log"
        handler_errores = logging.handlers.RotatingFileHandler(
            archivo_errores,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=10,
            encoding='utf-8'
        )
        handler_errores.setLevel(logging.ERROR)
        
        # === HANDLER 3: Consola para desarrollo ===
        handler_consola = logging.StreamHandler(sys.stdout)
        handler_consola.setLevel(nivel_numerico)
        
        # === HANDLER 4: Archivo de debug (solo en desarrollo) ===
        if os.getenv('ENTORNO', 'desarrollo').lower() == 'desarrollo':
            archivo_debug = directorio_logs / "debug.log"
            handler_debug = logging.handlers.RotatingFileHandler(
                archivo_debug,
                maxBytes=20 * 1024 * 1024,  # 20 MB
                backupCount=3,
                encoding='utf-8'
            )
            handler_debug.setLevel(logging.DEBUG)
            
            formato_debug = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)s.%(funcName)s:%(lineno)d | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler_debug.setFormatter(formato_debug)
            logger_raiz.addHandler(handler_debug)
        
        # === FORMATOS ===
        # Formato detallado para archivos
        formato_archivo = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Formato simple para consola
        formato_consola = logging.Formatter(
            '%(levelname)-8s | %(name)s | %(message)s'
        )
        
        # Formato específico para errores con stack trace
        formato_errores = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s.%(funcName)s:%(lineno)d\n'
            'MENSAJE: %(message)s\n'
            '%(exc_info)s\n' + '-' * 80,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Asignar formatos a handlers
        handler_archivo.setFormatter(formato_archivo)
        handler_errores.setFormatter(formato_errores)
        handler_consola.setFormatter(formato_consola)
        
        # Agregar handlers al logger
        logger_raiz.addHandler(handler_archivo)
        logger_raiz.addHandler(handler_errores)
        logger_raiz.addHandler(handler_consola)
        
        # Configurar loggers específicos para módulos externos
        self._configurar_loggers_externos()
        
        # Log inicial de configuración
        logger_raiz.info("Sistema de logging configurado correctamente")
        logger_raiz.info(f"Nivel de logging: {nivel_logging}")
        logger_raiz.info(f"Directorio de logs: {directorio_logs.absolute()}")
    
    def _configurar_loggers_externos(self):
        """Configura el nivel de logging para librerías externas."""
        # Reducir verbosidad de SQLAlchemy
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
        
        # Reducir verbosidad de urllib3 (requests)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    
    @staticmethod
    def obtener_logger(nombre: str) -> logging.Logger:
        """
        Obtiene un logger configurado para un módulo específico.
        
        Args:
            nombre (str): Nombre del módulo/clase que solicita el logger
            
        Returns:
            logging.Logger: Logger configurado
        """
        # Asegurar que la configuración esté inicializada
        ConfiguradorLogging()
        
        # Crear logger hijo del sistema principal
        nombre_completo = f"sistema_ventas.{nombre}"
        return logging.getLogger(nombre_completo)


def registrar_operacion(operacion: str):
    """
    Decorador para registrar automáticamente operaciones en métodos.
    
    Args:
        operacion (str): Descripción de la operación a registrar
    """
    def decorador(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Obtener logger basado en la clase/módulo
            if args and hasattr(args[0], '__class__'):
                nombre_clase = args[0].__class__.__name__
            else:
                nombre_clase = func.__module__.split('.')[-1]
            
            logger = ConfiguradorLogging.obtener_logger(nombre_clase)
            
            # Registrar inicio de operación
            logger.info(f"INICIANDO: {operacion}")
            
            try:
                inicio = datetime.now()
                resultado = func(*args, **kwargs)
                duracion = (datetime.now() - inicio).total_seconds()
                
                # Registrar éxito
                logger.info(f"COMPLETADO: {operacion} (duración: {duracion:.3f}s)")
                return resultado
                
            except Exception as e:
                # Registrar error con stack trace
                logger.error(
                    f"ERROR en {operacion}: {str(e)}", 
                    exc_info=True
                )
                raise
        
        return wrapper
    return decorador


def registrar_consulta_sql(func):
    """
    Decorador específico para registrar consultas SQL.
    """
    @wraps(func)
    def wrapper(self, consulta_sql: str, *args, **kwargs):
        logger = ConfiguradorLogging.obtener_logger("ConexionBD")
        
        # Registrar consulta (truncada para legibilidad)
        consulta_truncada = consulta_sql[:200] + "..." if len(consulta_sql) > 200 else consulta_sql
        logger.debug(f"Ejecutando SQL: {consulta_truncada}")
        
        try:
            inicio = datetime.now()
            resultado = func(self, consulta_sql, *args, **kwargs)
            duracion = (datetime.now() - inicio).total_seconds()
            
            if resultado is not None:
                filas = len(resultado) if hasattr(resultado, '__len__') else 'N/A'
                logger.info(f"Consulta exitosa: {filas} filas en {duracion:.3f}s")
            else:
                logger.warning(f"Consulta retornó None en {duracion:.3f}s")
            
            return resultado
            
        except Exception as e:
            logger.error(f"Error en consulta SQL: {str(e)}", exc_info=True)
            logger.debug(f"SQL que falló: {consulta_sql}")
            raise
    
    return wrapper


class RegistradorEstadisticas:
    """
    Utilidad para registrar estadísticas y métricas del sistema.
    """
    
    def __init__(self):
        self.logger = ConfiguradorLogging.obtener_logger("Estadisticas")
    
    def registrar_metricas_base_datos(self, conexiones_activas: int, consultas_ejecutadas: int):
        """Registra métricas relacionadas con la base de datos."""
        self.logger.info(
            f"MÉTRICAS BD - Conexiones activas: {conexiones_activas}, "
            f"Consultas ejecutadas: {consultas_ejecutadas}"
        )
    
    def registrar_uso_memoria(self, uso_mb: float):
        """Registra el uso de memoria del proceso."""
        if uso_mb > 500:  # Advertencia si supera 500MB
            self.logger.warning(f"MEMORIA ALTA: {uso_mb:.1f} MB en uso")
        else:
            self.logger.debug(f"Uso de memoria: {uso_mb:.1f} MB")
    
    def registrar_rendimiento_consulta(self, tipo_consulta: str, duracion: float, filas: int):
        """Registra métricas de rendimiento de consultas."""
        if duracion > 5.0:  # Consultas lentas
            self.logger.warning(
                f"CONSULTA LENTA [{tipo_consulta}]: {duracion:.3f}s, {filas} filas"
            )
        else:
            self.logger.debug(
                f"Rendimiento [{tipo_consulta}]: {duracion:.3f}s, {filas} filas"
            )


class ManejadorErrores:
    """
    Manejador centralizado de errores con logging automático.
    """
    
    def __init__(self, contexto: str):
        self.contexto = contexto
        self.logger = ConfiguradorLogging.obtener_logger("ManejadorErrores")
    
    def manejar_error_bd(self, error: Exception, consulta: str = None):
        """Maneja errores específicos de base de datos."""
        mensaje_error = f"Error de BD en {self.contexto}: {str(error)}"
        
        self.logger.error(mensaje_error, exc_info=True)
        
        if consulta:
            self.logger.debug(f"Consulta que falló: {consulta}")      
    
    def manejar_error_validacion(self, campo: str, valor, regla: str):
        """Maneja errores de validación de datos."""
        mensaje = f"Error de validación en {self.contexto} - Campo: {campo}, Valor: {valor}, Regla: {regla}"
        self.logger.warning(mensaje)
    
    def manejar_error_critico(self, error: Exception):
        """Maneja errores críticos que pueden requerir intervención inmediata."""
        mensaje = f"ERROR CRÍTICO en {self.contexto}: {str(error)}"
        
        self.logger.critical(mensaje, exc_info=True)       

# Funciones utilitarias para logging común
def log_inicio_aplicacion():
    """Registra el inicio de la aplicación con información del entorno."""
    logger = ConfiguradorLogging.obtener_logger("Sistema")
    
    logger.info("=" * 60)
    logger.info("INICIANDO SISTEMA DE ANÁLISIS DE VENTAS")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Python: {sys.version}")
    logger.info(f"Directorio de trabajo: {os.getcwd()}")
    logger.info(f"Entorno: {os.getenv('ENTORNO', 'desarrollo')}")
    logger.info("-" * 60)


def log_fin_aplicacion():
    """Registra el fin de la aplicación."""
    logger = ConfiguradorLogging.obtener_logger("Sistema")
    
    logger.info("-" * 60)
    logger.info("FINALIZANDO SISTEMA DE ANÁLISIS DE VENTAS")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)


# Configuración automática al importar el módulo
if __name__ == "__main__":
    # Demo del sistema de logging
    log_inicio_aplicacion()
    
    # Ejemplo de uso de diferentes loggers
    logger_demo = ConfiguradorLogging.obtener_logger("Demo")
    logger_demo.debug("Mensaje de debug")
    logger_demo.info("Mensaje informativo")
    logger_demo.warning("Mensaje de advertencia")
    logger_demo.error("Mensaje de error")
    
    # Ejemplo con decorador
    @registrar_operacion("operación de prueba")
    def operacion_ejemplo():
        import time
        time.sleep(0.1)  # Simular trabajo
        return "resultado"
    
    resultado = operacion_ejemplo()
    logger_demo.info(f"Resultado: {resultado}")
    
    log_fin_aplicacion()