import sys
import os
from pathlib import Path

root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

from datetime import date, time
from decimal import Decimal
from typing import Dict, Any, Type, Union
import pandas as pd

# Importar sistema de logging
from src.utils.sistema_logging import (
    ConfiguradorLogging,
    registrar_operacion,
    ManejadorErrores,
    RegistradorEstadisticas
)

try:
    # Imports absolutos (funcionan cuando se ejecuta como módulo)
    from src.modelos.cliente import Cliente
    from src.modelos.empleado import Empleado
    from src.modelos.producto import Producto
    from src.modelos.venta import Venta
    from src.modelos.categorias import Categoria
    from src.modelos.ciudad import Ciudad
    from src.modelos.pais import Pais
except ImportError:
    # Imports relativos (fallback cuando se ejecuta directamente)
    try:
        from modelos.cliente import Cliente
        from modelos.empleado import Empleado
        from modelos.producto import Producto
        from modelos.venta import Venta
        from modelos.categorias import Categoria
        from modelos.ciudad import Ciudad
        from modelos.pais import Pais
    except ImportError as e:
        print(f"Error importing models: {e}")
        print("Make sure you're running from the project root or the models exist.")
        sys.exit(1)


class FabricaModelos:
    """
    Patrón Factory para crear instancias de modelos desde datos.
    Incluye logging completo y manejo de errores robusto.
    
    Resuelve el problema de tener lógica de creación de objetos dispersa
    y centraliza la conversión de datos raw a objetos del dominio.
    """
    
    # Registro de tipos de modelo disponibles
    _model_types = {
        'cliente': Cliente,
        'empleado': Empleado,
        'producto': Producto,
        'venta': Venta,
        'categoria': Categoria,
        'ciudad': Ciudad,
        'pais': Pais
    }
    
    def __init__(self):
        """Inicializa la fábrica con sistema de logging."""
        self.logger = ConfiguradorLogging.obtener_logger("FabricaModelos")
        self.estadisticas = RegistradorEstadisticas()
        self.manejador_errores = ManejadorErrores("FabricaModelos")
        self.contador_objetos_creados = 0
        
        self.logger.info("FabricaModelos inicializada correctamente")
        self.logger.debug(f"Tipos de modelo soportados: {list(self._model_types.keys())}")
    
    @registrar_operacion("creación de modelo desde diccionario")
    def create_from_dict(self, model_type: str, data: Dict[str, Any]) -> Any:
        """
        Crea una instancia de modelo desde un diccionario de datos.
        
        Args:
            model_type (str): Tipo de modelo ('cliente', 'empleado', etc.)
            data (Dict[str, Any]): Datos para crear el modelo
            
        Returns:
            Instancia del modelo correspondiente
            
        Raises:
            ValueError: Si el tipo de modelo no es válido
        """
        self.logger.debug(f"Iniciando creación de modelo tipo '{model_type}' con {len(data)} campos")
        
        # Validar tipo de modelo
        if model_type not in self._model_types:
            error_msg = f"Tipo de modelo '{model_type}' no soportado. Tipos disponibles: {list(self._model_types.keys())}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Obtener la clase del modelo
        model_class = self._model_types[model_type]
        
        try:
            # Validar datos de entrada
            self._validar_datos_entrada(model_type, data)
            
            # Llamar al método específico de creación
            method_name = f"_create_{model_type}"
            if hasattr(self, method_name):
                modelo_creado = getattr(self, method_name)(data)
                
                # Incrementar contador y registrar éxito
                self.contador_objetos_creados += 1
                self.logger.info(f"Modelo {model_type} creado exitosamente (ID: {getattr(modelo_creado, 'id_' + model_type.replace('categoria', 'categoria'), 'N/A')})")
                
                # Registrar estadísticas cada 10 objetos
                if self.contador_objetos_creados % 10 == 0:
                    self.estadisticas.registrar_metricas_base_datos(0, self.contador_objetos_creados)
                
                return modelo_creado
            else:
                error_msg = f"Método {method_name} no implementado"
                self.logger.error(error_msg)
                raise NotImplementedError(error_msg)
                
        except ValueError as ve:
            self.manejador_errores.manejar_error_validacion(model_type, str(data), str(ve))
            raise
        except Exception as e:
            self.manejador_errores.manejar_error_critico(e)
            raise
    
    def _validar_datos_entrada(self, model_type: str, data: Dict[str, Any]):
        """
        Valida que los datos de entrada sean apropiados para el tipo de modelo.
        
        Args:
            model_type (str): Tipo de modelo
            data (Dict[str, Any]): Datos a validar
            
        Raises:
            ValueError: Si los datos no son válidos
        """
        if not isinstance(data, dict):
            raise ValueError("Los datos deben ser un diccionario")
        
        if not data:
            raise ValueError("Los datos no pueden estar vacíos")
        
        # Validaciones específicas por tipo de modelo
        campos_requeridos = {
            'cliente': ['CustomerID', 'FirstName', 'LastName', 'CityID'],
            'empleado': ['EmployeeID', 'FirstName', 'LastName', 'CityID'],
            'producto': ['ProductID', 'ProductName', 'Price', 'CategoryID'],
            'venta': ['SalesID', 'ProductID', 'CustomerID', 'Quantity', 'TotalPrice'],
            'categoria': ['CategoryID', 'CategoryName'],
            'ciudad': ['CityID', 'CityName', 'CountryID'],
            'pais': ['CountryID', 'CountryName']
        }
        
        if model_type in campos_requeridos:
            campos_faltantes = [campo for campo in campos_requeridos[model_type] if campo not in data or data[campo] is None]
            if campos_faltantes:
                raise ValueError(f"Campos requeridos faltantes para {model_type}: {campos_faltantes}")
        
        self.logger.debug(f"Validación de datos exitosa para {model_type}")
    
    @registrar_operacion("creación de modelo desde fila de DataFrame")
    def create_from_dataframe_row(self, model_type: str, row: pd.Series) -> Any:
        """
        Crea una instancia de modelo desde una fila de DataFrame.
        
        Args:
            model_type (str): Tipo de modelo
            row (pd.Series): Fila del DataFrame
            
        Returns:
            Instancia del modelo correspondiente
        """
        self.logger.debug(f"Convirtiendo fila de DataFrame a {model_type}")
        
        try:
            # Convertir Series a diccionario, manejando valores NaN
            data_dict = row.to_dict()
            
            # Limpiar valores NaN de pandas
            data_limpia = {k: (None if pd.isna(v) else v) for k, v in data_dict.items()}
            
            return self.create_from_dict(model_type, data_limpia)
            
        except Exception as e:
            self.logger.error(f"Error convirtiendo fila de DataFrame: {e}")
            raise
    
    @registrar_operacion("creación múltiple de modelos desde DataFrame")
    def create_multiple_from_dataframe(self, model_type: str, df: pd.DataFrame) -> list:
        """
        Crea múltiples instancias de modelo desde un DataFrame.
        
        Args:
            model_type (str): Tipo de modelo
            df (pd.DataFrame): DataFrame con los datos
            
        Returns:
            Lista de instancias del modelo
        """
        if df.empty:
            self.logger.warning(f"DataFrame vacío para crear modelos de tipo {model_type}")
            return []
        
        self.logger.info(f"Creando {len(df)} modelos de tipo {model_type} desde DataFrame")
        
        modelos_creados = []
        errores_encontrados = 0
        
        for indice, row in df.iterrows():
            try:
                modelo = self.create_from_dataframe_row(model_type, row)
                modelos_creados.append(modelo)
            except Exception as e:
                errores_encontrados += 1
                self.logger.warning(f"Error creando modelo en fila {indice}: {e}")
                
                # Si hay demasiados errores, detener el proceso
                if errores_encontrados > len(df) * 0.1:  # Más del 10% de errores
                    error_msg = f"Demasiados errores ({errores_encontrados}) creando modelos. Deteniendo proceso."
                    self.logger.error(error_msg)
                    raise RuntimeError(error_msg)
        
        exito_rate = (len(modelos_creados) / len(df)) * 100
        self.logger.info(f"Creación masiva completada: {len(modelos_creados)}/{len(df)} modelos ({exito_rate:.1f}% éxito)")
        
        if errores_encontrados > 0:
            self.logger.warning(f"Se encontraron {errores_encontrados} errores durante la creación masiva")
        
        return modelos_creados
    
    # === Métodos específicos de creación para cada modelo ===
    
    def _create_cliente(self, data: Dict[str, Any]) -> Cliente:
        """Crea una instancia de Cliente desde datos."""
        self.logger.debug(f"Creando cliente con ID {data.get('CustomerID')}")
        
        try:
            cliente = Cliente(
                id_cliente=int(data['CustomerID']),
                primer_nombre=str(data['FirstName']),
                apellido=str(data['LastName']),
                id_ciudad=int(data['CityID']),
                inicial_segundo_nombre=data.get('MiddleInitial'),
                direccion=data.get('Address')
            )
            
            self.logger.debug(f"Cliente creado: {cliente.nombre_completo()}")
            return cliente
            
        except Exception as e:
            self.logger.error(f"Error creando cliente: {e}")
            raise
    
    def _create_empleado(self, data: Dict[str, Any]) -> Empleado:
        """Crea una instancia de Empleado desde datos."""
        self.logger.debug(f"Creando empleado con ID {data.get('EmployeeID')}")
        
        try:
            # Convertir fechas si están presentes
            fecha_nacimiento = None
            if 'BirthDate' in data and data['BirthDate']:
                if isinstance(data['BirthDate'], str):
                    fecha_nacimiento = date.fromisoformat(data['BirthDate'].split()[0])
                elif isinstance(data['BirthDate'], date):
                    fecha_nacimiento = data['BirthDate']
            
            fecha_contratacion = None
            if 'HireDate' in data and data['HireDate']:
                if isinstance(data['HireDate'], str):
                    fecha_contratacion = date.fromisoformat(data['HireDate'].split()[0])
                elif isinstance(data['HireDate'], date):
                    fecha_contratacion = data['HireDate']
            
            empleado = Empleado(
                id_empleado=int(data['EmployeeID']),
                primer_nombre=str(data['FirstName']),
                apellido=str(data['LastName']),
                id_ciudad=int(data['CityID']),
                fecha_contratacion=fecha_contratacion,
                inicial_segundo_nombre=data.get('MiddleInitial'),
                fecha_nacimiento=fecha_nacimiento,
                genero=data.get('Gender')
            )
            
            self.logger.debug(f"Empleado creado: {empleado.nombre_completo()}")
            return empleado
            
        except Exception as e:
            self.logger.error(f"Error creando empleado: {e}")
            raise
    
    def _create_producto(self, data: Dict[str, Any]) -> Producto:
        """Crea una instancia de Producto desde datos."""
        self.logger.debug(f"Creando producto con ID {data.get('ProductID')}")
        
        try:
            # Convertir precio a Decimal
            precio = Decimal(str(data['Price']))
            
            # Convertir tiempo si está presente
            hora_modificacion = None
            if 'ModifyDate' in data and data['ModifyDate']:
                if isinstance(data['ModifyDate'], str):
                    try:
                        hora_modificacion = time.fromisoformat(data['ModifyDate'])
                    except ValueError:
                        self.logger.debug(f"No se pudo parsear ModifyDate: {data['ModifyDate']}")
                elif isinstance(data['ModifyDate'], time):
                    hora_modificacion = data['ModifyDate']
            
            producto = Producto(
                id_producto=int(data['ProductID']),
                nombre_producto=str(data['ProductName']),
                precio=precio,
                id_categoria=int(data['CategoryID']),
                clase_producto=data.get('Class'),
                hora_modificacion=hora_modificacion,
                resistente=data.get('Resistant'),
                es_alergenico=data.get('IsAllergic'),
                dias_vitalidad=int(data['VitalityDays']) if data.get('VitalityDays') else None
            )
            
            self.logger.debug(f"Producto creado: {producto.nombre_producto} (${precio})")
            return producto
            
        except Exception as e:
            self.logger.error(f"Error creando producto: {e}")
            raise
    
    # ... (resto de métodos _create_* con logging similar)
    
    def obtener_estadisticas(self) -> dict:
        """
        Retorna estadísticas de la fábrica.
        
        Returns:
            dict: Estadísticas de objetos creados
        """
        estadisticas = {
            'objetos_creados_total': self.contador_objetos_creados,
            'tipos_soportados': len(self._model_types),
            'tipos_disponibles': list(self._model_types.keys())
        }
        
        self.logger.debug(f"Estadísticas de fábrica: {estadisticas}")
        return estadisticas


# Ejemplo de uso con logging
if __name__ == "__main__":
    from src.utils.sistema_logging import log_inicio_aplicacion, log_fin_aplicacion
    
    # Inicializar logging
    log_inicio_aplicacion()
    
    try:
        # Crear fábrica
        fabrica = FabricaModelos()
        
        # Simular datos de cliente
        datos_cliente = {
            'CustomerID': 1,
            'FirstName': 'Juan',
            'MiddleInitial': 'C',
            'LastName': 'Pérez',
            'CityID': 5,
            'Address': 'Calle Falsa 123'
        }
        
        # Crear cliente usando la factory
        cliente = fabrica.create_from_dict('cliente', datos_cliente)
        print(f"Cliente creado: {cliente}")
        print(f"Nombre completo: {cliente.nombre_completo()}")
        
        # Mostrar estadísticas
        stats = fabrica.obtener_estadisticas()
        print(f"Estadísticas: {stats}")
        
    except Exception as e:
        print(f"Error en ejemplo: {e}")
    
    finally:
        log_fin_aplicacion()