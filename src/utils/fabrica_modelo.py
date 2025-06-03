import sys
import os
from pathlib import Path

root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

from datetime import date, time
from decimal import Decimal
from typing import Dict, Any, Type, Union
import pandas as pd

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
    
    @classmethod
    def create_from_dict(cls, model_type: str, data: Dict[str, Any]) -> Any:
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
        if model_type not in cls._model_types:
            raise ValueError(f"Tipo de modelo '{model_type}' no soportado. "
                           f"Tipos disponibles: {list(cls._model_types.keys())}")
        
        # Obtener la clase del modelo
        model_class = cls._model_types[model_type]
        
        # Llamar al método específico de creación
        method_name = f"_create_{model_type}"
        if hasattr(cls, method_name):
            return getattr(cls, method_name)(data)
        else:
            raise NotImplementedError(f"Método {method_name} no implementado")
    
    @classmethod
    def create_from_dataframe_row(cls, model_type: str, row: pd.Series) -> Any:
        """
        Crea una instancia de modelo desde una fila de DataFrame.
        
        Args:
            model_type (str): Tipo de modelo
            row (pd.Series): Fila del DataFrame
            
        Returns:
            Instancia del modelo correspondiente
        """
        # Convertir Series a diccionario y llamar a create_from_dict
        return cls.create_from_dict(model_type, row.to_dict())
    
    @classmethod
    def create_multiple_from_dataframe(cls, model_type: str, df: pd.DataFrame) -> list:
        """
        Crea múltiples instancias de modelo desde un DataFrame.
        
        Args:
            model_type (str): Tipo de modelo
            df (pd.DataFrame): DataFrame con los datos
            
        Returns:
            Lista de instancias del modelo
        """
        return [cls.create_from_dataframe_row(model_type, row) 
                for _, row in df.iterrows()]
    
    # Métodos específicos de creación para cada modelo
    
    @staticmethod
    def _create_cliente(data: Dict[str, Any]) -> Cliente:
        """Crea una instancia de Cliente desde datos."""
        return Cliente(
            id_cliente=int(data['CustomerID']),
            primer_nombre=str(data['FirstName']),
            apellido=str(data['LastName']),
            id_ciudad=int(data['CityID']),
            inicial_segundo_nombre=data.get('MiddleInitial'),
            direccion=data.get('Address')
        )
    
    @staticmethod
    def _create_empleado(data: Dict[str, Any]) -> Empleado:
        """Crea una instancia de Empleado desde datos."""
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
        
        return Empleado(
            id_empleado=int(data['EmployeeID']),
            primer_nombre=str(data['FirstName']),
            apellido=str(data['LastName']),
            id_ciudad=int(data['CityID']),
            fecha_contratacion=fecha_contratacion,
            inicial_segundo_nombre=data.get('MiddleInitial'),
            fecha_nacimiento=fecha_nacimiento,
            genero=data.get('Gender')
        )
    
    @staticmethod
    def _create_producto(data: Dict[str, Any]) -> Producto:
        """Crea una instancia de Producto desde datos."""
        # Convertir precio a Decimal
        precio = Decimal(str(data['Price']))
        
        # Convertir tiempo si está presente
        hora_modificacion = None
        if 'ModifyDate' in data and data['ModifyDate']:
            if isinstance(data['ModifyDate'], str):
                # Asumiendo formato HH:MM:SS o similar
                try:
                    hora_modificacion = time.fromisoformat(data['ModifyDate'])
                except ValueError:
                    pass  # Si no se puede parsear, se deja None
            elif isinstance(data['ModifyDate'], time):
                hora_modificacion = data['ModifyDate']
        
        return Producto(
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
    
    @staticmethod
    def _create_venta(data: Dict[str, Any]) -> Venta:
        """Crea una instancia de Venta desde datos."""
        # Convertir valores numéricos
        precio_total = Decimal(str(data['TotalPrice']))
        descuento = Decimal(str(data['Discount'])) if data.get('Discount') else None
        
        # Convertir tiempo si está presente
        hora_venta = None
        if 'SalesDate' in data and data['SalesDate']:
            if isinstance(data['SalesDate'], str):
                try:
                    hora_venta = time.fromisoformat(data['SalesDate'])
                except ValueError:
                    pass
            elif isinstance(data['SalesDate'], time):
                hora_venta = data['SalesDate']
        
        return Venta(
            id_venta=int(data['SalesID']),
            id_producto=int(data['ProductID']),
            id_cliente=int(data['CustomerID']),
            cantidad=int(data['Quantity']),
            precio_total=precio_total,
            id_empleado_vendedor=int(data['SalesPersonID']) if data.get('SalesPersonID') else None,
            descuento=descuento,
            hora_venta=hora_venta,
            numero_transaccion=data.get('TransactionNumber')
        )
    
    @staticmethod
    def _create_categoria(data: Dict[str, Any]) -> Categoria:
        """Crea una instancia de Categoria desde datos."""
        return Categoria(
            id_categoria=int(data['CategoryID']),
            nombre_categoria=str(data['CategoryName'])
        )
    
    @staticmethod
    def _create_ciudad(data: Dict[str, Any]) -> Ciudad:
        """Crea una instancia de Ciudad desde datos."""
        return Ciudad(
            id_ciudad=int(data['CityID']),
            nombre_ciudad=str(data['CityName']),
            id_pais=int(data['CountryID']),
            codigo_postal=data.get('Zipcode')
        )
    
    @staticmethod
    def _create_pais(data: Dict[str, Any]) -> Pais:
        """Crea una instancia de Pais desde datos."""
        return Pais(
            id_pais=int(data['CountryID']),
            nombre_pais=str(data['CountryName']),
            codigo_pais=data.get('CountryCode')
        )

# Ejemplo de uso
if __name__ == "__main__":
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
    cliente = FabricaModelos.create_from_dict('cliente', datos_cliente)
    print(f"Cliente creado: {cliente}")
    print(f"Nombre completo: {cliente.nombre_completo()}")