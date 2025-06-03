import pytest
import pandas as pd
from decimal import Decimal
from datetime import date, time
from unittest.mock import Mock, patch, MagicMock

# Imports del proyecto
from src.conexion_bd import ConexionBD
from src.utils.fabrica_modelo import FabricaModelos
from src.utils.constructor_consulta import ConstructorConsultaSQL
from src.modelos.cliente import Cliente
from src.modelos.empleado import Empleado
from src.modelos.producto import Producto


class TestPatronSingleton:
    """
    Conjunto de pruebas para validar la correcta implementación 
    del patrón Singleton en la clase ConexionBD.
    """
    
    def test_singleton_misma_instancia(self):
        """Verifica que múltiples instanciaciones retornen el mismo objeto."""
        conexion_1 = ConexionBD()
        conexion_2 = ConexionBD()
        conexion_3 = ConexionBD()
        
        # Todas las instancias deben ser exactamente el mismo objeto
        assert conexion_1 is conexion_2
        assert conexion_2 is conexion_3
        assert id(conexion_1) == id(conexion_2) == id(conexion_3)
    
    def test_singleton_motor_compartido(self):
        """Verifica que el motor de BD sea compartido entre instancias."""
        conexion_1 = ConexionBD()
        conexion_2 = ConexionBD()
        
        motor_1 = conexion_1.obtener_motor()
        motor_2 = conexion_2.obtener_motor()
        
        # Los motores deben ser el mismo objeto
        if motor_1 and motor_2:  # Solo si la conexión fue exitosa
            assert motor_1 is motor_2
    
    def test_singleton_sesion_independiente_pero_singleton_preservado(self):
        """Verifica que aunque se cierre la sesión, el singleton se preserve."""
        conexion_1 = ConexionBD()
        conexion_1.cerrar_sesion()
        
        conexion_2 = ConexionBD()
        
        # Aún debe ser la misma instancia
        assert conexion_1 is conexion_2
    
    @patch.dict('os.environ', {
        'DB_HOST': 'localhost_test',
        'DB_USER': 'test_user', 
        'DB_PASSWORD': 'test_pass',
        'DB_NAME': 'test_db'
    })
    def test_singleton_inicializacion_con_variables_entorno(self):
        """Verifica que el singleton use correctamente las variables de entorno."""
        # Resetear la instancia singleton para esta prueba
        ConexionBD._ConexionBD__instancia = None
        
        with patch('src.conexion_bd.create_engine') as mock_create_engine:
            mock_create_engine.return_value = Mock()
            
            conexion = ConexionBD()
            
            # Verificar que create_engine fue llamado con los parámetros correctos
            mock_create_engine.assert_called_once()
            args, kwargs = mock_create_engine.call_args
            cadena_conexion = args[0]
            
            assert 'test_user' in cadena_conexion
            assert 'localhost_test' in cadena_conexion
            assert 'test_db' in cadena_conexion


class TestPatronFactory:
    """
    Conjunto de pruebas para validar la correcta implementación 
    del patrón Factory en la clase FabricaModelos.
    """
    
    def test_fabrica_crear_cliente_desde_diccionario(self):
        """Verifica la creación de Cliente desde diccionario."""
        datos_cliente = {
            'CustomerID': 123,
            'FirstName': 'María',
            'MiddleInitial': 'E',
            'LastName': 'González',
            'CityID': 45,
            'Address': 'Avenida Libertador 1234'
        }
        
        cliente = FabricaModelos.create_from_dict('cliente', datos_cliente)
        
        assert isinstance(cliente, Cliente)
        assert cliente.id_cliente == 123
        assert cliente.primer_nombre == 'María'
        assert cliente.inicial_segundo_nombre == 'E'
        assert cliente.apellido == 'González'
        assert cliente.id_ciudad == 45
        assert cliente.direccion == 'Avenida Libertador 1234'
        assert cliente.nombre_completo() == 'María E. González'
    
    def test_fabrica_crear_empleado_con_fechas(self):
        """Verifica la creación de Empleado con conversión de fechas."""
        datos_empleado = {
            'EmployeeID': 456,
            'FirstName': 'Carlos',
            'MiddleInitial': 'R',
            'LastName': 'Martínez',
            'BirthDate': '1985-03-15',
            'Gender': 'M',
            'CityID': 67,
            'HireDate': '2020-06-01'
        }
        
        empleado = FabricaModelos.create_from_dict('empleado', datos_empleado)
        
        assert isinstance(empleado, Empleado)
        assert empleado.id_empleado == 456
        assert empleado.primer_nombre == 'Carlos'
        assert empleado.fecha_nacimiento == date(1985, 3, 15)
        assert empleado.fecha_contratacion == date(2020, 6, 1)
        assert empleado.genero == 'M'
        
        # Verificar método de negocio
        antiguedad = empleado.calcular_antiguedad_anos()
        assert antiguedad is not None and antiguedad >= 4
    
    def test_fabrica_crear_producto_con_validaciones(self):
        """Verifica la creación de Producto con tipos correctos."""
        datos_producto = {
            'ProductID': 789,
            'ProductName': 'Manzanas Rojas Premium',
            'Price': '25.50',
            'CategoryID': 11,  # Produce
            'Class': 'High',
            'ModifyDate': '14:30:00',
            'Resistant': 'Durable',
            'IsAllergic': 'FALSE',
            'VitalityDays': 7
        }
        
        producto = FabricaModelos.create_from_dict('producto', datos_producto)
        
        assert isinstance(producto, Producto)
        assert producto.id_producto == 789
        assert producto.nombre_producto == 'Manzanas Rojas Premium'
        assert producto.precio == Decimal('25.50')
        assert producto.id_categoria == 11
        assert producto.dias_vitalidad == 7
    
    def test_fabrica_crear_desde_dataframe(self):
        """Verifica la creación desde DataFrame de pandas."""
        datos_df = pd.DataFrame([
            {
                'CustomerID': 100,
                'FirstName': 'Ana',
                'LastName': 'López',
                'CityID': 1,
                'MiddleInitial': 'B',
                'Address': 'Calle Mayor 123'
            },
            {
                'CustomerID': 101,
                'FirstName': 'Luis',
                'LastName': 'Pérez',
                'CityID': 2,
                'MiddleInitial': None,
                'Address': 'Plaza Central 456'
            }
        ])
        
        clientes = FabricaModelos.create_multiple_from_dataframe('cliente', datos_df)
        
        assert len(clientes) == 2
        assert all(isinstance(cliente, Cliente) for cliente in clientes)
        assert clientes[0].primer_nombre == 'Ana'
        assert clientes[1].primer_nombre == 'Luis'
        assert clientes[0].inicial_segundo_nombre == 'B'
        assert clientes[1].inicial_segundo_nombre is None
    
    def test_fabrica_tipo_modelo_invalido(self):
        """Verifica manejo de errores para tipos de modelo no soportados."""
        datos = {'campo': 'valor'}
        
        with pytest.raises(ValueError) as excinfo:
            FabricaModelos.create_from_dict('modelo_inexistente', datos)
        
        assert 'modelo_inexistente' in str(excinfo.value)
        assert 'no soportado' in str(excinfo.value)


class TestPatronBuilder:
    """
    Conjunto de pruebas para validar la correcta implementación 
    del patrón Builder en la clase ConstructorConsultaSQL.
    """
    
    def test_constructor_consulta_basica(self):
        """Verifica la construcción de una consulta SELECT básica."""
        constructor = ConstructorConsultaSQL()
        
        consulta_sql = (constructor
                       .seleccionar("nombre", "edad")
                       .desde_tabla("usuarios")
                       .construir())
        
        assert "SELECT nombre, edad" in consulta_sql
        assert "FROM usuarios" in consulta_sql
        assert consulta_sql.endswith(";")
    
    def test_constructor_consulta_con_joins(self):
        """Verifica la construcción de consultas con JOINs."""
        constructor = ConstructorConsultaSQL()
        
        consulta_sql = (constructor
                       .seleccionar("c.nombre", "v.total")
                       .desde_tabla("clientes c")
                       .unir("ventas v", "c.id = v.cliente_id")
                       .unir_izquierda("ciudades ci", "c.ciudad_id = ci.id")
                       .construir())
        
        assert "SELECT c.nombre, v.total" in consulta_sql
        assert "FROM clientes c" in consulta_sql
        assert "INNER JOIN ventas v ON c.id = v.cliente_id" in consulta_sql
        assert "LEFT JOIN ciudades ci ON c.ciudad_id = ci.id" in consulta_sql
    
    def test_constructor_consulta_compleja(self):
        """Verifica la construcción de una consulta compleja con todas las cláusulas."""
        constructor = ConstructorConsultaSQL()
        
        consulta_sql = (constructor
                       .seleccionar("categoria", "COUNT(*) as total", "AVG(precio) as precio_promedio")
                       .desde_tabla("productos p")
                       .unir("categorias c", "p.categoria_id = c.id")
                       .donde("p.precio > 50")
                       .donde("p.activo = 1")
                       .agrupar_por("categoria")
                       .habiendo("COUNT(*) > 5")
                       .ordenar_por("precio_promedio", "DESC")
                       .limite(10)
                       .construir())
        
        # Verificar todas las partes de la consulta
        assert "SELECT categoria, COUNT(*) as total, AVG(precio) as precio_promedio" in consulta_sql
        assert "FROM productos p" in consulta_sql
        assert "INNER JOIN categorias c ON p.categoria_id = c.id" in consulta_sql
        assert "WHERE p.precio > 50 AND p.activo = 1" in consulta_sql
        assert "GROUP BY categoria" in consulta_sql
        assert "HAVING COUNT(*) > 5" in consulta_sql
        assert "ORDER BY precio_promedio DESC" in consulta_sql
        assert "LIMIT 10" in consulta_sql
    
    def test_constructor_reutilizable(self):
        """Verifica que el constructor se pueda reutilizar con reiniciar()."""
        constructor = ConstructorConsultaSQL()
        
        # Primera consulta
        consulta1 = (constructor
                    .seleccionar("nombre")
                    .desde_tabla("clientes")
                    .construir())
        
        assert "SELECT nombre" in consulta1
        assert "FROM clientes" in consulta1
        
        # Reiniciar y crear segunda consulta
        consulta2 = (constructor
                    .reiniciar()
                    .seleccionar("producto")
                    .desde_tabla("inventario")
                    .limite(5)
                    .construir())
        
        assert "SELECT producto" in consulta2
        assert "FROM inventario" in consulta2
        assert "LIMIT 5" in consulta2
        # No debe contener elementos de la consulta anterior
        assert "clientes" not in consulta2
    
    def test_constructor_validacion_campos_obligatorios(self):
        """Verifica validación de campos obligatorios."""
        constructor = ConstructorConsultaSQL()
        
        # Sin SELECT ni FROM
        with pytest.raises(ValueError) as excinfo:
            constructor.construir()
        assert "SELECT y FROM son obligatorios" in str(excinfo.value)
        
        # Solo con SELECT
        constructor.seleccionar("campo")
        with pytest.raises(ValueError) as excinfo:
            constructor.construir()
        assert "SELECT y FROM son obligatorios" in str(excinfo.value)
    
    @patch('src.utils.constructor_consulta.ConexionBD')
    def test_constructor_ejecucion_consulta(self, mock_conexion_bd):
        """Verifica la ejecución de consultas a través del constructor."""
        # Configurar mocks
        mock_instancia = Mock()
        mock_conexion_bd.return_value = mock_instancia
        
        datos_esperados = pd.DataFrame({
            'nombre': ['Juan', 'María'],
            'edad': [25, 30]
        })
        mock_instancia.ejecutar_consulta.return_value = datos_esperados
        
        constructor = ConstructorConsultaSQL()
        
        # Ejecutar consulta
        resultado = (constructor
                    .seleccionar("nombre", "edad")
                    .desde_tabla("usuarios")
                    .ejecutar())
        
        # Verificar que se llamó a la conexión y se ejecutó la consulta
        mock_conexion_bd.assert_called_once()
        mock_instancia.ejecutar_consulta.assert_called_once()
        
        # Verificar el resultado
        assert isinstance(resultado, pd.DataFrame)
        assert len(resultado) == 2
        assert 'nombre' in resultado.columns
        assert 'edad' in resultado.columns


class TestIntegracionPatrones:
    """
    Pruebas de integración que verifican que los patrones trabajen correctamente juntos.
    """
    
    @patch('src.conexion_bd.create_engine')
    def test_patron_singleton_con_factory(self, mock_create_engine):
        """Verifica integración entre Singleton (ConexionBD) y Factory."""
        mock_create_engine.return_value = Mock()
        
        # Resetear singleton para la prueba
        ConexionBD._ConexionBD__instancia = None
        
        # Simular datos que vendrían de la BD via Singleton
        datos_cliente = {
            'CustomerID': 1,
            'FirstName': 'Pedro',
            'LastName': 'Ramírez',
            'CityID': 1
        }
        
        # Crear cliente via Factory
        cliente = FabricaModelos.create_from_dict('cliente', datos_cliente)
        
        # Obtener conexión via Singleton
        conexion = ConexionBD()
        
        assert isinstance(cliente, Cliente)
        assert conexion is not None
        # Verificar que solo se creó una instancia de conexión
        conexion2 = ConexionBD()
        assert conexion is conexion2
    
    @patch('src.utils.constructor_consulta.ConexionBD')
    def test_patron_builder_con_singleton(self, mock_conexion_bd):
        """Verifica integración entre Builder y Singleton."""
        # Configurar mock
        mock_instancia = Mock()
        mock_conexion_bd.return_value = mock_instancia
        mock_instancia.ejecutar_consulta.return_value = pd.DataFrame({'resultado': [1, 2, 3]})
        
        # Usar Builder que internamente usa Singleton
        constructor = ConstructorConsultaSQL()
        resultado = (constructor
                    .seleccionar("*")
                    .desde_tabla("test_tabla")
                    .ejecutar())
        
        # Verificar que el Builder usó el Singleton correctamente
        mock_conexion_bd.assert_called_once()
        assert isinstance(resultado, pd.DataFrame)


# Configuración adicional para las pruebas
@pytest.fixture(autouse=True)
def limpiar_singleton():
    """Fixture que limpia el singleton antes de cada prueba para evitar interferencias."""
    # Esto se ejecuta antes de cada prueba
    yield
    # Esto se ejecuta después de cada prueba
    # Resetear el singleton si existe
    if hasattr(ConexionBD, '_ConexionBD__instancia'):
        ConexionBD._ConexionBD__instancia = None


if __name__ == "__main__":
    # Ejecutar las pruebas desde línea de comandos
    pytest.main([__file__, "-v", "--tb=short"])