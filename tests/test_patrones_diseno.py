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
        # Limpiar la instancia Singleton antes de esta prueba específica si es necesario
        # para asegurar un estado limpio si otras pruebas la modificaron.
        # Sin embargo, el fixture autouse=True debería manejar esto globalmente.
        ConexionBD._ConexionBD__instancia = None # Asegurar que se cree una nueva
        conexion_1 = ConexionBD()
        conexion_2 = ConexionBD()
        conexion_3 = ConexionBD()

        assert conexion_1 is conexion_2
        assert conexion_2 is conexion_3
        assert id(conexion_1) == id(conexion_2) == id(conexion_3)

    def test_singleton_motor_compartido(self):
        """Verifica que el motor de BD sea compartido entre instancias."""
        ConexionBD._ConexionBD__instancia = None # Asegurar que se cree una nueva
        conexion_1 = ConexionBD()
        conexion_2 = ConexionBD()

        motor_1 = conexion_1.obtener_motor()
        motor_2 = conexion_2.obtener_motor()

        if motor_1 and motor_2:
            assert motor_1 is motor_2
        elif motor_1 is None and motor_2 is None:
            # Si ambos son None (por ejemplo, si la conexión falla controladamente
            # y el mock de create_engine no se usa aquí), también es consistente.
            pass
        else:
            # Esto sería un fallo si uno tiene motor y el otro no.
            assert motor_1 is motor_2, "Los motores deberían ser el mismo objeto o ambos None"


    def test_singleton_sesion_independiente_pero_singleton_preservado(self):
        """Verifica que aunque se cierre la sesión, el singleton se preserve."""
        ConexionBD._ConexionBD__instancia = None # Asegurar que se cree una nueva
        conexion_1 = ConexionBD()

        try:
            conexion_1.cerrar_sesion()
        except Exception:
            # Si cerrar_sesion falla porque no hay motor/sesión (ej. en un entorno de test sin BD real
            # y create_engine no fue mockeado para esta inicialización específica),
            # se puede ignorar este error para el propósito de esta prueba de identidad de Singleton.
            pass


        conexion_2 = ConexionBD()
        assert conexion_1 is conexion_2

    @patch.dict('os.environ', {
        'DB_HOST': 'localhost_test',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_pass',
        'DB_NAME': 'test_db'
    })
    def test_singleton_inicializacion_con_variables_entorno(self):
        """Verifica que el singleton use correctamente las variables de entorno."""
        ConexionBD._ConexionBD__instancia = None

        mock_engine_instance = MagicMock()
        mock_connection_context_manager = MagicMock()
        mock_db_connection = MagicMock() # El objeto que representa la conexión real

        # Configurar mock_engine_instance.connect() para devolver el context manager
        mock_engine_instance.connect.return_value = mock_connection_context_manager
        # Configurar el context manager para que su __enter__ devuelva la conexión mockeada
        mock_connection_context_manager.__enter__.return_value = mock_db_connection
        # Configurar el método execute de la conexión mockeada para que no falle
        mock_db_connection.execute.return_value = Mock() # O un resultado mockeado específico si es necesario

        with patch('src.conexion_bd.create_engine', return_value=mock_engine_instance) as mock_create_engine_call:
            conexion = ConexionBD() # Esto llamará a __inicializar_conexion

            mock_create_engine_call.assert_called_once()
            args, kwargs_call = mock_create_engine_call.call_args # Renombrado para evitar conflicto con variable kwargs
            cadena_conexion = args[0]

            assert 'test_user' in cadena_conexion
            assert 'localhost_test' in cadena_conexion
            assert 'test_db' in cadena_conexion


class TestPatronFactory:
    """
    Conjunto de pruebas para validar la correcta implementación
    del patrón Factory en la clase FabricaModelos.
    """
    # Crear una instancia de la fábrica para usar en los tests de esta clase
    def setup_method(self):
        """Este método se ejecuta antes de cada test en esta clase."""
        self.fabrica = FabricaModelos()

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
        cliente = self.fabrica.create_from_dict('cliente', datos_cliente)

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
        empleado = self.fabrica.create_from_dict('empleado', datos_empleado)

        assert isinstance(empleado, Empleado)
        assert empleado.id_empleado == 456
        assert empleado.primer_nombre == 'Carlos'
        assert empleado.fecha_nacimiento == date(1985, 3, 15)
        assert empleado.fecha_contratacion == date(2020, 6, 1)
        assert empleado.genero == 'M'
        antiguedad = empleado.calcular_antiguedad_anos()
        assert antiguedad is not None and antiguedad >= 4 # Ajusta según la fecha actual de ejecución


    def test_fabrica_crear_producto_con_validaciones(self):
        """Verifica la creación de Producto con tipos correctos."""
        datos_producto = {
            'ProductID': 789,
            'ProductName': 'Manzanas Rojas Premium',
            'Price': '25.50',
            'CategoryID': 11,
            'Class': 'High',
            'ModifyDate': '14:30:00', # Asumiendo que la fábrica o el modelo manejan HH:MM:SS
            'Resistant': 'Durable',
            'IsAllergic': 'FALSE', # La fábrica debe convertir esto a booleano o el modelo manejarlo
            'VitalityDays': 7
        }
        # --- Usar la instancia de la fábrica ---
        producto = self.fabrica.create_from_dict('producto', datos_producto)

        assert isinstance(producto, Producto)
        assert producto.id_producto == 789
        assert producto.nombre_producto == 'Manzanas Rojas Premium'
        assert producto.precio == Decimal('25.50')
        assert producto.id_categoria == 11
        assert producto.dias_vitalidad == 7

    def test_fabrica_crear_desde_dataframe(self):
        """Verifica la creación desde DataFrame de pandas."""
        datos_df = pd.DataFrame([
            # ... (tus datos_df) ...
            {
                'CustomerID': 100, 'FirstName': 'Ana', 'LastName': 'López', 'CityID': 1,
                'MiddleInitial': 'B', 'Address': 'Calle Mayor 123'
            },
            {
                'CustomerID': 101, 'FirstName': 'Luis', 'LastName': 'Pérez', 'CityID': 2,
                'MiddleInitial': None, 'Address': 'Plaza Central 456'
            }
        ])
        # --- Usar la instancia de la fábrica ---
        clientes = self.fabrica.create_multiple_from_dataframe('cliente', datos_df)

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
            # --- Usar la instancia de la fábrica ---
            self.fabrica.create_from_dict('modelo_inexistente', datos)

        assert 'modelo_inexistente' in str(excinfo.value)
        assert 'no soportado' in str(excinfo.value).lower() or 'no implementado' in str(excinfo.value).lower()


class TestPatronBuilder:
    """
    Conjunto de pruebas para validar la correcta implementación
    del patrón Builder en la clase ConstructorConsultaSQL.
    """
    def test_constructor_consulta_basica(self):
        constructor = ConstructorConsultaSQL()
        consulta_sql = (constructor
                       .seleccionar("nombre", "edad")
                       .desde_tabla("usuarios")
                       .construir())
        assert "SELECT nombre, edad" in consulta_sql
        assert "FROM usuarios" in consulta_sql
        assert consulta_sql.endswith(";")

    def test_constructor_consulta_con_joins(self):
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
        assert "SELECT categoria, COUNT(*) as total, AVG(precio) as precio_promedio" in consulta_sql
        assert "FROM productos p" in consulta_sql
        assert "INNER JOIN categorias c ON p.categoria_id = c.id" in consulta_sql
        assert "WHERE p.precio > 50 AND p.activo = 1" in consulta_sql
        assert "GROUP BY categoria" in consulta_sql
        assert "HAVING COUNT(*) > 5" in consulta_sql
        assert "ORDER BY precio_promedio DESC" in consulta_sql
        assert "LIMIT 10" in consulta_sql

    def test_constructor_reutilizable(self):
        constructor = ConstructorConsultaSQL()
        consulta1 = (constructor
                    .seleccionar("nombre")
                    .desde_tabla("clientes")
                    .construir())
        assert "SELECT nombre" in consulta1
        assert "FROM clientes" in consulta1

        consulta2 = (constructor
                    .reiniciar()
                    .seleccionar("producto")
                    .desde_tabla("inventario")
                    .limite(5)
                    .construir())
        assert "SELECT producto" in consulta2
        assert "FROM inventario" in consulta2
        assert "LIMIT 5" in consulta2
        assert "clientes" not in consulta2

    def test_constructor_validacion_campos_obligatorios(self):
        constructor = ConstructorConsultaSQL()
        with pytest.raises(ValueError) as excinfo:
            constructor.construir()
        assert "SELECT y FROM son obligatorios" in str(excinfo.value)

        constructor.seleccionar("campo")
        with pytest.raises(ValueError) as excinfo:
            constructor.construir()
        assert "SELECT y FROM son obligatorios" in str(excinfo.value)

    @patch('src.conexion_bd.ConexionBD') 
    def test_constructor_ejecucion_consulta(self, mock_conexion_bd_clase): 
        """Verifica la ejecución de consultas a través del constructor."""
        mock_instancia_conexion = Mock() # Este es el mock de la *instancia* de ConexionBD
        mock_conexion_bd_clase.return_value = mock_instancia_conexion # Cuando se llame a ConexionBD(), devolverá nuestro mock

        datos_esperados = pd.DataFrame({'nombre': ['Juan', 'María'], 'edad': [25, 30]})
        mock_instancia_conexion.ejecutar_consulta.return_value = datos_esperados

        constructor = ConstructorConsultaSQL()
        resultado = (constructor
                    .seleccionar("nombre", "edad")
                    .desde_tabla("usuarios")
                    .ejecutar()) # ejecutar() internamente llamará a ConexionBD()

        mock_conexion_bd_clase.assert_called_once() # Verificar que ConexionBD() fue llamado
        mock_instancia_conexion.ejecutar_consulta.assert_called_once()
        assert isinstance(resultado, pd.DataFrame)
        assert len(resultado) == 2
        assert 'nombre' in resultado.columns
        assert 'edad' in resultado.columns


class TestIntegracionPatrones:
    """
    Pruebas de integración que verifican que los patrones trabajen correctamente juntos.
    """
    def setup_method(self):
        self.fabrica = FabricaModelos()

    @patch('src.conexion_bd.create_engine') # create_engine sigue en conexion_bd
    def test_patron_singleton_con_factory(self, mock_create_engine):
        """Verifica integración entre Singleton (ConexionBD) y Factory."""
        # --- Reutilizar la lógica de mockeo de engine como en TestPatronSingleton ---
        mock_engine_instance = MagicMock()
        mock_connection_context_manager = MagicMock()
        mock_db_connection = MagicMock()
        mock_engine_instance.connect.return_value = mock_connection_context_manager
        mock_connection_context_manager.__enter__.return_value = mock_db_connection
        mock_db_connection.execute.return_value = Mock()
        mock_create_engine.return_value = mock_engine_instance
        # --- Fin de la reutilización ---

        ConexionBD._ConexionBD__instancia = None

        datos_cliente = {
            'CustomerID': 1, 'FirstName': 'Pedro', 'LastName': 'Ramírez', 'CityID': 1
        }
        # --- Usar la instancia de la fábrica ---
        cliente = self.fabrica.create_from_dict('cliente', datos_cliente)
        conexion = ConexionBD() # Esto usa el create_engine mockeado

        assert isinstance(cliente, Cliente)
        assert conexion is not None
        conexion2 = ConexionBD()
        assert conexion is conexion2

    # --- Inicio del Cambio para AttributeError en patch ---
    @patch('src.conexion_bd.ConexionBD') 
    # --- Fin del Cambio para AttributeError en patch ---
    def test_patron_builder_con_singleton(self, mock_conexion_bd_clase): # Renombrar el argumento
        """Verifica integración entre Builder y Singleton."""
        mock_instancia_conexion = Mock()
        mock_conexion_bd_clase.return_value = mock_instancia_conexion
        mock_instancia_conexion.ejecutar_consulta.return_value = pd.DataFrame({'resultado': [1, 2, 3]})

        constructor = ConstructorConsultaSQL()
        resultado = (constructor
                    .seleccionar("*")
                    .desde_tabla("test_tabla")
                    .ejecutar())

        mock_conexion_bd_clase.assert_called_once()
        assert isinstance(resultado, pd.DataFrame)


@pytest.fixture(autouse=True)
def limpiar_singleton():
    """Fixture que limpia el singleton ANTES de cada prueba para evitar interferencias."""
    # Esto se ejecuta ANTES de cada prueba
    ConexionBD._ConexionBD__instancia = None

    yield # Aquí se ejecuta la prueba

    ConexionBD._ConexionBD__instancia = None

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])