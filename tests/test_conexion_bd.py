import pytest
from src.conexion_bd import ConexionBD 
import pandas as pd 

def test_singleton_conexion_bd():
    """
    Verifica que la clase ConexionBD implemente correctamente el patrón Singleton.
    """
    # Crear dos instancias (deberían ser la misma)
    conector1 = ConexionBD()
    conector2 = ConexionBD()

    assert conector1 is conector2, "ConexionBD no está implementando el patrón Singleton correctamente."

    motor1 = conector1.obtener_motor()
    motor2 = conector2.obtener_motor()

    assert motor1 is motor2, "Los motores de las instancias Singleton no son los mismos."
    
    # Verificar que la conexión se puede establecer si las variables de entorno están configuradas     
    if motor1:
        try:
            with motor1.connect() as conn:
                assert not conn.closed
            print("Prueba de conexión real exitosa (requiere DB configurada y accesible).")
        except Exception as e:         
            pytest.skip(f"No se pudo conectar a la BD para la prueba completa del Singleton: {e}")
    else:
        print("Motor de BD no inicializado en test_singleton_conexion_bd, prueba de conexión real omitida.")
        pass

def test_ejecutar_consulta_sin_conexion(mocker):
    """
    Prueba que ejecutar_consulta maneja el caso donde no hay conexión (self.__motor es None).
    """
    # Obtenemos la instancia Singleton. Podría ser una ya existente.
    conector_para_prueba = ConexionBD()

    # Usar mocker para parchear el atributo '__motor' (nombre mangled: _ConexionBD__motor)
    # de la instancia específica 'conector_para_prueba' para que sea None. Esto simula que la conexión no se estableció o se perdió.
    mocker.patch.object(conector_para_prueba, '_ConexionBD__motor', None)
    
    if hasattr(conector_para_prueba, '_ConexionBD__sesion'):
         mocker.patch.object(conector_para_prueba, '_ConexionBD__sesion', None)

    df_resultado = conector_para_prueba.ejecutar_consulta("SELECT 1")
    
    assert df_resultado is None, "Debería retornar None si no hay motor de conexión."