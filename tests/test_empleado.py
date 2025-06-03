from datetime import date
import pytest 

from src.modelos.empleado import Empleado 


def test_empleado_describir_antiguedad_varios_casos():
    """
    Prueba el método describir_antiguedad de la clase Empleado para diferentes escenarios.
    """
    # Caso 1: Empleado con varios años de antigüedad
    
    fecha_hoy_simulada = date(2025, 5, 30) # Fecha ejemplo para la lógica de la prueba

    # Empleado contratado hace más de 1 año
    empleado1 = Empleado(id_empleado=1, primer_nombre="Ana", apellido="Perez", id_ciudad=1,
                         fecha_contratacion=date(2022, 3, 15)) 
    
    assert empleado1.describir_antiguedad() == "3 años de antigüedad." 
    
    # Caso 2: Empleado con 1 año de antigüedad
    empleado2 = Empleado(id_empleado=2, primer_nombre="Luis", apellido="Gomez", id_ciudad=1,
                         fecha_contratacion=date(2024, 4, 1))
    
    # Para el 30/05/2025, Luis tiene 1 año y casi 2 meses. (5,30) > (4,1) -> no se resta. Años = 1.
    assert empleado2.describir_antiguedad() == "1 año de antigüedad."

    # Caso 3: Empleado con menos de un año (contratado este año, pero ya pasó el aniversario)
    empleado3 = Empleado(id_empleado=3, primer_nombre="Sara", apellido="Vazquez", id_ciudad=1,
                         fecha_contratacion=date(2025, 1, 10))
    # Para el 30/05/2025, Sara tiene 0 años y unos meses. (5,30) > (1,10) -> no se resta. Años = 0.
    assert empleado3.describir_antiguedad() == "Menos de un año de antigüedad."

    # Caso 4: Empleado sin fecha de contratación
    empleado_sin_fecha = Empleado(id_empleado=4, primer_nombre="Juan", apellido="Nadie", id_ciudad=1)
    assert empleado_sin_fecha.describir_antiguedad() == "Fecha de contratación no especificada."