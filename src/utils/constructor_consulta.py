import sys
import os
from pathlib import Path
import pandas as pd

from src.utils.sistema_logging import ConfiguradorLogging

class ConstructorConsultaSQL:
    """
    Patrón Constructor para crear consultas SQL complejas de manera fluida.
    Resuelve el problema de crear consultas SQL complejas de manera legible y mantenible.
    También permite ejecutarlas directamente.
    """

    def __init__(self):
        self.reiniciar()
        #Inicializar un logger para esta clase si planeas añadir logs aquí
        self.logger = ConfiguradorLogging.obtener_logger("ConstructorConsultaSQL")

    def reiniciar(self):
        """Reinicia el constructor para construir una nueva consulta."""
        self._campos_seleccion = []
        self._tabla_desde = ""
        self._uniones = []
        self._condiciones_donde = []
        self._campos_agrupar_por = []
        self._condiciones_habiendo = []
        self._campos_ordenar_por = []
        self._valor_limite = None
        return self

    def seleccionar(self, *campos):
        self._campos_seleccion.extend(campos)
        return self

    def desde_tabla(self, tabla):
        self._tabla_desde = tabla
        return self

    def unir(self, tabla, condicion_union, tipo_union="INNER"):
        self._uniones.append(f"{tipo_union} JOIN {tabla} ON {condicion_union}")
        return self

    def unir_izquierda(self, tabla, condicion_union):
        return self.unir(tabla, condicion_union, "LEFT")

    def donde(self, condicion):
        self._condiciones_donde.append(condicion)
        return self

    def agrupar_por(self, *campos):
        self._campos_agrupar_por.extend(campos)
        return self

    def habiendo(self, condicion):
        self._condiciones_habiendo.append(condicion)
        return self

    def ordenar_por(self, campo, direccion="ASC"):
        self._campos_ordenar_por.append(f"{campo} {direccion}")
        return self

    def limite(self, cantidad):
        self._valor_limite = cantidad
        return self

    def construir(self) -> str:
        if not self._campos_seleccion or not self._tabla_desde:
            raise ValueError("SELECT y FROM son obligatorios para construir la consulta.")

        partes_consulta = []
        partes_consulta.append(f"SELECT {', '.join(self._campos_seleccion)}")
        partes_consulta.append(f"FROM {self._tabla_desde}")
        if self._uniones:
            partes_consulta.extend(self._uniones)
        if self._condiciones_donde:
            partes_consulta.append(f"WHERE {' AND '.join(self._condiciones_donde)}")
        if self._campos_agrupar_por:
            partes_consulta.append(f"GROUP BY {', '.join(self._campos_agrupar_por)}")
        if self._condiciones_habiendo:
            partes_consulta.append(f"HAVING {' AND '.join(self._condiciones_habiendo)}")
        if self._campos_ordenar_por:
            partes_consulta.append(f"ORDER BY {', '.join(self._campos_ordenar_por)}")
        if self._valor_limite is not None: # Comprobar contra None, ya que limite 0 puede ser válido
            partes_consulta.append(f"LIMIT {self._valor_limite}")

        return "\n".join(partes_consulta) + ";"

    def ejecutar(self, parametros: dict = None) -> pd.DataFrame | None:
        """
        Ejecuta la consulta SQL construida y devuelve los resultados como un DataFrame.
        """
        from src.conexion_bd import ConexionBD

        consulta_sql = self.construir()
        resultados = None
        try:
            # Obtener conexión usando el patrón Singleton
            conexion = ConexionBD()
            # Ejecutar la consulta
            resultados = conexion.ejecutar_consulta(consulta_sql, parametros)

            if resultados is None:
                pass
            return resultados
        except Exception as e:
            self.logger.error(f"Excepción en ConstructorConsultaSQL.ejecutar() para consulta '{consulta_sql[:50]}...': {e}", exc_info=True)
            # Re-elevar como un error más genérico o específico del constructor
            raise ValueError(f"Error al ejecutar la consulta construida: {str(e)}") from e

if __name__ == "__main__":
    constructor = ConstructorConsultaSQL()

    try:
        # Este ejemplo solo construye la consulta, no la ejecuta aquí para evitar
        # dependencias de BD al correr este script directamente de forma aislada.
        consulta_generada = (constructor
            .seleccionar("c.CategoryName as Categoría", "p.ProductName as Producto", "SUM(s.Quantity) as TotalVendido")
            .desde_tabla("Sales s")
            .unir("Products p", "s.ProductID = p.ProductID")
            .unir("Categories c", "p.CategoryID = c.CategoryID")
            .agrupar_por("c.CategoryName", "p.ProductName")
            .ordenar_por("c.CategoryName", "ASC")
            .ordenar_por("TotalVendido", "DESC")
            .limite(10)
            .construir() # Solo construir
        )
        print("Consulta generada por el constructor:")
        print(consulta_generada)

    except ValueError as ve:
        print(f"\nError durante el ejemplo del constructor: {ve}")
    except Exception as e_main:
        print(f"\nError inesperado en el ejemplo del constructor: {e_main}")
