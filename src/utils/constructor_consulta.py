import sys
import os
from pathlib import Path

root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

from src.conexion_bd import ConexionBD
import pandas as pd

class ConstructorConsultaSQL:
    """
    Patrón Constructor para crear consultas SQL complejas de manera fluida.
    Resuelve el problema de crear consultas SQL complejas de manera legible y mantenible.
    También permite ejecutarlas directamente.
    """
    
    def __init__(self):
        self.reiniciar()
    
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
        """Añade campos al SELECT."""
        self._campos_seleccion.extend(campos)
        return self
    
    def desde_tabla(self, tabla):
        """Establece la tabla principal."""
        self._tabla_desde = tabla
        return self
    
    def unir(self, tabla, condicion_union, tipo_union="INNER"):
        """Añade un JOIN a la consulta."""
        self._uniones.append(f"{tipo_union} JOIN {tabla} ON {condicion_union}")
        return self
    
    def unir_izquierda(self, tabla, condicion_union):
        """Añade un LEFT JOIN."""
        return self.unir(tabla, condicion_union, "LEFT")
    
    def donde(self, condicion):
        """Añade condición WHERE."""
        self._condiciones_donde.append(condicion)
        return self
    
    def agrupar_por(self, *campos):
        """Añade campos al GROUP BY."""
        self._campos_agrupar_por.extend(campos)
        return self
    
    def habiendo(self, condicion):
        """Añade condición HAVING."""
        self._condiciones_habiendo.append(condicion)
        return self
    
    def ordenar_por(self, campo, direccion="ASC"):
        """Añade campo al ORDER BY."""
        self._campos_ordenar_por.append(f"{campo} {direccion}")
        return self
    
    def limite(self, cantidad):
        """Añade LIMIT."""
        self._valor_limite = cantidad
        return self
    
    def construir(self) -> str:
        """Construye y retorna la consulta SQL final."""
        if not self._campos_seleccion or not self._tabla_desde:
            raise ValueError("SELECT y FROM son obligatorios")
        
        partes_consulta = []
        
        # SELECT
        partes_consulta.append(f"SELECT {', '.join(self._campos_seleccion)}")
        
        # FROM
        partes_consulta.append(f"FROM {self._tabla_desde}")
        
        # JOINs
        if self._uniones:
            partes_consulta.extend(self._uniones)
        
        # WHERE
        if self._condiciones_donde:
            partes_consulta.append(f"WHERE {' AND '.join(self._condiciones_donde)}")
        
        # GROUP BY
        if self._campos_agrupar_por:
            partes_consulta.append(f"GROUP BY {', '.join(self._campos_agrupar_por)}")
        
        # HAVING
        if self._condiciones_habiendo:
            partes_consulta.append(f"HAVING {' AND '.join(self._condiciones_habiendo)}")
        
        # ORDER BY
        if self._campos_ordenar_por:
            partes_consulta.append(f"ORDER BY {', '.join(self._campos_ordenar_por)}")
        
        # LIMIT
        if self._valor_limite:
            partes_consulta.append(f"LIMIT {self._valor_limite}")
        
        return "\n".join(partes_consulta) + ";"
    
    def ejecutar(self, parametros: dict = None) -> pd.DataFrame:
        """
        Ejecuta la consulta SQL construida y devuelve los resultados como un DataFrame.
        
        Args:
            parametros (dict, optional): Parámetros para la consulta SQL. Por defecto None.
            
        Returns:
            pd.DataFrame: DataFrame con los resultados de la consulta.
            
        Raises:
            ValueError: Si la consulta no se pudo ejecutar o no hay conexión a la BD.
        """
        # Construir la consulta
        consulta_sql = self.construir()
        
        try:
            # Obtener conexión usando el patrón Singleton
            conexion = ConexionBD()
            
            # Ejecutar la consulta
            resultados = conexion.ejecutar_consulta(consulta_sql, parametros)
            
            if resultados is None:
                raise ValueError("Error al ejecutar la consulta. Revisa la conexión a la base de datos.")
            
            return resultados
        except Exception as e:
            raise ValueError(f"Error al ejecutar la consulta: {str(e)}")

# Ejemplo de uso
if __name__ == "__main__":
    constructor = ConstructorConsultaSQL()
    
    # Construir consulta: Top 5 productos más vendidos por categoría
    consulta = (constructor
        .seleccionar("c.CategoryName as Categoría", "p.ProductName as Producto", "SUM(s.Quantity) as TotalVendido")
        .desde_tabla("Sales s")
        .unir("Products p", "s.ProductID = p.ProductID")
        .unir("Categories c", "p.CategoryID = c.CategoryID")
        .agrupar_por("c.CategoryName", "p.ProductName")
        .ordenar_por("c.CategoryName", "ASC")
        .ordenar_por("TotalVendido", "DESC")
        .limite(10)
        )
    
    print("Consulta generada:")
    print(consulta.construir())
    
    try:
        # Ejecutar la consulta y mostrar resultados
        print("\nEjecutando consulta...")
        resultados = consulta.ejecutar()
        
        print("\nResultados de la consulta:")
        print(resultados)
    except ValueError as e:
        print(f"\nError: {e}")
        print("Asegúrate de que la base de datos esté configurada correctamente.")