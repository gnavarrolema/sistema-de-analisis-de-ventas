import pandas as pd
from decimal import Decimal, InvalidOperation
from datetime import time, date
from pathlib import Path

"""
Script de validación aislado para revisar la integridad y el formato
de los archivos CSV antes de su carga en la base de datos.

Este script NO MODIFICA los archivos, solo genera un reporte en la consola.

Ejecución desde la terminal (en la raíz del proyecto):
python validar_csv.py
"""

# --- Configuración de Archivos y Columnas ---

RUTA_BASE_DATOS = Path("data/")

# Definición de las columnas esperadas y sus validaciones para cada archivo.
# 'tipo': el tipo de dato de Python esperado.
# 'requerido': True si la columna no puede tener valores nulos.
ESQUEMAS = {
    "products.csv": {
        "columnas": ["ProductID", "ProductName", "Price", "CategoryID", "ModifyDate"],
        "validaciones": {
            "ProductID": {'tipo': int, 'requerido': True},
            "ProductName": {'tipo': str, 'requerido': True},
            "Price": {'tipo': Decimal, 'requerido': True},
            "CategoryID": {'tipo': int, 'requerido': True},
            "ModifyDate": {'tipo': "time_mm_ss", 'requerido': False}
        }
    },
    "sales.csv": {
        "columnas": ["SalesID", "ProductID", "CustomerID", "Quantity", "TotalPrice", "SalesDate"],
        "validaciones": {
            "SalesID": {'tipo': int, 'requerido': True},
            "ProductID": {'tipo': int, 'requerido': True},
            "CustomerID": {'tipo': int, 'requerido': True},
            "Quantity": {'tipo': int, 'requerido': True},
            "TotalPrice": {'tipo': Decimal, 'requerido': True},
            "SalesDate": {'tipo': "time_mm_ss", 'requerido': False},
            "SalesPersonID": {'tipo': int, 'requerido': False} # Puede ser nulo
        }
    },
    "employees.csv": {
        "columnas": ["EmployeeID", "FirstName", "LastName", "CityID", "BirthDate", "HireDate"],
        "validaciones": {
            "EmployeeID": {'tipo': int, 'requerido': True},
            "FirstName": {'tipo': str, 'requerido': True},
            "LastName": {'tipo': str, 'requerido': True},
            "CityID": {'tipo': int, 'requerido': True},
            "BirthDate": {'tipo': date, 'requerido': False},
            "HireDate": {'tipo': date, 'requerido': False}
        }
    },
    "customers.csv": {
        "columnas": ["CustomerID", "FirstName", "LastName", "CityID"],
        "validaciones": {
            "CustomerID": {'tipo': int, 'requerido': True},
            "FirstName": {'tipo': str, 'requerido': True},
            "LastName": {'tipo': str, 'requerido': True},
            "CityID": {'tipo': int, 'requerido': True}
        }
    },
    "categories.csv": {
        "columnas": ["CategoryID", "CategoryName"],
        "validaciones": {
            "CategoryID": {'tipo': int, 'requerido': True},
            "CategoryName": {'tipo': str, 'requerido': True}
        }
    },
    "cities.csv": {
        "columnas": ["CityID", "CityName", "CountryID"],
        "validaciones": {
            "CityID": {'tipo': int, 'requerido': True},
            "CityName": {'tipo': str, 'requerido': True},
            "CountryID": {'tipo': int, 'requerido': True}
        }
    },
    "countries.csv": {
        "columnas": ["CountryID", "CountryName"],
        "validaciones": {
            "CountryID": {'tipo': int, 'requerido': True},
            "CountryName": {'tipo': str, 'requerido': True}
        }
    }
}

def validar_tipo_time(valor_str: str) -> bool:
    """Valida el formato de tiempo específico 'MM:SS.f'."""
    if pd.isna(valor_str):
        return True  # Aceptamos nulos si no es requerido
    try:
        partes = str(valor_str).split(':')
        if len(partes) != 2: return False
        minutos = int(partes[0])
        segundos_partes = partes[1].split('.')
        segundos = int(segundos_partes[0])
        if not (0 <= minutos <= 59 and 0 <= segundos <= 59):
            return False
        return True
    except (ValueError, TypeError, IndexError):
        return False

def validar_archivo(nombre_archivo: str, config: dict) -> list:
    """Función genérica para validar un archivo CSV según su configuración."""
    ruta_completa = RUTA_BASE_DATOS / nombre_archivo
    errores = []
    
    try:
        df = pd.read_csv(ruta_completa, keep_default_na=False, na_values=['', 'NA', 'N/A', 'NULL'])
    except FileNotFoundError:
        errores.append(f"Error Crítico: El archivo '{ruta_completa}' no fue encontrado.")
        return errores

    # 1. Validación de columnas
    columnas_esperadas = set(config["columnas"])
    columnas_reales = set(df.columns)
    if not columnas_esperadas.issubset(columnas_reales):
        columnas_faltantes = columnas_esperadas - columnas_reales
        errores.append(f"Columnas faltantes: {list(columnas_faltantes)}")
        return errores # Error crítico, no se puede continuar

    # 2. Validación de filas
    for indice, fila in df.iterrows():
        nro_fila_real = indice + 2
        for columna, reglas in config["validaciones"].items():
            # Si la columna no existe en el archivo, pero sí en la validación, la ignoramos.
            if columna not in fila:
                continue
                
            valor = fila[columna]
            
            # Validar requeridos
            if reglas.get('requerido') and pd.isna(valor):
                errores.append(f"Fila {nro_fila_real}: El valor requerido para '{columna}' está vacío.")
                continue

            if pd.isna(valor):
                continue
            
            # Validar tipos
            tipo_esperado = reglas['tipo']
            valido = True
            try:
                if tipo_esperado == int:
                    int(valor)
                elif tipo_esperado == str:
                    str(valor)
                elif tipo_esperado == Decimal:
                    Decimal(str(valor))
                elif tipo_esperado == date:
                    # El formato en el CSV es 'YYYY-MM-DD HH:MM:SS.mmm', tomamos solo la fecha.
                    date.fromisoformat(str(valor).split()[0])
                elif tipo_esperado == "time_mm_ss":
                    valido = validar_tipo_time(valor)
            except (ValueError, TypeError, InvalidOperation, IndexError):
                valido = False

            if not valido:
                tipo_nombre = tipo_esperado.__name__ if hasattr(tipo_esperado, '__name__') else tipo_esperado
                errores.append(f"Fila {nro_fila_real}: El valor '{valor}' en la columna '{columna}' no tiene el formato esperado '{tipo_nombre}'.")

    return errores

def main():
    """Función principal que ejecuta todas las validaciones."""
    print("--- INICIANDO VALIDACIÓN DE ARCHIVOS CSV ---")
    
    total_errores = 0
    archivos_procesados = 0
    for archivo, config in ESQUEMAS.items():
        print(f"\n[+] Validando archivo: {archivo}...")
        archivos_procesados += 1
        errores_encontrados = validar_archivo(archivo, config)
        
        if not errores_encontrados:
            print("    └─ [✓] ¡Archivo saludable! No se encontraron errores de formato o datos requeridos.")
        else:
            print(f"    └─ [✗] Se encontraron {len(errores_encontrados)} problemas:")
            total_errores += len(errores_encontrados)
            # Imprimir los primeros 5 errores para no saturar la consola
            for error in errores_encontrados[:5]:
                print(f"        - {error}")
            if len(errores_encontrados) > 5:
                print("        - ... (y más errores)")

    print("\n--- VALIDACIÓN FINALIZADA ---")
    print(f"Se revisaron {archivos_procesados} archivos.")
    if total_errores == 0:
        print("¡Excelente! Todos los archivos revisados pasaron las validaciones.")
    else:
        print(f"Se encontró un total de {total_errores} problemas en los archivos.")


if __name__ == "__main__":
    main()