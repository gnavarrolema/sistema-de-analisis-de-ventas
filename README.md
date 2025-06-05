# Sistema de Análisis de Ventas

## Descripción del Proyecto

Este sistema robusto y escalable ha sido desarrollado para analizar datos de ventas de una empresa de comestibles que opera en varias ciudades del país. Inicialmente enfocado en el procesamiento de datos desde archivos CSV, su almacenamiento en una base de datos relacional MySQL y el modelado del sistema en Python aplicando principios de Programación Orientada a Objetos, el proyecto ha evolucionado para optimizar consultas y automatizar procesos clave.

En sus fases más recientes, el sistema se ha potenciado con la implementación de consultas SQL avanzadas (utilizando CTEs y Funciones de Ventana) y objetos SQL (Vistas y Procedimientos Almacenados). Estas mejoras lo convierten en una herramienta no solo robusta, sino también estratégica, capaz de generar reportes analíticos complejos de manera eficiente, facilitar la toma de decisiones informadas y operar con agilidad frente al crecimiento del volumen de datos.

## Estructura del Proyecto

```
sistema-de-analisis-de-ventas/
├── data/                   # Archivos CSV de datos (ej. products.csv, sales.csv, etc.)
├── sql/                    # Scripts SQL
│   └── load_data.sql       # Script para crear tablas y cargar datos iniciales
├── src/                    # Código fuente Python
│   ├── modelos/            # Clases del modelo de datos (Producto, Cliente, Venta, etc.)
│   ├── utils/              # Clases de utilidad (ConexionBD, FabricaModelos, ConstructorConsultaSQL, etc.)
├── tests/                  # Pruebas unitarias y de integración (con pytest)
├── .env                    # Variables de entorno para configuración (ej. credenciales BD)
├── .gitignore              # Archivos y directorios ignorados por Git
├── requirements.txt        # Dependencias del proyecto Python
├── pytest.ini              # Configuración para pytest
├── demostracion_sistema.ipynb # Notebook de Jupyter para demostración interactiva del sistema
├── venv
└── README.md               
```

## Entidades del Sistema

El sistema maneja las siguientes entidades principales, reflejadas en la estructura de la base de datos y las clases de Python:

- **Categories**: Categorías de productos
- **Products**: Información de productos disponibles
- **Customers**: Datos de clientes
- **Cities**: Información geográfica de ciudades
- **Countries**: Metadatos de países
- **Employees**: Detalles de empleados
- **Sales**: Datos transaccionales de ventas

## Configuración del Entorno

### 1. Requisitos Previos

- Python 3.8 o superior
- Servidor de base de datos MySQL

### 2. Crear Entorno Virtual

Se recomienda utilizar un entorno virtual:

```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

### 3. Instalar Dependencias

Las dependencias se gestionan a través del archivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno

Crea un archivo `.env` con las siguientes credenciales de base de datos y otros parámetros necesarios:

```env
DB_HOST=localhost
DB_PORT=3306
DB_NAME=sistema_de_analisis_de_ventas # Asegúrate que coincida con tu BD
DB_USER=tu_usuario_mysql
DB_PASSWORD=tu_contraseña_mysql

# Variables para el sistema de logging y cache (opcionales, con valores por defecto)
# NIVEL_LOGGING=INFO
# CACHE_CONSULTAS_HABILITADO=True
# CACHE_DURACION_MINUTOS=15
```

**Importante:** El archivo `.env` está incluido en `.gitignore` para no exponer credenciales en el repositorio.

## Carga de Datos

- **Archivos CSV**: Asegúrate de que los archivos CSV con los datos fuente estén ubicados en el directorio `data/`.

- **Script SQL**: El script `sql/load_data.sql` contiene las sentencias DDL para crear la estructura de la base de datos y las tablas, así como los comandos `LOAD DATA LOCAL INFILE` para cargar los datos desde los CSV.

- **Nota sobre LOAD DATA LOCAL INFILE**: Es posible que necesites habilitar la opción `local_infile` en tu servidor MySQL y/o en la configuración de la conexión. Revisa la documentación de MySQL si encuentras problemas. Asegúrate de que las rutas a los archivos CSV en `load_data.sql` sean correctas para tu entorno local donde ejecutas el script SQL.

- **Ejecución**: Utiliza un cliente de MySQL, como MySQL Workbench, para conectarte a tu servidor y ejecutar el contenido de `sql/load_data.sql`. Esto creará la base de datos (si no existe), las tablas y las poblará con los datos.

## Uso del Sistema

### Ejecución Principal

Actualmente, la funcionalidad principal y las demostraciones se encuentran en el notebook de Jupyter.

### Notebook de Demostración

El archivo `demostracion_sistema.ipynb` sirve como una guía interactiva de las funcionalidades del sistema. Muestra:

- Conexión a la base de datos.
- Ejecución de consultas SQL simples y avanzadas.
- Uso de los patrones de diseño implementados (Factory, Builder).
- Demostración de objetos SQL (Vistas, Procedimientos Almacenados).
- Ejecución de pruebas unitarias (integrado mediante comandos de shell).

Se recomienda ejecutar este notebook celda por celda para entender el flujo y las capacidades del sistema.

### Ejecutar Pruebas Unitarias

El proyecto utiliza pytest para las pruebas:

```bash
pytest tests/
```

Para ver un informe de cobertura de pruebas:

```bash
pytest --cov=src tests/
```

## Principios de Desarrollo y Patrones Aplicados

El sistema se ha desarrollado siguiendo principios de Programación Orientada a Objetos (POO) y aplicando patrones de diseño para mejorar la modularidad, escalabilidad y mantenibilidad del código:

- **Encapsulamiento**: Protegiendo el estado interno de los objetos y exponiendo solo interfaces controladas.
- **Constructores Personalizados**: Para la correcta inicialización de los objetos del modelo.
- **Métodos Relevantes para el Negocio**: Funcionalidad específica dentro de las clases modelo (ej. `cliente.nombre_completo()`, `empleado.describir_antiguedad()`).
- **Código Limpio**: Esfuerzo por mantener el código legible, simple y bien documentado.
- **Testing Riguroso**: Pruebas unitarias para asegurar la fiabilidad de los componentes.

### Patrones de Diseño Implementados

#### Singleton (Patrón Creacional):

- **Implementación**: `ConexionBD` y `ConfiguradorLogging`, `GestorCacheConsultas`.
- **Justificación**: Asegura que exista una única instancia de la conexión a la base de datos, del configurador de logging y del gestor de caché en toda la aplicación. Esto previene la creación múltiple de conexiones (costoso en recursos), configuraciones de log inconsistentes y múltiples caches descoordinados.

#### Factory Method (Patrón Creacional):

- **Implementación**: `FabricaModelos`.
- **Justificación**: Centraliza la lógica de creación de objetos del modelo (Cliente, Producto, Empleado, etc.) a partir de diversas fuentes de datos (diccionarios, filas de DataFrames). Esto desacopla el código cliente de las clases concretas de los modelos, facilitando la adición de nuevos tipos de modelos o cambios en la lógica de instanciación sin afectar al resto del sistema.

#### Builder (Patrón Creacional):

- **Implementación**: `ConstructorConsultaSQL`.
- **Justificación**: Permite construir consultas SQL complejas paso a paso de una manera fluida y legible. Separa la construcción de una consulta de su representación final, lo que facilita la creación de diferentes tipos de consultas SQL o la modificación de partes de ellas sin alterar la clase constructora principal.

## Funcionalidades Avanzadas SQL (Tercer Avance)

Para mejorar la eficiencia y la capacidad analítica del sistema, se han implementado las siguientes funcionalidades SQL avanzadas:

### Consultas SQL Avanzadas

Se han desarrollado y demostrado consultas complejas en `demostracion_sistema.ipynb` utilizando:

- **Common Table Expressions (CTEs)**: Para mejorar la legibilidad y modularidad de consultas largas, permitiendo definir conjuntos de resultados temporales con nombre.
- **Funciones de Ventana** (ej. `RANK()`, `NTILE()`, `LAG()`): Para realizar cálculos sobre un conjunto de filas relacionadas con la fila actual, permitiendo análisis sofisticados como rankings, segmentación y cálculos de series temporales.

#### Ejemplos de Consultas Implementadas:

- **Ranking de Productos por Ventas Netas en Cada Categoría**: Identifica los productos estrella dentro de cada categoría.
- **Ranking de Rendimiento de Empleados**: Clasifica a los empleados según el valor total de ventas netas y la cantidad de productos vendidos.
- **Segmentación de Clientes por Frecuencia y Valor (FM)**: Agrupa a los clientes en segmentos basados en su comportamiento de compra para estrategias de marketing dirigidas.

### Objetos SQL Avanzados

Se han creado los siguientes objetos directamente en la base de datos para optimizar y reutilizar la lógica SQL:

#### Vista (View): `VistaVentasGlobalesDetalladas`

- **Propósito**: Proporciona una tabla virtual pre-unida con información detallada de cada venta, incluyendo datos de productos, categorías, clientes y empleados.
- **Beneficios**: Simplifica enormemente las consultas de reporte recurrentes, abstrae la complejidad de los JOINs y puede mejorar la seguridad al restringir el acceso a las tablas base.

#### Procedimiento Almacenado (Stored Procedure): `ResumenVentasPorEmpleado(IN id_empleado_param INT)`

- **Propósito**: Recibe un ID de empleado como parámetro y devuelve un resumen de los productos vendidos por ese empleado, incluyendo la cantidad total y el valor neto de las ventas por producto.
- **Beneficios**: Encapsula lógica de negocio en el servidor de la base de datos, reduce el tráfico de red (al enviar solo la llamada y recibir el resultado final), mejora el rendimiento para operaciones comunes y permite la reutilización de código SQL complejo.

## Tecnologías Utilizadas

- **Lenguaje Principal**: Python 3.8+
- **Base de Datos**: MySQL
- **Manipulación y Análisis de Datos**: pandas, NumPy
- **ORM y Conectividad DB**: SQLAlchemy (para la conexión y ejecución de consultas), pymysql / mysql-connector-python (como drivers de MySQL).
- **Gestión de Entorno y Dependencias**: venv, pip, requirements.txt
- **Variables de Entorno**: python-dotenv
- **Testing**: pytest, pytest-mock, pytest-cov
- **Notebooks (Demostración e Interacción)**: Jupyter Notebook / JupyterLab, IPython
- **Logging**: Módulo logging de Python, con configuración centralizada.
- **Métricas de Sistema**: psutil (para monitoreo básico de recursos en ConexionBD)
- **(Consideradas para el futuro o desarrollo)**: matplotlib, seaborn, plotly (para visualizaciones); pydantic (para validación avanzada de datos).