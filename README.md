# Sistema de Análisis de Ventas

Sistema robusto y escalable para analizar datos de ventas de una empresa de comestibles que opera en varias ciudades del país.

## Descripción del Proyecto

Este sistema procesa datos de ventas a partir de archivos CSV, almacena la información en una base de datos relacional MySQL, permite realizar análisis avanzados mediante consultas SQL y modela el sistema en Python aplicando principios de programación orientada a objetos.

## Estructura del Proyecto

```
sales-analysis-system/
├── data/                   # Archivos CSV de datos
├── sql/                    # Scripts SQL
│   └── load_data.sql       # Script para cargar datos
├── src/                    # Código fuente Python
│   ├── models/             # Clases del modelo de datos
│   ├── database/           # Conexión y operaciones DB
│   └── utils/              # Utilidades y helpers
├── tests/                  # Pruebas unitarias
├── .env                    # Variables de entorno
├── .gitignore              # Archivos ignorados por Git
├── requirements.txt        # Dependencias Python
└── README.md               # Este archivo
```

## Entidades del Sistema

El sistema maneja las siguientes entidades principales:

- **Categories**: Categorías de productos
- **Products**: Información de productos disponibles
- **Customers**: Datos de clientes
- **Cities**: Información geográfica de ciudades
- **Countries**: Metadatos de países
- **Employees**: Detalles de empleados
- **Sales**: Datos transaccionales de ventas

## Configuración del Entorno

### 1. Crear entorno virtual

```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

Renombra `.env.example` a `.env` y configura tus credenciales de base de datos:

```env
DB_HOST=localhost
DB_PORT=3306
DB_NAME=sales_system
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
```

## Carga de Datos

1. Coloca los archivos CSV en la carpeta `data/`
2. Ejecuta el script `sql/load_data.sql` en MySQL Workbench
3. Verifica que los datos se hayan cargado correctamente

## Uso

### Ejecutar el sistema

```bash
python src/main.py
```

### Ejecutar pruebas

```bash
pytest tests/
```

### Ejecutar pruebas con cobertura

```bash
pytest --cov=src tests/
```

## Tecnologías Utilizadas

- **Python 3.8+**: Lenguaje principal
- **MySQL**: Base de datos relacional
- **pandas**: Manipulación de datos
- **mysql-connector-python**: Conectividad con MySQL
- **pytest**: Framework de testing
- **python-dotenv**: Gestión de variables de entorno

## Principios de Desarrollo

- **Programación Orientada a Objetos**: Encapsulamiento, herencia, polimorfismo
- **Patrones de Diseño**: Repository, Factory, Singleton
- **Testing**: Pruebas unitarias con pytest
- **Clean Code**: Código limpio y mantenible