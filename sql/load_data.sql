-- ============================================
-- Script para crear y cargar la base de datos del sistema de ventas
-- ============================================

-- Crear la base de datos si no existe
CREATE DATABASE IF NOT EXISTS sales_system;
USE sales_system;

-- Configurar el modo SQL para permitir carga de datos locales
SET SQL_MODE = '';
SET GLOBAL local_infile = 1;

-- ============================================
-- CREAR TABLAS
-- ============================================

-- Tabla Countries
DROP TABLE IF EXISTS countries;
CREATE TABLE countries (
    CountryID INT PRIMARY KEY AUTO_INCREMENT,
    CountryName VARCHAR(45) NOT NULL,
    CountryCode VARCHAR(2) NOT NULL
);

-- Tabla Cities
DROP TABLE IF EXISTS cities;
CREATE TABLE cities (
    CityID INT PRIMARY KEY AUTO_INCREMENT,
    CityName VARCHAR(45) NOT NULL,
    Zipcode DECIMAL(5,0),
    CountryID INT NOT NULL,
    FOREIGN KEY (CountryID) REFERENCES countries(CountryID)
);

-- Tabla Categories
DROP TABLE IF EXISTS categories;
CREATE TABLE categories (
    CategoryID INT PRIMARY KEY AUTO_INCREMENT,
    CategoryName VARCHAR(45) NOT NULL
);

-- Tabla Products
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(45) NOT NULL,
    Price DECIMAL(10,2) NOT NULL,
    CategoryID INT NOT NULL,
    Class VARCHAR(45),
    ModifyDate DATE,
    Resistant VARCHAR(45),
    IsAllergic VARCHAR(10),
    WarningDays DECIMAL(3,0),
    FOREIGN KEY (CategoryID) REFERENCES categories(CategoryID)
);

-- Tabla Customers
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    CustomerID INT PRIMARY KEY AUTO_INCREMENT,
    FirstName VARCHAR(45) NOT NULL,
    MiddleName VARCHAR(1),
    LastName VARCHAR(45) NOT NULL,
    CityID INT NOT NULL,
    Address VARCHAR(90),
    FOREIGN KEY (CityID) REFERENCES cities(CityID)
);

-- Tabla Employees
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    EmployeeID INT PRIMARY KEY AUTO_INCREMENT,
    FirstName VARCHAR(45) NOT NULL,
    MiddleName VARCHAR(1),
    LastName VARCHAR(45) NOT NULL,
    BirthDate DATE,
    Gender VARCHAR(1),
    CityID INT NOT NULL,
    HireDate DATE,
    FOREIGN KEY (CityID) REFERENCES cities(CityID)
);

-- Tabla Sales
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    SalesID INT PRIMARY KEY AUTO_INCREMENT,
    SalesPersonID INT NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    Discount DECIMAL(10,2) DEFAULT 0,
    TotalPrice DECIMAL(10,2) NOT NULL,
    SalesDate DATETIME NOT NULL,
    TransactionNumber VARCHAR(255),
    FOREIGN KEY (SalesPersonID) REFERENCES employees(EmployeeID),
    FOREIGN KEY (CustomerID) REFERENCES customers(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES products(ProductID)
);

-- ============================================
-- CARGAR DATOS DESDE ARCHIVOS CSV
-- ============================================

-- IMPORTANTE: Ajustar las rutas según tu sistema operativo
-- Para Windows: 'C:/ruta/completa/al/archivo.csv'
-- Para Linux/Mac: '/ruta/completa/al/archivo.csv'

-- Cargar Countries
LOAD DATA LOCAL INFILE './data/countries.csv'
INTO TABLE countries
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(CountryName, CountryCode);

-- Cargar Cities
LOAD DATA LOCAL INFILE './data/cities.csv'
INTO TABLE cities
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(CityName, Zipcode, CountryID);

-- Cargar Categories
LOAD DATA LOCAL INFILE './data/categories.csv'
INTO TABLE categories
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(CategoryName);

-- Cargar Products
LOAD DATA LOCAL INFILE './data/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(ProductName, Price, CategoryID, Class, ModifyDate, Resistant, IsAllergic, WarningDays);

-- Cargar Customers
LOAD DATA LOCAL INFILE './data/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(FirstName, MiddleName, LastName, CityID, Address);

-- Cargar Employees
LOAD DATA LOCAL INFILE './data/employees.csv'
INTO TABLE employees
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(FirstName, MiddleName, LastName, BirthDate, Gender, CityID, HireDate);

-- Cargar Sales
LOAD DATA LOCAL INFILE './data/sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(SalesPersonID, CustomerID, ProductID, Quantity, Discount, TotalPrice, SalesDate, TransactionNumber);

-- ============================================
-- VERIFICAR CARGA DE DATOS
-- ============================================

-- Mostrar conteo de registros por tabla
SELECT 'countries' AS tabla, COUNT(*) AS registros FROM countries
UNION ALL
SELECT 'cities' AS tabla, COUNT(*) AS registros FROM cities
UNION ALL
SELECT 'categories' AS tabla, COUNT(*) AS registros FROM categories
UNION ALL
SELECT 'products' AS tabla, COUNT(*) AS registros FROM products
UNION ALL
SELECT 'customers' AS tabla, COUNT(*) AS registros FROM customers
UNION ALL
SELECT 'employees' AS tabla, COUNT(*) AS registros FROM employees
UNION ALL
SELECT 'sales' AS tabla, COUNT(*) AS registros FROM sales;

-- Verificar integridad referencial
SELECT 'Productos sin categoría' AS verificacion, COUNT(*) AS problemas
FROM products p LEFT JOIN categories c ON p.CategoryID = c.CategoryID
WHERE c.CategoryID IS NULL

UNION ALL

SELECT 'Ventas sin empleado' AS verificacion, COUNT(*) AS problemas
FROM sales s LEFT JOIN employees e ON s.SalesPersonID = e.EmployeeID
WHERE e.EmployeeID IS NULL

UNION ALL

SELECT 'Ventas sin cliente' AS verificacion, COUNT(*) AS problemas
FROM sales s LEFT JOIN customers c ON s.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL

UNION ALL

SELECT 'Ventas sin producto' AS verificacion, COUNT(*) AS problemas
FROM sales s LEFT JOIN products p ON s.ProductID = p.ProductID
WHERE p.ProductID IS NULL;

-- ============================================
-- CONSULTAS DE EJEMPLO PARA VERIFICAR
-- ============================================

-- Top 5 productos más vendidos
SELECT 
    p.ProductName,
    SUM(s.Quantity) as TotalVendido,
    SUM(s.TotalPrice) as IngresoTotal
FROM sales s
JOIN products p ON s.ProductID = p.ProductID
GROUP BY p.ProductID, p.ProductName
ORDER BY TotalVendido DESC
LIMIT 5;

-- Ventas por ciudad
SELECT 
    ci.CityName,
    co.CountryName,
    COUNT(s.SalesID) as NumeroVentas,
    SUM(s.TotalPrice) as IngresoTotal
FROM sales s
JOIN customers cu ON s.CustomerID = cu.CustomerID
JOIN cities ci ON cu.CityID = ci.CityID
JOIN countries co ON ci.CountryID = co.CountryID
GROUP BY ci.CityID, ci.CityName, co.CountryName
ORDER BY IngresoTotal DESC;