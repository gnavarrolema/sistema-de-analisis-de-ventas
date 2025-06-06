-- -----------------------------------------------------
-- Script para crear la base de datos, las tablas y cargar los datos
-- -----------------------------------------------------

-- Opcional: Habilitar la carga de archivos locales.
-- SET GLOBAL local_infile = 1; -- Ejecutar una vez en el servidor MySQL si es necesario.

SET FOREIGN_KEY_CHECKS = 0;

-- 1. Creación de la Base de Datos (si no existe)
CREATE DATABASE IF NOT EXISTS sistema_de_analisis_de_ventas CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE sistema_de_analisis_de_ventas;

-- -----------------------------------------------------
-- Tabla: countries
-- CSV: CountryID,CountryName,CountryCode [cite: 3]
-- -----------------------------------------------------
DROP TABLE IF EXISTS countries;
CREATE TABLE IF NOT EXISTS countries (
  CountryID INT NOT NULL,
  CountryName VARCHAR(45) NOT NULL,
  CountryCode VARCHAR(2) NULL,
  PRIMARY KEY (CountryID),
  UNIQUE INDEX CountryName_UNIQUE (CountryName ASC) VISIBLE,
  UNIQUE INDEX CountryCode_UNIQUE (CountryCode ASC) VISIBLE
) ENGINE = InnoDB;

LOAD DATA INFILE 'countries.csv'
INTO TABLE countries
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(CountryID, CountryName, CountryCode);

-- -----------------------------------------------------
-- Tabla: cities
-- CSV: CityID,CityName,Zipcode,CountryID [cite: 2]
-- -----------------------------------------------------
DROP TABLE IF EXISTS cities;
CREATE TABLE IF NOT EXISTS cities (
  CityID INT NOT NULL,
  CityName VARCHAR(45) NOT NULL,
  Zipcode VARCHAR(10) NULL,
  CountryID INT NOT NULL,
  PRIMARY KEY (CityID),
  INDEX fk_cities_countries_idx (CountryID ASC) VISIBLE,
  CONSTRAINT fk_cities_countries
    FOREIGN KEY (CountryID)
    REFERENCES countries (CountryID)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
) ENGINE = InnoDB;

LOAD DATA INFILE 'cities.csv'
INTO TABLE cities
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS
(CityID, CityName, Zipcode, CountryID);

-- -----------------------------------------------------
-- Tabla: categories
-- CSV: CategoryID,CategoryName [cite: 1]
-- -----------------------------------------------------
DROP TABLE IF EXISTS categories;
CREATE TABLE IF NOT EXISTS categories (
  CategoryID INT NOT NULL,
  CategoryName VARCHAR(45) NOT NULL,
  PRIMARY KEY (CategoryID),
  UNIQUE INDEX CategoryName_UNIQUE (CategoryName ASC) VISIBLE
) ENGINE = InnoDB;

LOAD DATA INFILE 'categories.csv'
INTO TABLE categories
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(CategoryID, CategoryName);

-- -----------------------------------------------------
-- Tabla: customers
-- CSV: CustomerID,FirstName,MiddleInitial,LastName,CityID,Address [cite: 4]
-- -----------------------------------------------------
DROP TABLE IF EXISTS customers;
CREATE TABLE IF NOT EXISTS customers (
  CustomerID INT NOT NULL,
  FirstName VARCHAR(45) NOT NULL,
  MiddleInitial VARCHAR(1) NULL,
  LastName VARCHAR(45) NOT NULL,
  Address VARCHAR(90) NULL,
  CityID INT NOT NULL,
  PRIMARY KEY (CustomerID),
  INDEX fk_customers_cities_idx (CityID ASC) VISIBLE,
  CONSTRAINT fk_customers_cities
    FOREIGN KEY (CityID)
    REFERENCES cities (CityID)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
) ENGINE = InnoDB;

LOAD DATA INFILE 'customers.csv'
INTO TABLE customers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(CustomerID, FirstName, MiddleInitial, LastName, CityID, Address);

-- -----------------------------------------------------
-- Tabla: employees
-- CSV: EmployeeID,FirstName,MiddleInitial,LastName,BirthDate,Gender,CityID,HireDate [cite: 5]
-- -----------------------------------------------------
DROP TABLE IF EXISTS employees;
CREATE TABLE IF NOT EXISTS employees (
  EmployeeID INT NOT NULL,
  FirstName VARCHAR(45) NOT NULL,
  MiddleInitial VARCHAR(1) NULL,
  LastName VARCHAR(45) NOT NULL,
  BirthDate DATE NULL,         -- CSV tiene YYYY-MM-DD HH:MM:SS.mmm, DATE tomará la parte de fecha
  Gender CHAR(1) NULL,
  CityID INT NOT NULL,
  HireDate DATE NULL,          -- CSV tiene YYYY-MM-DD HH:MM:SS.mmm, DATE tomará la parte de fecha (ERD usa DATE)
  PRIMARY KEY (EmployeeID),
  INDEX fk_employees_cities_idx (CityID ASC) VISIBLE,
  CONSTRAINT fk_employees_cities
    FOREIGN KEY (CityID)
    REFERENCES cities (CityID)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
) ENGINE = InnoDB;

LOAD DATA INFILE 'employees.csv'
INTO TABLE employees
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(EmployeeID, FirstName, MiddleInitial, LastName, @BirthDate, Gender, CityID, @HireDate)
SET
BirthDate = STR_TO_DATE(SUBSTRING_INDEX(@BirthDate, ' ', 1), '%Y-%m-%d'),
HireDate = STR_TO_DATE(SUBSTRING_INDEX(@HireDate, ' ', 1), '%Y-%m-%d'); -- Asume que la fecha está antes del primer espacio

-- -----------------------------------------------------
-- Tabla: products
-- CSV: ProductID,ProductName,Price,CategoryID,Class,ModifyDate,Resistant,IsAllergic,VitalityDays [cite: 6]
-- -----------------------------------------------------
DROP TABLE IF EXISTS products;
CREATE TABLE IF NOT EXISTS products (
  ProductID INT NOT NULL,
  ProductName VARCHAR(45) NOT NULL,
  Price DECIMAL(10,4) NOT NULL,
  CategoryID INT NOT NULL,
  Class VARCHAR(45) NULL,
  ModifyDate TIME NULL, 
  Resistant VARCHAR(45) NULL,
  IsAllergic VARCHAR(10) NULL,
  VitalityDays DECIMAL(3,0) NULL,
  PRIMARY KEY (ProductID),
  INDEX fk_products_categories_idx (CategoryID ASC) VISIBLE,
  CONSTRAINT fk_products_categories
    FOREIGN KEY (CategoryID)
    REFERENCES categories (CategoryID)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
) ENGINE = InnoDB;

LOAD DATA INFILE 'products.csv'
INTO TABLE products
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS
(ProductID, ProductName, Price, CategoryID, Class, @ModifyDate_str, Resistant, IsAllergic, VitalityDays)
SET ModifyDate = CASE
                    WHEN @ModifyDate_str IS NULL OR @ModifyDate_str = '' THEN NULL
                    ELSE TIME(CONCAT('00:', @ModifyDate_str)) -- Asume que @ModifyDate_str es MM:SS.s
                 END;

-- -----------------------------------------------------
-- Tabla: sales
-- CSV: SalesID,SalesPersonID,ProductID,CustomerID,Quantity,Discount,TotalPrice,SalesDate,TransactionNumber
-- -----------------------------------------------------
DROP TABLE IF EXISTS sales;
CREATE TABLE IF NOT EXISTS sales (
  SalesID INT NOT NULL,
  SalesPersonID INT NULL,
  ProductID INT NOT NULL,
  CustomerID INT NOT NULL,
  Quantity INT NOT NULL,
  Discount DECIMAL(10,2) NULL DEFAULT 0.00,
  TotalPrice DECIMAL(10,2) NOT NULL,
  SalesDate TIME NULL,  
  TransactionNumber VARCHAR(255) NULL,
  PRIMARY KEY (SalesID),
  INDEX fk_sales_employees_idx (SalesPersonID ASC) VISIBLE,
  INDEX fk_sales_customers_idx (CustomerID ASC) VISIBLE,
  INDEX fk_sales_products_idx (ProductID ASC) VISIBLE,
  CONSTRAINT fk_sales_employees
    FOREIGN KEY (SalesPersonID)
    REFERENCES employees (EmployeeID)
    ON DELETE SET NULL
    ON UPDATE CASCADE,
  CONSTRAINT fk_sales_customers
    FOREIGN KEY (CustomerID)
    REFERENCES customers (CustomerID)
    ON DELETE RESTRICT
    ON UPDATE CASCADE,
  CONSTRAINT fk_sales_products
    FOREIGN KEY (ProductID)
    REFERENCES products (ProductID)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
) ENGINE = InnoDB;

LOAD DATA INFILE 'sales.csv'
INTO TABLE sales
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS
(SalesID, SalesPersonID, ProductID, CustomerID, Quantity, Discount, TotalPrice, @SalesDate_str, TransactionNumber)
SET SalesDate = CASE
                  WHEN @SalesDate_str IS NULL OR @SalesDate_str = '' THEN NULL
                  ELSE TIME(CONCAT('00:', @SalesDate_str)) -- Asume que @SalesDate_str es MM:SS.s
                END;

SET FOREIGN_KEY_CHECKS = 1;

-- -----------------------------------------------------
-- Validación de la carga de datos
-- -----------------------------------------------------
-- Desactivar las comprobaciones de claves foráneas para evitar errores al truncar tablas
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE sales;
TRUNCATE TABLE products;
TRUNCATE TABLE employees;
TRUNCATE TABLE customers;
TRUNCATE TABLE categories;
TRUNCATE TABLE cities;
TRUNCATE TABLE countries;
SET FOREIGN_KEY_CHECKS = 1;
-- -----------------------------------------------------

-- Validación por conteo de filas en cada tabla

SELECT 'Países (countries)' AS Tabla, COUNT(*) AS Filas FROM countries
UNION ALL
SELECT 'Ciudades (cities)', COUNT(*) FROM cities
UNION ALL
SELECT 'Categorías (categories)', COUNT(*) FROM categories
UNION ALL
SELECT 'Clientes (customers)', COUNT(*) FROM customers
UNION ALL
SELECT 'Empleados (employees)', COUNT(*) FROM employees
UNION ALL
SELECT 'Productos (products)', COUNT(*) FROM products
UNION ALL
SELECT 'Ventas (sales)', COUNT(*) FROM sales;

-- Revisar la columna ModifyDate para algunos productos. Debido a que ModifyDate es TIME, se espera un formato de HH:MM:SS
SELECT ProductID, ProductName, Price, ModifyDate FROM products WHERE ProductID IN (1, 2, 3, 4, 5);

-- Revisar la columna SalesDate para algunas ventas. Debido a que SalesDate es TIME, se espera un formato de HH:MM:SS
SELECT SalesID, ProductID, CustomerID, TotalPrice, SalesDate FROM sales WHERE SalesID IN (624877, 712592, 245650, 566614, 4176244);
