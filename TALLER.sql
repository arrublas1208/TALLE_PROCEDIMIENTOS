-- Crea un evento que aumente la superficie de los países en 5% cada año.

DELIMITER $$

CREATE EVENT aumentar_superficie_paises
ON SCHEDULE 
    EVERY 1 YEAR 
DO
BEGIN
    UPDATE world_C3.country  
    SET SurfaceArea = SurfaceArea * 1.05  
    WHERE SurfaceArea IS NOT NULL;       
END$$

DELIMITER ;


-- Crea un evento que registre en una tabla la cantidad de países por continente cada mes.

DELIMITER $$

CREATE EVENT registrar_paises_por_continente
ON SCHEDULE
  EVERY 1 MONTH 
DO
BEGIN
  
  CREATE TABLE IF NOT EXISTS world_C3.estadisticas_continentes (
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    continente VARCHAR(15),
    cantidad_paises INT
  );
  
  INSERT INTO world_C3.estadisticas_continentes (continente, cantidad_paises)
  SELECT Continent, COUNT(*) 
  FROM world_C3.country
  GROUP BY Continent;
END$$

DELIMITER ;



-- Programa un evento que guarde un registro de cambios de población cada semana.

DELIMITER &&

CREATE EVENT guardar_cambios_poblacion
ON SCHEDULE 
EVERY 1 WEEK
DO
BEGIN 
	CREATE TABLE IF NOT EXISTS world_C3.cambios(
	fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	Name varchar(52),
	Population int);

INSERT INTO world_C3.cambios(Name, Population)
SELECT c.name, c.Population
FROM world_C3.country c;
END&&
DELIMITER ;



-- Crea un evento que elimine países sin ciudades registradas cada 3 meses. 
-- Este evento debe dejar una traza de cuáles fueron los países eliminado en otra tabla

DELIMITER $$

CREATE TABLE IF NOT EXISTS world_c3.paises_eliminados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    codigo_pais CHAR(3),
    nombre_pais VARCHAR(52)
);

CREATE PROCEDURE eliminar_paises_sin_ciudades()
BEGIN 
    INSERT INTO world_c3.paises_eliminados (codigo_pais, nombre_pais)
    SELECT c.Code, c.Name
    FROM world_c3.country c
    LEFT JOIN world_c3.city ct ON ct.CountryCode = c.Code
    GROUP BY c.Code, c.Name
    HAVING COUNT(ct.Name) = 0;
    
    DELETE FROM world_c3.country
    WHERE Code IN (
        SELECT c.Code
        FROM world_c3.country c
        LEFT JOIN world_c3.city ct ON ct.CountryCode = c.Code
        GROUP BY c.Code
        HAVING COUNT(ct.Name) = 0
    );
    
    SELECT CONCAT('Países eliminados: ', ROW_COUNT()) AS resultado;
END$$

CREATE EVENT limpieza_trimestral_paises
ON SCHEDULE 
    EVERY 3 MONTH
    STARTS CURRENT_TIMESTAMP
DO
BEGIN
    CALL eliminar_paises_sin_ciudades();
END$$

DELIMITER ;


-- Crear un evento que elimine y mueva a otra tabla todos los datos de los  
-- países que se independizaron hace más de 500 años. Este evento ocurre cada viernes.


DELIMITER $$

CREATE TABLE IF NOT EXISTS world_c3.paises_historicos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fecha_movimiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    codigo_pais CHAR(3),
    nombre_pais VARCHAR(52),
    anio_independencia SMALLINT,
    continente VARCHAR(15),
    poblacion INT
);

CREATE PROCEDURE mover_paises_antiguos()
BEGIN
    INSERT INTO world_c3.paises_historicos (
        codigo_pais, 
        nombre_pais, 
        anio_independencia, 
        continente, 
        poblacion
    )
    SELECT 
        Code, 
        Name, 
        IndepYear, 
        Continent, 
        Population
    FROM 
        world_c3.country
    WHERE 
        IndepYear IS NOT NULL 
        AND (YEAR(CURRENT_DATE) - IndepYear) > 500;
    
    DELETE FROM 
        world_c3.country
    WHERE 
        Code IN (
            SELECT codigo_pais 
            FROM world_c3.paises_historicos 
            WHERE DATE(fecha_movimiento) = CURRENT_DATE()
        );
    
    SELECT CONCAT('Países movidos: ', ROW_COUNT()) AS resultado;
END$$

CREATE EVENT limpieza_paises_antiguos
ON SCHEDULE 
    EVERY 1 WEEK
    STARTS NEXT_DAY(CURRENT_DATE, 'FRIDAY') + INTERVAL 0 HOUR
DO
BEGIN
    CALL mover_paises_antiguos();
END$$

DELIMITER ;
