-- =====================================================
-- COVID-19 DATA ANALYSIS PROJECT
-- Database: ProtfolioProject
-- =====================================================

-- Step 1: Create and Use Database
-- =====================================================
CREATE DATABASE ProtfolioProject;
USE ProtfolioProject;

-- Enable local infile loading for CSV import
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';

-- =====================================================
-- Step 2: Create Tables
-- =====================================================

-- Create CovidDeaths table with ALL columns allowing NULL
CREATE TABLE IF NOT EXISTS covid_deaths (
    iso_code VARCHAR(10) NULL,
    continent VARCHAR(50) NULL,
    location VARCHAR(100) NULL,
    date DATE NULL,
    population BIGINT NULL,
    total_cases INT NULL,
    new_cases INT NULL,
    new_cases_smoothed DECIMAL(10,2) NULL,
    total_deaths INT NULL,
    new_deaths INT NULL,
    new_deaths_smoothed DECIMAL(10,2) NULL,
    total_cases_per_million DECIMAL(10,2) NULL,
    new_cases_per_million DECIMAL(10,2) NULL,
    new_cases_smoothed_per_million DECIMAL(10,2) NULL,
    total_deaths_per_million DECIMAL(10,2) NULL,
    new_deaths_per_million DECIMAL(10,2) NULL,
    new_deaths_smoothed_per_million DECIMAL(10,2) NULL,
    reproduction_rate DECIMAL(5,2) NULL,
    icu_patients INT NULL,
    icu_patients_per_million DECIMAL(10,2) NULL,
    hosp_patients INT NULL,
    hosp_patients_per_million DECIMAL(10,2) NULL,
    weekly_icu_admissions INT NULL,
    weekly_icu_admissions_per_million DECIMAL(10,2) NULL,
    weekly_hosp_admissions INT NULL,
    weekly_hosp_admissions_per_million DECIMAL(10,2) NULL
);

-- Create CovidVaccinations table with ALL columns allowing NULL
CREATE TABLE IF NOT EXISTS covid_vaccinations (
    iso_code VARCHAR(10) NULL,
    continent VARCHAR(50) NULL,
    location VARCHAR(100) NULL,
    date DATE NULL,
    new_tests INT NULL,
    total_tests BIGINT NULL,
    total_tests_per_thousand DECIMAL(10,2) NULL,
    new_tests_per_thousand DECIMAL(10,2) NULL,
    new_tests_smoothed INT NULL,
    new_tests_smoothed_per_thousand DECIMAL(10,2) NULL,
    positive_rate DECIMAL(5,2) NULL,
    tests_per_case DECIMAL(10,2) NULL,
    tests_units VARCHAR(50) NULL,
    total_vaccinations BIGINT NULL,
    people_vaccinated BIGINT NULL,
    people_fully_vaccinated BIGINT NULL,
    new_vaccinations INT NULL,
    new_vaccinations_smoothed INT NULL,
    total_vaccinations_per_hundred DECIMAL(5,2) NULL,
    people_vaccinated_per_hundred DECIMAL(5,2) NULL,
    people_fully_vaccinated_per_hundred DECIMAL(5,2) NULL,
    new_vaccinations_smoothed_per_million INT NULL,
    stringency_index DECIMAL(5,2) NULL,
    population_density DECIMAL(10,2) NULL,
    median_age DECIMAL(5,1) NULL,
    aged_65_older DECIMAL(5,2) NULL,
    aged_70_older DECIMAL(5,2) NULL,
    gdp_per_capita DECIMAL(10,2) NULL,
    extreme_poverty DECIMAL(5,2) NULL,
    cardiovasc_death_rate DECIMAL(10,2) NULL,
    diabetes_prevalence DECIMAL(5,2) NULL,
    female_smokers DECIMAL(5,2) NULL,
    male_smokers DECIMAL(5,2) NULL,
    handwashing_facilities DECIMAL(5,2) NULL,
    hospital_beds_per_thousand DECIMAL(5,2) NULL,
    life_expectancy DECIMAL(5,2) NULL,
    human_development_index DECIMAL(5,3) NULL
); 

-- =====================================================
-- Step 3: Load Data from CSV Files
-- =====================================================

-- Data loading of covid deaths 
-- Converts empty strings to NULL and formats dates properly
LOAD DATA LOCAL INFILE 'C:/Users/Apeksha Tank/Desktop/Resume-Project/CovidDeaths.csv'
INTO TABLE covid_deaths
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(iso_code, continent, location, @date, population, 
 @total_cases, @new_cases, @new_cases_smoothed, 
 @total_deaths, @new_deaths, @new_deaths_smoothed, 
 @total_cases_per_million, @new_cases_per_million,
 @new_cases_smoothed_per_million, @total_deaths_per_million, 
 @new_deaths_per_million, @new_deaths_smoothed_per_million, 
 @reproduction_rate, @icu_patients, @icu_patients_per_million,
 @hosp_patients, @hosp_patients_per_million, 
 @weekly_icu_admissions, @weekly_icu_admissions_per_million,
 @weekly_hosp_admissions, @weekly_hosp_admissions_per_million)
SET 
    date = STR_TO_DATE(@date, '%Y-%m-%d'),
    total_cases = NULLIF(@total_cases, ''),
    new_cases = NULLIF(@new_cases, ''),
    new_cases_smoothed = NULLIF(@new_cases_smoothed, ''),
    total_deaths = NULLIF(@total_deaths, ''),
    new_deaths = NULLIF(@new_deaths, ''),
    new_deaths_smoothed = NULLIF(@new_deaths_smoothed, ''),
    total_cases_per_million = NULLIF(@total_cases_per_million, ''),
    new_cases_per_million = NULLIF(@new_cases_per_million, ''),
    new_cases_smoothed_per_million = NULLIF(@new_cases_smoothed_per_million, ''),
    total_deaths_per_million = NULLIF(@total_deaths_per_million, ''),
    new_deaths_per_million = NULLIF(@new_deaths_per_million, ''),
    new_deaths_smoothed_per_million = NULLIF(@new_deaths_smoothed_per_million, ''),
    reproduction_rate = NULLIF(@reproduction_rate, ''),
    icu_patients = NULLIF(@icu_patients, ''),
    icu_patients_per_million = NULLIF(@icu_patients_per_million, ''),
    hosp_patients = NULLIF(@hosp_patients, ''),
    hosp_patients_per_million = NULLIF(@hosp_patients_per_million, ''),
    weekly_icu_admissions = NULLIF(@weekly_icu_admissions, ''),
    weekly_icu_admissions_per_million = NULLIF(@weekly_icu_admissions_per_million, ''),
    weekly_hosp_admissions = NULLIF(@weekly_hosp_admissions, ''),
    weekly_hosp_admissions_per_million = NULLIF(@weekly_hosp_admissions_per_million, '');
    
-- Data loading of covid vaccination     
-- Converts empty strings to NULL and formats dates properly
LOAD DATA LOCAL INFILE 'C:/Users/Apeksha Tank/Desktop/Resume-Project/CovidVaccinations.csv'
INTO TABLE covid_vaccinations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(iso_code, continent, location, @date, 
 @new_tests, @total_tests, @total_tests_per_thousand,
 @new_tests_per_thousand, @new_tests_smoothed, @new_tests_smoothed_per_thousand, 
 @positive_rate, @tests_per_case, tests_units, 
 @total_vaccinations, @people_vaccinated, @people_fully_vaccinated,
 @new_vaccinations, @new_vaccinations_smoothed, 
 @total_vaccinations_per_hundred, @people_vaccinated_per_hundred, 
 @people_fully_vaccinated_per_hundred, @new_vaccinations_smoothed_per_million, 
 @stringency_index, @population_density, @median_age,
 @aged_65_older, @aged_70_older, @gdp_per_capita, @extreme_poverty, 
 @cardiovasc_death_rate, @diabetes_prevalence, @female_smokers, @male_smokers, 
 @handwashing_facilities, @hospital_beds_per_thousand, @life_expectancy, 
 @human_development_index)
SET 
    date = STR_TO_DATE(@date, '%Y-%m-%d'),
    new_tests = NULLIF(@new_tests, ''),
    total_tests = NULLIF(@total_tests, ''),
    total_tests_per_thousand = NULLIF(@total_tests_per_thousand, ''),
    new_tests_per_thousand = NULLIF(@new_tests_per_thousand, ''),
    new_tests_smoothed = NULLIF(@new_tests_smoothed, ''),
    new_tests_smoothed_per_thousand = NULLIF(@new_tests_smoothed_per_thousand, ''),
    positive_rate = NULLIF(@positive_rate, ''),
    tests_per_case = NULLIF(@tests_per_case, ''),
    total_vaccinations = NULLIF(@total_vaccinations, ''),
    people_vaccinated = NULLIF(@people_vaccinated, ''),
    people_fully_vaccinated = NULLIF(@people_fully_vaccinated, ''),
    new_vaccinations = NULLIF(@new_vaccinations, ''),
    new_vaccinations_smoothed = NULLIF(@new_vaccinations_smoothed, ''),
    total_vaccinations_per_hundred = NULLIF(@total_vaccinations_per_hundred, ''),
    people_vaccinated_per_hundred = NULLIF(@people_vaccinated_per_hundred, ''),
    people_fully_vaccinated_per_hundred = NULLIF(@people_fully_vaccinated_per_hundred, ''),
    new_vaccinations_smoothed_per_million = NULLIF(@new_vaccinations_smoothed_per_million, ''),
    stringency_index = NULLIF(@stringency_index, ''),
    population_density = NULLIF(@population_density, ''),
    median_age = NULLIF(@median_age, ''),
    aged_65_older = NULLIF(@aged_65_older, ''),
    aged_70_older = NULLIF(@aged_70_older, ''),
    gdp_per_capita = NULLIF(@gdp_per_capita, ''),
    extreme_poverty = NULLIF(@extreme_poverty, ''),
    cardiovasc_death_rate = NULLIF(@cardiovasc_death_rate, ''),
    diabetes_prevalence = NULLIF(@diabetes_prevalence, ''),
    female_smokers = NULLIF(@female_smokers, ''),
    male_smokers = NULLIF(@male_smokers, ''),
    handwashing_facilities = NULLIF(@handwashing_facilities, ''),
    hospital_beds_per_thousand = NULLIF(@hospital_beds_per_thousand, ''),
    life_expectancy = NULLIF(@life_expectancy, ''),
    human_development_index = NULLIF(@human_development_index, '');

-- =====================================================
-- Step 4: Data Exploration Queries
-- =====================================================

-- View all death data (excluding continent aggregates)
SELECT * FROM covid_deaths
WHERE continent IS NOT NULL 
  AND continent != '' 
ORDER BY 3,4;
    
-- View all vaccination data (excluding continent aggregates)
SELECT * FROM covid_vaccinations
WHERE continent IS NOT NULL 
  AND continent != '' 
ORDER BY 3,4;

-- Select specific columns for analysis
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid_deaths
WHERE continent IS NOT NULL 
  AND continent != '' 
ORDER BY 1,2;

-- =====================================================
-- Analysis: Total Cases vs Total Deaths
-- =====================================================

-- Looking at total cases vs total deaths (global)
SELECT location, date, total_cases, total_deaths, 
       (total_deaths/total_cases)*100 AS DeathPercentage
FROM covid_deaths
WHERE continent IS NOT NULL 
  AND continent != '' 
ORDER BY 1,2;
    
-- For specific country (shows likelihood of dying if you contract covid)
SELECT location, date, total_cases, total_deaths, 
       (total_deaths/total_cases)*100 AS DeathPercentage
FROM covid_deaths
WHERE location LIKE '%indi%'
  AND continent IS NOT NULL 
  AND continent != '' 
ORDER BY 1,2;
    
-- =====================================================
-- Analysis: Total Cases vs Population
-- =====================================================

-- Shows what percentage of population got covid
SELECT location, date, population, total_cases, 
       (total_cases/population)*100 as CasePercentage
FROM covid_deaths
WHERE location LIKE '%indi%'
  AND continent IS NOT NULL 
  AND continent != '' 
ORDER BY 1,2;

-- Countries with highest infection rate compared to population
SELECT location, population, 
       MAX(total_cases) AS HighestInfectionCount, 
       MAX((total_cases/population))*100 as PercentPopulationInfected
FROM covid_deaths
WHERE continent IS NOT NULL 
  AND continent != '' 
GROUP BY location, population
ORDER BY PercentPopulationInfected DESC;
    
-- =====================================================
-- Analysis: Death Counts
-- =====================================================

-- Countries with the highest death count per population     
SELECT location, MAX(total_deaths) as TotalDeathCount
FROM covid_deaths
WHERE continent IS NOT NULL 
  AND continent != '' 
GROUP BY location
ORDER BY TotalDeathCount DESC;

-- Note: If total_deaths is not integer, use CAST
-- Syntax: MAX(CAST(total_deaths AS UNSIGNED))

-- =====================================================
-- Analysis: Continent Level
-- =====================================================

-- Continent with highest death count per population
SELECT continent, MAX(total_deaths) as TotalContinentDeathCount
FROM covid_deaths
WHERE continent IS NOT NULL 
  AND continent != '' 
GROUP BY continent
ORDER BY TotalContinentDeathCount DESC;

-- Overall death percentage for EACH continent
SELECT 
    continent, 
    MAX(total_cases) as TotalCases,
    MAX(total_deaths) as TotalDeaths,
    (MAX(total_deaths) / MAX(total_cases)) * 100 as ContinentDeathPercentage
FROM covid_deaths
WHERE continent IS NOT NULL 
    AND continent != ''
GROUP BY continent
ORDER BY ContinentDeathPercentage DESC;

-- =====================================================
-- Analysis: Global Numbers (Daily)
-- =====================================================

-- Global death percentage for EACH day
SELECT 
    date as DD, 
    MAX(total_cases) as totalcases, 
    MAX(total_deaths) as totaldeaths,
    (MAX(total_deaths)/MAX(total_cases))*100 as DeathPercentage
FROM covid_deaths
WHERE continent IS NOT NULL AND continent != ''
GROUP BY DD
ORDER BY DeathPercentage DESC;

-- New cases based on each day
SELECT date, SUM(new_cases) 
FROM covid_deaths
WHERE continent IS NOT NULL AND continent != ''
GROUP BY date
ORDER BY 1,2;

-- New cases and new deaths based on each day
SELECT date, SUM(new_cases), SUM(new_deaths) 
FROM covid_deaths
WHERE continent IS NOT NULL AND continent != ''
GROUP BY date
ORDER BY 1,2;

-- New cases vs new deaths percentage by day
SELECT date, 
       SUM(new_cases) as New_Total_Cases, 
       SUM(new_deaths) as New_Total_Deaths, 
       (SUM(new_deaths)/SUM(new_cases))*100 AS NewDeathPercentage
FROM covid_deaths
WHERE continent IS NOT NULL AND continent != ''
GROUP BY date
ORDER BY 1,2;

-- =====================================================
-- Analysis: Global Totals (All time)
-- =====================================================

-- Entire WORLD's total numbers (all countries combined)
SELECT 
    SUM(new_cases) as New_Total_Cases, 
    SUM(new_deaths) as New_Total_Deaths, 
    (SUM(new_deaths)/SUM(new_cases))*100 AS NewDeathPercentage
FROM covid_deaths
WHERE continent IS NOT NULL AND continent != '';

-- =====================================================
-- Step 5: Joins - Population vs Vaccinations
-- =====================================================

-- View both tables
SELECT * FROM covid_vaccinations;
SELECT * FROM covid_deaths;

-- Joining two tables (INNER JOIN)
SELECT * 
FROM covid_deaths cd
JOIN covid_vaccinations cv 
    ON cd.location = cv.location 
    AND cd.date = cv.date;

-- Looking at total population vs vaccinations
-- Shows when each country started vaccinating
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations
FROM covid_deaths cd
JOIN covid_vaccinations cv 
    ON cd.location = cv.location 
    AND cd.date = cv.date
WHERE cd.continent IS NOT NULL 
    AND cd.continent != ''
ORDER BY 2,3;

-- Vaccination rate on each day per country
SELECT 
    cd.continent, 
    cd.location, 
    cd.date,
    cd.population, 
    cv.total_vaccinations,
    (cv.total_vaccinations / cd.population) * 100 as VaccinationRate
FROM covid_deaths cd
JOIN covid_vaccinations cv 
    ON cd.location = cv.location 
    AND cd.date = cv.date
WHERE cd.continent IS NOT NULL 
    AND cd.continent != ''
ORDER BY cd.location, cd.date;

-- How vaccination numbers accumulate over time for each country
SELECT 
    cd.continent, 
    cd.location, 
    cd.date, 
    cd.population, 
    cv.new_vaccinations,
    SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS RollingPeopleVaccinated
FROM covid_deaths cd
JOIN covid_vaccinations cv 
    ON cd.location = cv.location 
    AND cd.date = cv.date
WHERE cd.continent IS NOT NULL 
    AND cd.continent != ''
ORDER BY 2,3;

-- =====================================================
-- Step 6: Advanced Analysis - CTE, Temp Tables, Views
-- =====================================================

-- OPTION 1: Using CTE (Common Table Expression)
-- CTE is a temporary named query that you can reference later
WITH PopvsVsc (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
AS 
(
    SELECT 
        cd.continent, 
        cd.location, 
        cd.date, 
        cd.population, 
        cv.new_vaccinations,
        SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS RollingPeopleVaccinated
    FROM covid_deaths cd
    JOIN covid_vaccinations cv 
        ON cd.location = cv.location AND cd.date = cv.date
    WHERE cd.continent IS NOT NULL AND cd.continent != ''
)
SELECT *, (RollingPeopleVaccinated/Population)*100 AS PercentVaccinated
FROM PopvsVsc
ORDER BY Location, Date;

-- OPTION 2: Using Temporary Table
-- Temp table exists temporarily in the database for the duration of your session
DROP TABLE IF EXISTS PercentPopulationVaccinated;

CREATE TEMPORARY TABLE PercentPopulationVaccinated
(
    Continent VARCHAR(255),
    Location VARCHAR(255),
    Date DATE,
    Population BIGINT,
    New_Vaccinations BIGINT,
    RollingPeopleVaccinated BIGINT
);

INSERT INTO PercentPopulationVaccinated
SELECT 
    cd.continent, 
    cd.location, 
    cd.date, 
    cd.population, 
    cv.new_vaccinations,
    SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS RollingPeopleVaccinated
FROM covid_deaths cd
JOIN covid_vaccinations cv 
    ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent IS NOT NULL AND cd.continent != '';

SELECT 
    *, 
    (RollingPeopleVaccinated / Population) * 100 AS PercentVaccinated
FROM PercentPopulationVaccinated
ORDER BY Location, Date;

-- OPTION 3: Creating VIEW for later visualizations
-- A VIEW is a virtual table that stores a saved SQL query
CREATE OR REPLACE VIEW PercentPopulationVaccinated AS 
SELECT 
    cd.continent, 
    cd.location, 
    cd.date, 
    cd.population, 
    cv.new_vaccinations,
    SUM(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.date) AS RollingPeopleVaccinated
FROM covid_deaths cd
JOIN covid_vaccinations cv 
    ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent IS NOT NULL AND cd.continent != '';

-- Query the view
SELECT * FROM PercentPopulationVaccinated;