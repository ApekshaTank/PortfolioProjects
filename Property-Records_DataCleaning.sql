-- =====================================================
-- NASHVILLE HOUSING DATA ANALYSIS PROJECT
-- Database: NashvilleHousing
-- =====================================================

-- Step 1: Create and Use Database
-- =====================================================
CREATE DATABASE Nashville_Housing;
USE Nashville_Housing;

-- Enable local infile loading for CSV import
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';

-- =====================================================
-- Step 2: Create Table with ALL columns allowing NULL
-- =====================================================

CREATE TABLE IF NOT EXISTS nashville_housing (
    UniqueID INT NULL,
    ParcelID VARCHAR(50) NULL,
    LandUse VARCHAR(100) NULL,
    PropertyAddress VARCHAR(255) NULL,
    SaleDate DATE NULL,
    SalePrice INT NULL,
    LegalReference VARCHAR(100) NULL,
    SoldAsVacant VARCHAR(10) NULL,
    OwnerName VARCHAR(255) NULL,
    OwnerAddress VARCHAR(255) NULL,
    Acreage DECIMAL(10,2) NULL,
    TaxDistrict VARCHAR(100) NULL,
    LandValue INT NULL,
    BuildingValue INT NULL,
    TotalValue INT NULL,
    YearBuilt INT NULL,
    Bedrooms INT NULL,
    FullBath INT NULL,
    HalfBath INT NULL
);

-- =====================================================
-- Step 3: Load Data from CSV File
-- =====================================================

-- Data loading for Nashville Housing data
-- Converts empty strings to NULL and handles missing values
LOAD DATA LOCAL INFILE 'C:/Users/Apeksha Tank/Desktop/Resume-Project/NashvilleHousing.csv'
INTO TABLE nashville_housing
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(UniqueID, ParcelID, LandUse, PropertyAddress, @SaleDate, SalePrice, 
 LegalReference, SoldAsVacant, OwnerName, OwnerAddress, @Acreage, 
 TaxDistrict, LandValue, BuildingValue, TotalValue, @YearBuilt, 
 Bedrooms, FullBath, HalfBath)
SET 
    SaleDate = STR_TO_DATE(@SaleDate, '%Y-%m-%d'),
    Acreage = NULLIF(@Acreage, ''),
    YearBuilt = NULLIF(@YearBuilt, ''),
    -- Handle empty strings for integer fields
    UniqueID = NULLIF(UniqueID, ''),
    SalePrice = NULLIF(SalePrice, ''),
    LandValue = NULLIF(LandValue, ''),
    BuildingValue = NULLIF(BuildingValue, ''),
    TotalValue = NULLIF(TotalValue, ''),
    Bedrooms = NULLIF(Bedrooms, ''),
    FullBath = NULLIF(FullBath, ''),
    HalfBath = NULLIF(HalfBath, ''),
    -- Handle empty strings for text fields (set to NULL)
    ParcelID = NULLIF(ParcelID, ''),
    LandUse = NULLIF(LandUse, ''),
    PropertyAddress = NULLIF(PropertyAddress, ''),
    LegalReference = NULLIF(LegalReference, ''),
    SoldAsVacant = NULLIF(SoldAsVacant, ''),
    OwnerName = NULLIF(OwnerName, ''),
    OwnerAddress = NULLIF(OwnerAddress, ''),
    TaxDistrict = NULLIF(TaxDistrict, '');

-- =====================================================
-- Step 4: Verify Data Import
-- =====================================================

-- Check total row count
SELECT COUNT(*) AS total_records FROM nashville_housing;

-- View first 20 records
SELECT * FROM nashville_housing LIMIT 20;



-- DATA CLEANING STARTS FROM HERE ---
SELECT * FROM nashville_housing;

-- Q. Standardize Date Format

-- Use CAST()
Select SaleDate, CAST(SaleDate AS DATE)
From nashville_housing;

ALTER TABLE nashville_housing 
ADD SaleDateConverted DATE;

UPDATE nashville_housing
SET SaleDateConverted =  CAST(SaleDate AS DATE);

-- Q. Populated Property Address data
SELECT * FROM nashville_housing
-- where PropertyAddress is null
order by ParcelID;

-- using self join
SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, 
	IFNULL(a.PropertyAddress,b.PropertyAddress)
FROM nashville_housing a
JOIN nashville_housing b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;

-- Turn off safe mode
SET SQL_SAFE_UPDATES = 0;

-- Run your update
UPDATE nashville_housing a
JOIN nashville_housing b
    ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = IFNULL(a.PropertyAddress, b.PropertyAddress)
WHERE a.PropertyAddress IS NULL;

-- Turn safe mode back on
SET SQL_SAFE_UPDATES = 1;


-- Q. (A) Breaking out Address into individual columns (address, city, state)
select PropertyAddress
from nashville_housing;

-- we will use substring index
SELECT PropertyAddress,
    SUBSTRING_INDEX(PropertyAddress, ',', 1) AS Address,
    SUBSTRING_INDEX(PropertyAddress, ',', -1) AS City
FROM nashville_housing;

-- Create two columns address and state and update the values in it.

-- Turn off safe mode
SET SQL_SAFE_UPDATES = 0;

ALTER TABLE nashville_housing 
ADD PropertySplitAddress VARCHAR(255);

UPDATE nashville_housing
SET PropertySplitAddress =  SUBSTRING_INDEX(PropertyAddress, ',', 1);

ALTER TABLE nashville_housing 
ADD PropertySplitCity VARCHAR(255);

UPDATE nashville_housing
SET PropertySplitCity =  SUBSTRING_INDEX(PropertyAddress, ',', -1);

-- Turn safe mode back on
SET SQL_SAFE_UPDATES = 1;

select *
from nashville_housing;



-- Q (B) WORKING ON OWNER ADDRESS
SELECT OwnerAddress FROM nashville_housing;


select OwnerAddress,
	TRIM(substring_index(OwnerAddress,',',1)) as ADDRESS,
    TRIM(substring_index(substring_index(OwnerAddress,',',2),',','-1')) as CITY,
    TRIM(substring_index(OwnerAddress,',',-1)) as STATE
from nashville_housing;

-- trime used to remove space
-- without trim function
select OwnerAddress,
	substring_index(OwnerAddress,',',1) as ADDRESS,
    substring_index(substring_index(OwnerAddress,',',2),',','-1') as CITY,
    substring_index(OwnerAddress,',',-1) as STATE
from nashville_housing;


SET SQL_SAFE_UPDATES = 0;
ALTER TABLE nashville_housing
ADD OwnerSplitAddress varchar(255);

UPDATE nashville_housing
SET OwnerSplitAddress = TRIM(substring_index(OwnerAddress,',',1));

ALTER TABLE nashville_housing
ADD OwnerSplitCity VARCHAR(255);

UPDATE nashville_housing
SET OwnerSplitCity = TRIM(substring_index(substring_index(OwnerAddress,',',2),',','-1'));

ALTER TABLE nashville_housing
ADD OwnerSplitState VARCHAR(255);

UPDATE nashville_housing
SET OwnerSplitState = TRIM(substring_index(OwnerAddress,',',-1));

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM nashville_housing;


-- Q. Change Y and N to yes or no in "Sold as vacant" field.
select distinct(SoldAsVacant), count(SoldAsVacant)
from nashville_housing
group by SoldAsVacant
order by 2;


SELECT SoldAsVacant,
	CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
		 WHEN SoldAsVacant = 'N' THEN 'No'
         ELSE SoldAsVacant
         END
FROM nashville_housing;

-- UPDATING 
SET SQL_SAFE_UPDATES = 0;
UPDATE nashville_housing
SET SoldAsVacant = CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
		 WHEN SoldAsVacant = 'N' THEN 'No'
         ELSE SoldAsVacant
         END;
SET SQL_SAFE_UPDATES = 1;

-- Q. Remove Duplicates

-- See what will be deleted
SELECT * FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
            ORDER BY UniqueID
        ) AS row_num
    FROM nashville_housing
) AS preview
WHERE row_num > 1
ORDER BY ParcelID, UniqueID;

-- updated in the main table. (used subquery method)
set SQL_SAFE_UPDATES = 0;

DELETE FROM nashville_housing
WHERE UniqueID IN (
    SELECT UniqueID FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
                ORDER BY UniqueID
            ) AS row_num
        FROM nashville_housing
    ) AS duplicates
    WHERE row_num > 1
);

set SQL_SAFE_UPDATES = 1;

-- Delete Unused Columns
select * from nashville_housing;

ALTER TABLE nashville_housing
DROP COLUMN PropertyAddress,
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict;

ALTER TABLE nashville_housing
DROP COLUMN SaleDateConverted;    -- WE ALREADY HAS THE CORRECT FORMAT IN THE OG TABLE.