-- Data Cleaning 
select * from Layoffs;

--1. Remove Duplicates
--2. Standardize the data 
--3. Null Values or blank values
--4. remove any columns & rows 

-- Commit: Create the staging table and insert data from Layoffs
CREATE TABLE layoffs_staging
(LIKE Layoffs INCLUDING ALL);

INSERT INTO layoffs_staging
SELECT * FROM Layoffs;

-- Commit: Review the data in the staging table
SELECT * FROM layoffs_staging;

-- Commit: Rename the staging table for further processing
ALTER TABLE layoffs_staging
RENAME TO layoffs_staging1;

-- Commit: Add row numbers for identifying duplicates
SELECT *, 
       ROW_NUMBER() OVER (ORDER BY company, location, industry, total_laid_off, 
                          percentage_laid_off, date, stage, country, 
                          funds_raised_millions) AS row_num
FROM layoffs_staging1;

-- Commit: Identify and delete duplicates
WITH duplicate_cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (ORDER BY company, location, industry, total_laid_off, 
                              percentage_laid_off, date, stage, country, 
                              funds_raised_millions) AS row_num
    FROM layoffs_staging1
)
-- Delete duplicate records where row number is greater than 1
DELETE FROM layoffs_staging1
WHERE ctid IN (
    SELECT ctid 
    FROM duplicate_cte 
    WHERE row_num > 1
);

-- Commit: Create a new staging table for cleaned data
CREATE TABLE layoffs_staging2 (
  company text,
  location text,
  industry text,
  total_laid_off text,
  percentage_laid_off text,
  date text,
  stage text,
  country text,
  funds_raised_millions text,
  row_num text
);

-- Commit: Insert data into the new staging table with row numbers
INSERT INTO layoffs_staging2
SELECT *, 
       ROW_NUMBER() OVER (ORDER BY company, location, industry, total_laid_off, 
                          percentage_laid_off, date, stage, country, 
                          funds_raised_millions) AS row_num
FROM layoffs_staging1;

-- Commit: Check for duplicates in the new staging table
SELECT * 
FROM layoffs_staging2
WHERE CAST(row_num AS integer) > 1;

-- Commit: Standardize data in the company column
SELECT company, TRIM(company) FROM layoffs_staging2;

UPDATE layoffs_staging2 SET company = TRIM(company);

-- Commit: Check distinct industries before standardization
SELECT DISTINCT industry FROM layoffs_staging2 ORDER BY 1;

-- Commit: Standardize industry values starting with 'Crypto'
SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Commit: Review distinct countries for standardization
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1;

-- Commit: Standardize country names by trimming spaces
SELECT DISTINCT country, TRIM(country) FROM layoffs_staging2
WHERE country LIKE 'United states%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United states%';

-- Commit: Standardize date formats in the staging table
SELECT * FROM layoffs_staging2;

SELECT TO_CHAR(TO_DATE('2/20/2023', 'MM/DD/YYYY'), 'YYYY-MM-DD') AS formatted_date;

UPDATE layoffs_staging2
SET "date" = 
    CASE 
        WHEN "date" ~ '^\d{1,2}/\d{1,2}/\d{4}$'  -- Regex to match MM/DD/YYYY format
        THEN TO_CHAR(TO_DATE("date", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE "date"  -- Keep original value if it's not a valid date
    END;

-- Commit: Change the date column type to DATE after formatting
ALTER TABLE layoffs_staging2
ALTER COLUMN "date" TYPE DATE 
USING 
    CASE 
        WHEN "date" ~ '^\d{4}-\d{2}-\d{2}$'  -- Check for YYYY-MM-DD format
        THEN "date"::DATE  -- Directly cast the already valid date
        WHEN "date" ~ '^\d{1,2}/\d{1,2}/\d{4}$'  -- Check for MM/DD/YYYY format
        THEN TO_DATE("date", 'MM/DD/YYYY')  -- Convert from MM/DD/YYYY to DATE
        ELSE NULL  -- Handle invalid or unrecognized formats
    END;

-- Commit: Identify rows with NULL values in total_laid_off and percentage_laid_off
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Commit: Identify NULL or empty industries
SELECT DISTINCT industry FROM layoffs_staging2;

SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Commit: Check specific company data (e.g., Airbnb)
SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Commit: Update industry values based on company and location
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company
AND (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Commit: Set empty industries to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Commit: Final cleanup and remove unnecessary columns
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Commit: Remove rows where total_laid_off and percentage_laid_off are NULL
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;






-- Create a table 'layoffs_staging2' with specified columns.
CREATE TABLE layoffs_staging2 (
  company text,
  location text,
  industry text,
  total_laid_off text,
  percentage_laid_off text,
  date text,
  stage text,
  country text,
  funds_raised_millions text,
  row_num text
);

-- Insert data into 'layoffs_staging2' while generating a row number for each record.
INSERT INTO layoffs_staging2
SELECT *, 
       ROW_NUMBER() OVER (ORDER BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Select records from 'layoffs_staging2' where the 'row_num' is greater than 1.
SELECT * 
FROM layoffs_staging2
WHERE CAST(row_num AS integer) > 1;

-- Standardize data by trimming whitespace from the 'company' column.
SELECT company, TRIM(company) FROM layoffs_staging2;

-- Update the 'company' column to remove leading and trailing whitespace.
UPDATE layoffs_staging2 
SET company = TRIM(company);

-- Retrieve distinct 'industry' values from 'layoffs_staging2', ordered alphabetically.
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

-- Select all records from 'layoffs_staging2' for verification.
SELECT * FROM layoffs_staging2;

-- Retrieve records where 'industry' starts with 'Crypto'.
SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Update 'industry' to 'Crypto' where it starts with 'Crypto'.
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Retrieve distinct 'country' values from 'layoffs_staging2', ordered alphabetically.
SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

-- Trim the 'country' column and filter records that start with 'United States'.
SELECT DISTINCT country, TRIM(country) 
FROM layoffs_staging2
WHERE country LIKE 'United states%'
ORDER BY 1;

-- Trim the trailing period from 'country' values.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Update 'country' to remove the trailing period where the value starts with 'United States'.
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United states%';

-- Additional checks and updates for the data standardization process.

-- Check for records where 'total_laid_off' and 'percentage_laid_off' are NULL.
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Retrieve distinct 'industry' values from the table.
SELECT DISTINCT industry 
FROM layoffs_staging2;

-- Check for records where 'industry' is NULL or empty.
SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Select all details where the 'company' is 'Airbnb'.
SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Join the table to itself to find missing 'industry' information.
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Update the 'industry' field where it's missing using another row's data.
UPDATE layoffs_staging2 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company
AND (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Reset 'industry' to NULL where it is an empty string.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Delete rows where both 'total_laid_off' and 'percentage_laid_off' are NULL.
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop the 'row_num' column from the 'layoffs_staging2' table.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- changing the date formating 
select * from layoffs_staging2;

SELECT TO_CHAR(TO_DATE('2/20/2023', 'MM/DD/YYYY'), 'YYYY-MM-DD') AS formatted_date;
SELECT 
    CASE 
        WHEN "date" ~ '^\d{1,2}/\d{1,2}/\d{4}$'  -- Regex to match MM/DD/YYYY format
        THEN TO_CHAR(TO_DATE("date", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL  -- Return NULL for invalid values
    END AS formatted_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET "date" = 
    CASE 
        WHEN "date" ~ '^\d{1,2}/\d{1,2}/\d{4}$'  -- Regex to match MM/DD/YYYY format
        THEN TO_CHAR(TO_DATE("date", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE "date"  -- Keep original value if it's not a valid date
    END;

select * FROM layoffs_staging2;

select date from layoffs_staging2;

ALTER TABLE layoffs_staging2
ALTER COLUMN "date" TYPE DATE 
USING 
    CASE 
        WHEN "date" ~ '^\d{4}-\d{2}-\d{2}$'  -- Check for YYYY-MM-DD format
        THEN "date"::DATE  -- Directly cast the already valid date
        WHEN "date" ~ '^\d{1,2}/\d{1,2}/\d{4}$'  -- Check for MM/DD/YYYY format
        THEN TO_DATE("date", 'MM/DD/YYYY')  -- Convert from MM/DD/YYYY to DATE
        ELSE NULL  -- Handle invalid or unrecognized formats
    END;

-- Select all records from 'layoffs_staging2' where both 'total_laid_off' and 'percentage_laid_off' are NULL.
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Select distinct values from the 'industry' column in 'layoffs_staging2'.
SELECT DISTINCT industry FROM layoffs_staging2;

-- Select all records from 'layoffs_staging2' where the 'industry' field is NULL or empty.
SELECT * FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Select all records from 'layoffs_staging2' where the company name is 'Airbnb'.
SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Join 'layoffs_staging2' with itself to find records where 'industry' is NULL or empty
-- but another record with the same company and location has a non-null 'industry'.
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Update the 'industry' column in 'layoffs_staging2' using values from another row in the same table
-- where the 'company' matches and 'industry' is not NULL.
UPDATE layoffs_staging2 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company
AND (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Set the 'industry' to NULL where it is currently an empty string.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';





-- Update the 'industry' column in 'layoffs_staging1' by using corresponding values from 'layoffs_staging2' 
-- where the 'company' matches and 'industry' in 'layoffs_staging1' is NULL.
UPDATE layoffs_staging1 t1
SET industry = t2.industry
FROM layoffs_staging2 t2
WHERE t1.company = t2.company
AND t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Select all records from 'layoffs_staging2' where the company name starts with 'Bally'.
SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- Select all records from 'layoffs_staging2' where both 'total_laid_off' and 'percentage_laid_off' are NULL.
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Select all records from 'layoffs_staging2' to review the current state of the data.
SELECT * FROM layoffs_staging2;

-- Delete records from 'layoffs_staging2' where both 'total_laid_off' and 'percentage_laid_off' are NULL.
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop the 'row_num' column from the 'layoffs_staging2' table as it is no longer needed.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

























