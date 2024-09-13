# DATA CLEANING

SELECT *
FROM layoffs
;

# 1. Remove Duplicates
-- 2. Standardize Data
# 3. Null Values or Blank Values
-- 4. Remove Any Columns or Row

CREATE TABLE layoffs_staging
LIKE layoffs
;

SELECT *
FROM layoffs_staging
;

INSERT layoffs_staging
SELECT *
FROM layoffs
;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper'
;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;

DELETE
FROM layoffs_staging2
WHERE row_num > 1
;
SELECT *
FROM layoffs_staging2
WHERE row_num > 1
;
# DUPLICATES DELETED
SELECT *
FROM layoffs_staging2
;

# STANDARDIZING DATA

SELECT company, TRIM(company)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)
;

# (whitespace removed, trim applied)

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

# INDUSTRIES WHERE CRYPTO IS 'CRYPTOCURRENCY OR CRYPTO CURRENCY CHANGED TO SIMPLY CRYPTO

SELECT DISTINCT industry
FROM layoffs_staging2
;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1
;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

# PERIOD REMOVED FROM UNITED STATES WHEREAS ALL OTHER UNITED STATES HAD NO PERIOD

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

SELECT `date`
FROM layoffs_staging2
;

# TAKING IN THE (DATE) FORMAT AND CONVERTING INTO THE STANDARD DATE FORMAT

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE
;

SELECT *
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT DISTINCT industry
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
;

# POPULATING DATA WHERE A BLANK SECTOR OF DATA CAN BE POPULATED WITH VALID DATA

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

# SHOWING INDUSTRIES WHICH ARE BLANK OR NULL BUT THERE IS A SECOND (SAME) COMPANY 
# WHICH DOES NOT HAVE BLANK OR NULL INDUSTRY, MEANING IT CAN BE POPULATED

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

# POPULATING BLANK OR NULL DATA

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

# BLANK/NULL INDUSTRY DATA POPULATED

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%' 
;

# BALLY ONLY HAS ONE ROW, SO THERE IS NO ROW TO POPULATE BALLY'S NULL/BLANK ROW

SELECT *
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

# NO TOTAL LAID OFF AND NO PERCENTAGE LAID OFF MAKES THE DATA LIKELY 'USELESS'

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

# DATA REMOVED

SELECT *
FROM layoffs_staging2;

# ROW_NUM NO LONGER IS NECESSARY SO WILL NOW BE REMOVED

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
;

# ROW_NUM DELETED