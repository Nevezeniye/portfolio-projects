select * from layoffs;
CREATE TABLE layoffs_staging
like layoffs;
insert layoffs_staging
select * from layoffs_staging;

with cte as
(SELECT *, 
row_number() over(partition by  company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
SELECT * from cte 
wHERE row_num>1;

SELECT * from layoffs_staging
wHERE company='Casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
SELECT *, 
row_number() over(partition by  company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete from layoffs_staging2
wHERE row_num>1;

SElect company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

SELECT distinct industry 
from layoffs_staging2;

update layoffs_staging2
set country='United States'
where country like 'United States%';

SELECT distinct country
from layoffs_staging2
order by 1;

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;
update layoffs_staging2
set `date`=str_to_date(`date`,'%m/%d/%Y');

ALter TAble layoffs_staging2
modify column `date` DATe;

SELECT * from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
    and t1.location=t2.location
WHere (t1.industry='' or t1.industry is null )
AND t2.industry is not null;

update layoffs_staging2
set industry=null
where industry='';

update layoffs_staging2 as t1
join layoffs_staging2  as t2
	on t1.company=t2.company
set t1.industry= t2.industry
WHere ( t1.industry is null )
AND t2.industry is not null;

SELECT * from layoffs_staging2 where company='Airbnb';

DELETe 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
alter table layoffs_staging2
drop column row_num;
SELECT * from layoffs_staging2;