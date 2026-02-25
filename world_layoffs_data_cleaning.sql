SELECT * FROM world_layoffs.layoffs_sample;

SELECT *
FROM layoffs_sample
WHERE row_num > 1;



-- removing duplicate

SELECT * FROM world_layoffs.layoffs_sample;

select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) row_num
from layoffs_sample;

WITH ranked_layoffs AS (
  SELECT *, 
         ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
  FROM layoffs_sample
)
SELECT *
FROM ranked_layoffs
WHERE row_num = 1;

create table layoffs_cleaned1
select *
from  (
  SELECT *, 
         ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
  FROM layoffs_sample
) as ranked_layoffs
where row_num = 1;

drop table layoffs_sample;
rename table layoffs_cleaned1 to layoffs_sample;

-- triming raw data

UPDATE layoffs_sample
SET
  company = trim(company),
  location = TRIM(location),
  industry = TRIM(industry),
  country = TRIM(country)
WHERE
    company!= trim(company)
  or location != TRIM(location)
  OR industry != TRIM(industry)
  OR country != TRIM(country);
  
  select * from
  layoffs_sample
  where company!= trim(company)
  or location != TRIM(location)
  OR industry != TRIM(industry)
  OR country != TRIM(country);
  
  -- standardise data
  
select distinct industry
from layoffs_sample
order by 1;

select distinct industry
from layoffs_sample
where industry like 'crypto%';

update layoffs_sample
set industry = 'crypto'
where industry like 'crypto%';

select distinct location
from layoffs_sample
order by 1;

select distinct country
from layoffs_sample
order by 1;

update layoffs_sample
set country = 'United states'
where country like 'united states%';

-- date formating

select `date`,
str_to_date(`date`,'%m/%d/%y')
from layoffs_sample;

update layoffs_sample
set date = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_sample
modify column `date` date;

-- replacing nulls and blanks with common data or matching data

update layoffs_sample
set industry = null
where industry = ''

select *
from layoffs_sample
where industry is null

select *
from layoffs_sample
where company ='Airbnb';

select t1.industry,t2.industry
from layoffs_sample t1
join layoffs_sample t2
on t1.company=t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_sample t1
join layoffs_sample t2
on t1.company=t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- deleting nulls and blanks

select *
from layoffs_sample
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_sample
where total_laid_off is null
and percentage_laid_off is null;


alter table layoffs_sample
drop column row_num;

select *
from layoffs_sample;





  



