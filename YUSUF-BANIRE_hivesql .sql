-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/clinicaltrial')
-- MAGIC dbutils.fs.cp('FileStore/tables/clinicaltrial_2021.csv', 'FileStore/tables/clinicaltrial')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.ls('FileStore/tables/clinicaltrial')

-- COMMAND ----------

drop TABLE IF EXISTS clinical_2021;
CREATE EXTERNAL TABLE IF NOT EXISTS clinical_2021(
id string,
Status string,
Start string,
sponsor string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY "|"
LOCATION 'dbfs:/FileStore/tables/clinicaltrial';

-- COMMAND ----------

-----LOAD DATA INPATH '/FileStore/tables/clinical_2021' OVERWRITE INTO TABLE clinical_2021

-- COMMAND ----------

select *
from clinical_2021;

-- COMMAND ----------

drop view if exists clinicaldata  ;
create view  if  not exists clinicaldata as
select *
from clinical_2021
where id <> 'Id';

-- COMMAND ----------

select *
from clinicaldata;

-- COMMAND ----------

SELECT DISTINCT COUNT(Type) FROM clinicaldata where Type <> 'Type';

-- COMMAND ----------

select Type, count(Type)
from clinicaldata
where Type <> 'Type'
group by Type
order by count (Type) desc;


-- COMMAND ----------

SELECT
  Disease,count(*)
FROM (select explode(split(conditions,','))AS Disease
 FROM clinicaldata)
WHERE Disease != ''
GROUP BY Disease
ORDER BY count(*) DESC 
LIMIT 5;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/meshfile')
-- MAGIC dbutils.fs.cp('FileStore/tables/mesh.csv', 'FileStore/tables/meshfile')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.ls('FileStore/tables/meshfile')

-- COMMAND ----------

DROP TABLE IF EXISTS mesh2021;
CREATE EXTERNAL TABLE  IF NOT EXISTS mesh2021(
term string,
tree string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION '/FileStore/tables/meshfile';

-- COMMAND ----------

select *
from mesh2021;

-- COMMAND ----------

drop view if exists meshfile;
create view  if not exists meshfile as
select *
from mesh2021
where term <> 'term';

-- COMMAND ----------

select *
from meshfile

-- COMMAND ----------

------LOAD DATA INPATH 'dbfs:/FileStore/tables/mesh' OVERWRITE INTO TABLE meshfile;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs('FileStore/tables/pharmafile')
-- MAGIC dbutils.fs.cp('FileStore/tables/pharma.csv', 'FileStore/tables/pharmafile')

-- COMMAND ----------

DROP TABLE IF EXISTS pharma2021;
CREATE EXTERNAL TABLE IF NOT EXISTS pharma2021(
Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_eliminaTING_Multiple_Counting string,
Penalty_Year string,
Penalty_Date string,
Offense_Group string,
Primary_offense string,
Secondary_Offense string,
Description string,
Level_of_Government string,
Action_Type string,
Agency string,
Civil_Criminal string,
Prosecution_Agreement string,
Court string,
Case_ID string,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string,
City string,
Address string,
Zip string,
NAICS_Code string,
NAICS_Translation string,
HQ_Country_of_Parent string,
HQ_State_of_Parent string,
Ownership_Structure string,
Parent_Company_Stock_Ticker string,
Major_Industry_of_Parent string,
Specific_Industry_of_Parent string,
Info_Source string,
Notes string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION '/FileStore/tables/pharmafile'

-- COMMAND ----------

-----LOAD DATA INPATH '/FileStore/mypharm.csv' OVERWRITE INTO TABLE pharm;

-- COMMAND ----------

select *
from pharma2021;

-- COMMAND ----------

drop view if exists  pharmafile;
create view  if not exists pharmafile as
select *
from pharma2021
where Company <> '"Company"';

-- COMMAND ----------

select *
from pharmafile

-- COMMAND ----------


SELECT REPLACE (substring(tree,1,3),'"',''),count(Disease)
FROM meshfile
INNER JOIN (select explode (split(conditions,','))AS Disease 
FROM clinicaldata)
ON term = disease 
WHERE Disease != ''
GROUP BY REPLACE (substring(tree,1,3),'"','')
ORDER BY count(disease) DESC -- ordering by the frequency column in descending order
LIMIT -- top 5 rows
  5;

-- COMMAND ----------

SELECT Status, count(id)
FROM clinicaldata
LEFT JOIN pharmafile
ON REPLACE (parent_Company,'"',"") = Status
WHERE regexp_replace(parent_Company,'""',"")IS NULL
GROUP BY Status
ORDER BY count(ID) DESC 
LIMIT 10

-- COMMAND ----------

select Month, count from(
select case WHEN Month ='Jan' then 1 WHEN Month ='Feb' then 2 WHEN Month ='Mar' then 3 WHEN Month ='Apr' then 4
WHEN Month ='May' then 5
WHEN Month ='Jun' then 6
WHEN Month ='Jul' then 7
WHEN Month ='Aug' then 8
WHEN Month ='Sep' then 9
WHEN Month ='Oct' then 10
WHEN Month ='Nov' then 11
WHEN Month ='Dec' then 12 
END as order_month, Month, Count from(
SELECT SUBSTRING(Completion, 1,3) as Month, count (SUBSTRING(Completion, 1,3)) as Count
FROM clinicaldata
WHERE start = "Completed"  AND SUBSTRING(Completion,5,4)= 2021
GROUP BY SUBSTRING (Completion,1,3)
)
order by order_month asc
)


-- COMMAND ----------

SELECT *
FROM clinicaldata
inner JOIN pharmafile
ON REPLACE (parent_Company,'"',"") = Status
WHERE regexp_replace(parent_Company,'""',"")IS NULL 
LIMIT 10

-- COMMAND ----------

select parent_Company,City
from pharma2021
where City <> '""'

-- COMMAND ----------


