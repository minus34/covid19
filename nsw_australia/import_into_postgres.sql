
-- CREATE SCHEMA covid19;
-- CREATE EXTENSION postgis;

DROP TABLE IF EXISTS covid19.raw_nsw_cases;
CREATE TABLE covid19.raw_nsw_cases
(
    notification_date date,
    postcode character varying(4),
    lhd_2010_code text,
    lhd_2010_name text,
    lga_code19 text,
    lga_name19 text
) WITH (OIDS = FALSE);
ALTER TABLE covid19.raw_nsw_cases OWNER to postgres;

COPY covid19.raw_nsw_cases
FROM '/Users/hugh.saalmans/git/minus34/covid19/nsw_australia/input_files/nsw-covid19-data.csv' WITH (HEADER, DELIMITER ',', FORMAT CSV);

delete from covid19.raw_nsw_cases where postcode is null;

ANALYSE covid19.raw_nsw_cases;


-- group postcodes by day to get case counts
DROP TABLE IF EXISTS covid19.nsw_cases;
CREATE TABLE covid19.nsw_cases AS
SELECT max(notification_date) AS notification_date,
       postcode,
       lhd_2010_code,
       lhd_2010_name,
       lga_code19,
       lga_name19,
       count(*) as cases
--       null::integer as pop_2016,
--       null::geometry(multipolygon, 4283) as geom
FROM covid19.raw_nsw_cases
GROUP BY postcode,
         lhd_2010_code,
         lhd_2010_name,
         lga_code19,
         lga_name19;
ALTER TABLE covid19.nsw_cases OWNER to postgres;

ANALYSE covid19.nsw_cases;

ALTER TABLE covid19.nsw_cases ADD CONSTRAINT nsw_cases_pkey PRIMARY KEY (notification_date, postcode);



-- -- create  table of unique address points, based on population (source table built using gnaf-loader code)
--DROP TABLE IF EXISTS covid19.nsw_points;
--CREATE TABLE covid19.nsw_points AS
--select postcode,
--       count(*) as population,
--       geom
--from testing.address_principals_persons
--where left(mb_2016_code::text, 1) = '1'
--group by postcode,
--         geom;
--
--create index nsw_points_postcode_idx on covid19.nsw_points using btree (postcode);



-- get one person point per case
DROP TABLE IF EXISTS covid19.nsw_cases_points;
CREATE TABLE covid19.nsw_cases_points AS
WITH row_nums as (
    SELECT *, row_number() OVER (PARTITION BY postcode ORDER BY random()) as row_num
    FROM covid19.nsw_points
)
SELECT nsw.*,
       row_nums.geom
from covid19.nsw_cases as nsw
inner join row_nums on nsw.postcode = row_nums.postcode
WHERE row_num <= nsw.cases
;

ANALYSE covid19.nsw_cases_points;



-- get postcode bdy with population and % of cases
DROP TABLE IF EXISTS covid19.nsw_cases_postcodes;
CREATE TABLE covid19.nsw_cases_postcodes AS
WITH pc as (
    SELECT sum(population) as population,
           postcode
    FROM covid19.nsw_points
    GROUP BY postcode
)
SELECT nsw.notification_date,
       nsw.postcode,
       nsw.cases,
       pc.population,
       (nsw.cases::float / pc.population::float * 100.0)::numeric(5, 2) as percent_infected,
       bdys.geom
from covid19.nsw_cases as nsw
inner join pc on nsw.postcode = pc.postcode
inner join admin_bdys_201911.postcode_bdys_display as bdys on pc.postcode = bdys.postcode
;

ANALYSE covid19.nsw_cases_postcodes;


--select * from covid19.nsw_cases_postcodes order by cases desc;

--select *
--from covid19.nsw_cases as nsw
--inner join testing.address_principals_persons as pop on nsw.postcode = pop.postcode;

