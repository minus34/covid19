
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

ANALYSE covid19.raw_nsw_cases;


-- group postcodes by day to get case counts
DROP TABLE IF EXISTS covid19.nsw_cases;
CREATE TABLE covid19.nsw_cases AS
SELECT notification_date,
       postcode,
       lhd_2010_code,
       lhd_2010_name,
       lga_code19,
       lga_name19,
       count(*) as cases,
       null::integer as pop_2016,
       null::geometry(multipolygon, 4283) as geom
FROM covid19.raw_nsw_cases
GROUP BY notification_date,
         postcode,
         lhd_2010_code,
         lhd_2010_name,
         lga_code19,
         lga_name19;
ALTER TABLE covid19.nsw_cases OWNER to postgres;

delete from covid19.nsw_cases where postcode is null;

ANALYSE covid19.nsw_cases;

ALTER TABLE covid19.nsw_cases ADD CONSTRAINT nsw_cases_pkey PRIMARY KEY (notification_date, postcode);
