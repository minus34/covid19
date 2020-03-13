
-- load COVID-19 case data

DROP TABLE IF EXISTS covid19.raw_cases;
CREATE TABLE covid19.raw_cases
(
    status text,
	province_state text,
	country_region text,
	latitude numeric(8,6),
	longitude numeric(9,6),
	the_date date,
	persons integer
) WITH (OIDS = FALSE);
ALTER TABLE covid19.raw_cases OWNER to postgres;

COPY covid19.raw_cases (status, province_state, country_region, latitude, longitude, the_date, persons)
FROM '/Users/hugh.saalmans/git/minus34/covid19/time_series_19-covid-reformatted.csv' WITH (HEADER, DELIMITER ',', FORMAT CSV);

ANALYSE covid19.raw_cases;

-- flip data to have values in status columns
DROP TABLE IF EXISTS covid19.cases;
CREATE TABLE covid19.cases AS
with confirmed as (
	select province_state,
		   country_region,
		   the_date
	from covid19.raw_cases
	where status = 'confirmed'
	and persons > 0
), merge as (
	select cases.province_state,
		   cases.country_region,
		   cases.the_date,
		   latitude,
		   longitude,
		   sum(case when status = 'confirmed' then persons else 0 end) as confirmed,
		   sum(case when status = 'deaths' then persons else 0 end) as deaths,
		   sum(case when status = 'recovered' then persons else 0 end )as recovered
	from covid19.raw_cases as cases
	inner join confirmed on cases.province_state IS NOT DISTINCT FROM confirmed.province_state  -- handle NULLS in join
		and cases.country_region = confirmed.country_region
	    and cases.the_date = confirmed.the_date
	group by cases.province_state,
			 cases.country_region,
			 cases.the_date,
			 latitude,
			 longitude
)
select *,
       confirmed - deaths - recovered as active,
	   ST_SetSRID(ST_Makepoint(longitude, latitude), 4326) as geom
from merge
-- where province_state = 'King County, WA'
order by the_date desc;



--select * from covid19.cases where province_state = 'King County, WA' order by the_date desc;


DROP TABLE IF EXISTS covid19.countries;
CREATE TABLE covid19.countries AS
WITH latest as (
    select province_state,
		   country_region,
		   max(the_date) as max_date
    from covid19.cases
	group by province_state,
		     country_region
), merge as (
    select cases.*
    from covid19.cases
    inner join latest on cases.province_state IS NOT DISTINCT FROM latest.province_state  -- handle NULLS in join
            and cases.country_region = latest.country_region
            and cases.the_date = latest.max_date
)
select country_region,
       min(the_date) as min_date,
       max(the_date) as max_date,
       sum(confirmed) as confirmed,
       sum(deaths) as deaths,
       sum(recovered) as recovered,
       sum(active) as active,
	   avg(latitude)::numeric(8,6) as latitude,
	   avg(longitude)::numeric(9,6) as longitude,
	   ST_SetSRID(ST_Makepoint(avg(longitude), avg(latitude)), 4326) as geom
from merge
group by country_region
;


select * from covid19.countries
order by country_region;









-- Index: localities_geom_idx

-- DROP INDEX gnaf_201911.localities_geom_idx;

CREATE INDEX localities_geom_idx
    ON gnaf_201911.localities USING gist
    (geom)
    TABLESPACE pg_default;

ALTER TABLE gnaf_201911.localities
    CLUSTER ON localities_geom_idx;

-- Index: localities_gid_idx

-- DROP INDEX gnaf_201911.localities_gid_idx;

CREATE UNIQUE INDEX localities_gid_idx
    ON gnaf_201911.localities USING btree
    (gid)
    TABLESPACE pg_default;