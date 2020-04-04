
-- CREATE SCHEMA covid19;
-- CREATE EXTENSION postgis;

-- load JHU COVID-19 case data

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
FROM '/Users/hugh.saalmans/git/minus34/covid19/global/output_files/time_series_19-covid-reformatted.csv' WITH (HEADER, DELIMITER ',', FORMAT CSV);

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
), active as (
    select *,
           confirmed - deaths - recovered as active,
           (confirmed - deaths - recovered) - lag(confirmed - deaths - recovered) over (partition by province_state, country_region order by the_date) as daily_change,
           (confirmed - deaths - recovered) - lag(confirmed - deaths - recovered, 7) over (partition by province_state, country_region order by the_date) as weekly_change
    from merge
)
select *,
       (case when lag(active) over (partition by province_state, country_region order by the_date) > 0 then daily_change::float /
           (lag(active) over (partition by province_state, country_region order by the_date))::float * 100.0 end)::integer as daily_change_percent,
       (case when lag(active, 7) over (partition by province_state, country_region order by the_date) > 0 then weekly_change::float /
           (lag(active, 7) over (partition by province_state, country_region order by the_date))::float * 100.0 end)::integer as weekly_change_percent,
       ST_SetSRID(ST_Makepoint(longitude, latitude), 4326) as geom
from active
order by the_date desc;

ANALYSE covid19.cases;

--select * from covid19.cases
--where country_region = 'China'
--order by province_state, the_date
--;


-- load world population figures by country (source: World Bank - https://data.worldbank.org/indicator/SP.POP.TOTL)

DROP TABLE IF EXISTS covid19.world_population;
CREATE TABLE covid19.world_population
(
    country_name text,
	country_code text,
	year smallint,
	population bigint
) WITH (OIDS = FALSE);
ALTER TABLE covid19.world_population OWNER to postgres;

COPY covid19.world_population
FROM '/Users/hugh.saalmans/git/minus34/covid19/global/output_files/un-population-reformatted.csv'
--FROM '/Users/hugh.saalmans/git/minus34/covid19/global/output_files/world-bank-population-reformatted.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

delete from covid19.world_population where population is null; -- get rid of nulls, will cause issues down the line

ALTER TABLE covid19.world_population ADD CONSTRAINT world_population_pkey PRIMARY KEY (country_name);

analyse covid19.world_population;
-- vacuum analyse covid19.world_population;

--select * from covid19.world_population
--order by population desc;
