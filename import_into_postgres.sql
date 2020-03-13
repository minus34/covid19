
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
       null::integer as population,
       null::float as confirmed_percent,
       null::float as deaths_percent,
       null::float as recovered_percent,
       null::float as active_percent,
       null::smallint as population_year,
       avg(latitude)::numeric(8,6) as latitude,
	   avg(longitude)::numeric(9,6) as longitude,
	   ST_SetSRID(ST_Makepoint(avg(longitude), avg(latitude)), 4326) as geom
from merge
group by country_region
;

ALTER TABLE covid19.countries ADD CONSTRAINT countries_pkey PRIMARY KEY (country_region);
CREATE INDEX countries_geom_idx ON covid19.countries USING gist (geom);
ALTER TABLE covid19.countries CLUSTER ON countries_geom_idx;

-- update country names to match World Bank names
update covid19.countries set country_region = 'Brunei Darussalam' where country_region = 'Brunei';
update covid19.countries set country_region = 'Congo, Dem. Rep.' where country_region = 'Congo (Kinshasa)';
update covid19.countries set country_region = 'Korea, Rep.' where country_region = 'Korea, South';
update covid19.countries set country_region = 'United States' where country_region = 'US';
update covid19.countries set country_region = 'Iran, Islamic Rep.' where country_region = 'Iran';
update covid19.countries set country_region = 'Egypt, Arab Rep.' where country_region = 'Egypt';
update covid19.countries set country_region = 'Russian Federation' where country_region = 'Russia';
update covid19.countries set country_region = 'Czech Republic' where country_region = 'Czechia';
update covid19.countries set country_region = 'Slovak Republic' where country_region = 'Slovakia';

-- manually set populations
update covid19.countries set population = 859959, population_year = 2020 where country_region = 'Reunion';
update covid19.countries set population = 376480, population_year = 2016 where country_region = 'Martinique';
update covid19.countries set population = 23780000, population_year = 2018 where country_region = 'Taiwan*';
update covid19.countries set population = 290691, population_year = 2020 where country_region = 'French Guiana';
update covid19.countries set population = 1000, population_year = 2017 where country_region = 'Holy See';

-- fix coords and geoms of countries with territories that skew their centroid
update covid19.countries
    set latitude = 47.2,
        longitude = 3.0,
        geom = ST_SetSRID(ST_Makepoint(3.0, 47.2), 4326)
where country_region = 'France';

update covid19.countries
    set latitude = 54.0,
        longitude = -2.0,
        geom = ST_SetSRID(ST_Makepoint(-2.0, 54.0), 4326)
where country_region = 'United Kingdom';

update covid19.countries
    set latitude = 56.0,
        longitude = 9.3,
        geom = ST_SetSRID(ST_Makepoint(9.3, 56.0), 4326)
where country_region = 'Denmark';


delete from covid19.countries where country_region = 'Cruise Ship';

analyse covid19.countries;


--select * from covid19.countries
--order by country_region;


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
FROM '/Users/hugh.saalmans/git/minus34/covid19/world-bank-population-reformatted.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

delete from covid19.world_population where population is null;

ALTER TABLE covid19.world_population ADD CONSTRAINT world_population_pkey PRIMARY KEY (country_name, year);

analyse covid19.world_population;
-- vacuum analyse covid19.world_population;

--select * from covid19.world_population
--order by population desc;


-- get population and normalised infection rates
with latest as (
    select country_name,
           max(year) as max_year
    from covid19.world_population
    group by country_name
), pop as (
	select wb.*
	from covid19.world_population as wb
	inner join latest on wb.country_name = latest.country_name
		and wb.year = latest.max_year
)
update covid19.countries as co
	set population = pop.population,
	    population_year = pop.year,
	    confirmed_percent = co.confirmed::float / pop.population:: float * 100.0,
	    deaths_percent = co.deaths::float / pop.population:: float * 100.0,
	    recovered_percent = co.recovered::float / pop.population:: float * 100.0,
	    active_percent = co.active::float / pop.population:: float * 100.0
from pop
where co.country_region = pop.country_name
;

--select * from covid19.countries where population is null;
