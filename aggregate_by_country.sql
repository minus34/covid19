

-- update country names to match World Bank names
update covid19.cases set country_region = 'Brunei Darussalam' where country_region = 'Brunei';
update covid19.cases set country_region = 'Congo, Dem. Rep.' where country_region = 'Congo (Kinshasa)';
update covid19.cases set country_region = 'Congo' where country_region = 'Congo (Brazzaville)';
update covid19.cases set country_region = 'Congo' where country_region = 'Republic of the Congo';
update covid19.cases set country_region = 'South Korea' where country_region = 'Korea, South';
update covid19.cases set country_region = 'United States of America' where country_region = 'US';
update covid19.cases set country_region = 'Iran' where country_region = 'Iran';
update covid19.cases set country_region = 'Egypt' where country_region = 'Egypt';
update covid19.cases set country_region = 'Russian Federation' where country_region = 'Russia';
update covid19.cases set country_region = 'Taiwan' where country_region = 'Taiwan*';
update covid19.cases set country_region = 'State of Palestine' where country_region = 'West Bank and Gaza';


-- aggregate for countries with 100+ cases
DROP TABLE IF EXISTS covid19.countries_100_plus cascade;
CREATE TABLE covid19.countries_100_plus AS
WITH cnty as (
    select country_region,
           the_date,
           sum(confirmed) as confirmed
    from covid19.cases
	group by country_region,
	         the_date
), dte as (
    select country_region,
		   min(the_date) as start_date,
		   max(the_date) as max_date
    from cnty
	where confirmed >= 100
	group by country_region
)
select cases.country_region,
       the_date,
       the_date - dte.start_date as days_since_100_cases,
       dte.start_date,
       dte.max_date,
       sum(confirmed) as confirmed,
       sum(deaths) as deaths,
       sum(recovered) as recovered,
       sum(active) as active,
       avg(latitude)::numeric(8,6) as latitude,
	   avg(longitude)::numeric(9,6) as longitude,
	   ST_SetSRID(ST_Makepoint(avg(longitude), avg(latitude)), 4326) as geom
from covid19.cases
inner join dte on cases.country_region = dte.country_region
  and cases.the_date >= dte.start_date
group by cases.country_region,
         the_date,
		 dte.start_date,
         dte.max_date
;

ALTER TABLE covid19.countries_100_plus ADD CONSTRAINT countries_100_plus_pkey PRIMARY KEY (country_region, the_date);
CREATE INDEX countries_100_plus_geom_idx ON covid19.countries_100_plus USING gist (geom);
ALTER TABLE covid19.countries_100_plus CLUSTER ON countries_100_plus_geom_idx;

ANALYSE covid19.countries_100_plus;


drop view if exists covid19.vw_countries_100_plus;
create view covid19.vw_countries_100_plus as
with aus as (
    select *
    from covid19.countries_100_plus
    where country_region = 'Australia'
)
select cnty.country_region,
       cnty.days_since_100_cases,
       cnty.active - aus.active as diff_to_aus,
       aus.active as aus_active,
       cnty.active
from covid19.countries_100_plus as cnty
inner join aus on cnty.days_since_100_cases = aus.days_since_100_cases
where cnty.country_region <> 'Australia'
order by cnty.country_region,
         cnty.days_since_100_cases;


-- aggregate case data by country and apply manual fixes to cleanup data
DROP TABLE IF EXISTS covid19.countries CASCADE;
CREATE TABLE covid19.countries AS
WITH dte as (
    select country_region,
		   min(the_date) as start_date,
		   max(the_date) as max_date
    from covid19.cases
	where confirmed > 0
	group by country_region
)
select cases.country_region,
       the_date,
       the_date - dte.start_date as days_since_first_case,
       dte.start_date,
       dte.max_date,
       sum(confirmed) as confirmed,
       sum(deaths) as deaths,
       sum(recovered) as recovered,
       sum(active) as active,
       null::integer as population,
       null::float as confirmed_per_100k,
       null::float as deaths_per_100k,
       null::float as recovered_per_100k,
       null::float as active_per_100k,
       null::smallint as population_year,
       avg(latitude)::numeric(8,6) as latitude,
	   avg(longitude)::numeric(9,6) as longitude,
	   ST_SetSRID(ST_Makepoint(avg(longitude), avg(latitude)), 4326) as geom
from covid19.cases
inner join dte on cases.country_region = dte.country_region
group by cases.country_region,
         the_date,
		 dte.start_date,
         dte.max_date
;

ALTER TABLE covid19.countries ADD CONSTRAINT countries_pkey PRIMARY KEY (country_region, the_date);
CREATE INDEX countries_geom_idx ON covid19.countries USING gist (geom);
ALTER TABLE covid19.countries CLUSTER ON countries_geom_idx;

ANALYSE covid19.countries;


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

ANALYSE covid19.countries;


-- get World Bank population and normalised infection rates by country
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
	    population_year = pop.year
from pop
where co.country_region = pop.country_name
;

--select * from covid19.countries where population is null;

-- get rates of infection per 100,000 people
update covid19.countries
    set confirmed_per_100k = confirmed::float / population:: float * 100000.0,
	    deaths_per_100k = deaths::float / population:: float * 100000.0,
	    recovered_per_100k = recovered::float / population:: float * 100000.0,
	    active_per_100k = active::float / population:: float * 100000.0;


-- get current data by country
DROP TABLE IF EXISTS covid19.countries_current;
CREATE TABLE covid19.countries_current AS
select *
from covid19.countries
where the_date = max_date
;

ALTER TABLE covid19.countries_current ADD CONSTRAINT countries_current_pkey PRIMARY KEY (country_region);
CREATE INDEX countries_current_geom_idx ON covid19.countries_current USING gist (geom);
ALTER TABLE covid19.countries_current CLUSTER ON countries_current_geom_idx;

ANALYSE covid19.countries_current;


-- update all country coordinates to their current coords
-- fixes quirk where country coords jump between dates when cases change counts
update covid19.countries as cnt
    set latitude = curr.latitude,
        longitude = curr.longitude,
		geom = curr.geom
from covid19.countries_current as curr
where cnt.country_region = curr.country_region;

analyse covid19.countries;



-- create view of days since 1 case per million
drop view if exists covid19.vw_countries_1_per_million;
create view covid19.vw_countries_1_per_million as
with dte as (
    select country_region,
           min(the_date) as start_date
    from covid19.countries
    where confirmed::float / (population::float / 1000000.0) >= 1.0
        and population > 1000000
    group by country_region
)
select cases.country_region,
       the_date,
       the_date - dte.start_date as days_since_1_per_mil,
       dte.start_date,
       confirmed,
       active,
       recovered,
       deaths,
       population,
       (confirmed::float / (population::float / 1000000.0))::integer as cases_per_million,
       (recovered::float / (population::float / 1000000.0))::integer as recovered_per_million,
       (active::float / (population::float / 1000000.0))::integer as active_per_million,
       (deaths::float / (population::float / 1000000.0))::integer as deaths_per_million
from covid19.countries as cases
inner join dte on cases.country_region = dte.country_region
  and cases.the_date >= dte.start_date
;

-- output to CSVs

COPY (SELECT province_state, country_region, the_date, latitude, longitude, confirmed, deaths, recovered, active, daily_change, weekly_change, daily_change_percent, weekly_change_percent FROM covid19.cases)
TO '/Users/hugh.saalmans/git/minus34/covid19/output_files/time_series_19-covid-cases.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (SELECT country_region, the_date, days_since_100_cases, start_date, max_date, confirmed, deaths, recovered, active, latitude, longitude FROM covid19.countries_100_plus)
TO '/Users/hugh.saalmans/git/minus34/covid19/output_files/time_series_19-covid-by-country-100-plus.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (
    SELECT country_region,
           days_since_1_per_mil,
           start_date,
           confirmed,
           active,
           recovered,
           deaths,
           population,
           cases_per_million,
           recovered_per_million,
           active_per_million,
           deaths_per_million
    FROM covid19.vw_countries_1_per_million
    WHERE country_region in ('Australia', 'Italy', 'Germany', 'Spain', 'France', 'United States of America', 'United Kingdom', 'China', 'Singapore', 'Iran', 'South Korea', 'Austria', 'Switzerland', 'Norway', 'Indonesia')
)
TO '/Users/hugh.saalmans/git/minus34/covid19/output_files/time_series_19-covid-by-country-1-per-million.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (SELECT country_region, days_since_100_cases, diff_to_aus, aus_active, active FROM covid19.vw_countries_100_plus)
TO '/Users/hugh.saalmans/git/minus34/covid19/output_files/time_series_19-covid-by-country-100-plus-view.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (SELECT country_region, the_date, days_since_first_case, start_date, max_date, confirmed, deaths, recovered, active, population, confirmed_per_100k, deaths_per_100k, recovered_per_100k, active_per_100k, population_year, latitude, longitude FROM covid19.countries)
TO '/Users/hugh.saalmans/git/minus34/covid19/output_files/time_series_19-covid-by-country.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (SELECT country_region, the_date, days_since_first_case, start_date, max_date, confirmed, deaths, recovered, active, population, confirmed_per_100k, deaths_per_100k, recovered_per_100k, active_per_100k, population_year, latitude, longitude FROM covid19.countries_current)
TO '/Users/hugh.saalmans/git/minus34/covid19/output_files/time_series_19-covid-by-country-current.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);


-- remove San Marino and The Vatican from countries tables as their cases per 100k are high off a small number of cases
delete from covid19.countries where country_region in ('San Marino', 'Holy See');
delete from covid19.countries_current where country_region in ('San Marino', 'Holy See');

-- remove the cruise shaip data
delete from covid19.countries where country_region = 'Cruise Ship';


select * from covid19.cases;
