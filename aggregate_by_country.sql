

-- update country names to match World Bank names
update covid19.cases set country_region = 'Brunei Darussalam' where country_region = 'Brunei';
update covid19.cases set country_region = 'Congo, Dem. Rep.' where country_region = 'Congo (Kinshasa)';
update covid19.cases set country_region = 'Congo, Rep.' where country_region = 'Congo (Brazzaville)';
update covid19.cases set country_region = 'Congo, Rep.' where country_region = 'Republic of the Congo';
update covid19.cases set country_region = 'Korea, Rep.' where country_region = 'Korea, South';
update covid19.cases set country_region = 'United States' where country_region = 'US';
update covid19.cases set country_region = 'Iran, Islamic Rep.' where country_region = 'Iran';
update covid19.cases set country_region = 'Egypt, Arab Rep.' where country_region = 'Egypt';
update covid19.cases set country_region = 'Russian Federation' where country_region = 'Russia';
update covid19.cases set country_region = 'Czech Republic' where country_region = 'Czechia';
update covid19.cases set country_region = 'Slovak Republic' where country_region = 'Slovakia';
update covid19.cases set country_region = 'Taiwan' where country_region = 'Taiwan*';
update covid19.cases set country_region = 'St. Lucia' where country_region = 'Saint Lucia';
update covid19.cases set country_region = 'Venezuela, RB' where country_region = 'Venezuela';
update covid19.cases set country_region = 'St. Vincent and the Grenadines' where country_region = 'Saint Vincent and the Grenadines';
update covid19.cases set country_region = 'Bahamas, The' where country_region = 'The Bahamas';



-- change these nearby territories to their "mother" country
update covid19.cases
    set province_state = country_region,
        country_region = 'United Kingdom'
where country_region = 'Jersey';

update covid19.cases
    set province_state = country_region,
        country_region = 'United Kingdom'
where country_region = 'Guernsey';


-- aggregate case data by country and apply manual fixes to cleanup data
DROP TABLE IF EXISTS covid19.countries;
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

-- manually set populations
update covid19.countries set population = 859959, population_year = 2020 where country_region = 'Reunion';
update covid19.countries set population = 376480, population_year = 2016 where country_region = 'Martinique';
update covid19.countries set population = 23780000, population_year = 2018 where country_region = 'Taiwan';
update covid19.countries set population = 290691, population_year = 2020 where country_region = 'French Guiana';
update covid19.countries set population = 1000, population_year = 2017 where country_region = 'Holy See';
update covid19.countries set population = 395700, population_year = 2016 where country_region = 'Guadeloupe';
update covid19.countries set population = 270372, population_year = 2019 where country_region = 'Mayotte';


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


-- output to CSVs

COPY (SELECT province_state, country_region, the_date, latitude, longitude, confirmed, deaths, recovered, active, daily_change, weekly_change, daily_change_percent, weekly_change_percent FROM covid19.cases)
TO '/Users/hugh.saalmans/git/minus34/covid19/time_series_19-covid-cases.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (SELECT country_region, the_date, days_since_first_case, start_date, max_date, confirmed, deaths, recovered, active, population, confirmed_per_100k, deaths_per_100k, recovered_per_100k, active_per_100k, population_year, latitude, longitude FROM covid19.countries)
TO '/Users/hugh.saalmans/git/minus34/covid19/time_series_19-covid-by-country.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);

COPY (SELECT country_region, the_date, days_since_first_case, start_date, max_date, confirmed, deaths, recovered, active, population, confirmed_per_100k, deaths_per_100k, recovered_per_100k, active_per_100k, population_year, latitude, longitude FROM covid19.countries_current)
TO '/Users/hugh.saalmans/git/minus34/covid19/time_series_19-covid-by-country-current.csv'
WITH (HEADER, DELIMITER ',', FORMAT CSV);


-- remove San Marino and The Vatican from countries tables as their cases per 100k are high off a small number of cases
delete from covid19.countries where country_region in ('San Marino', 'Holy See');
delete from covid19.countries_current where country_region in ('San Marino', 'Holy See');

