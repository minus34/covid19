
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


-- load world population figures by country (source: World Bank - https://data.worldbank.org/indicator/SP.POP.TOTL)

"1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019"

DROP TABLE IF EXISTS covid19.world_population;
CREATE TABLE covid19.world_population
(
    country_name text,
	country_code text,
	indicator_name text,
	indicator_code text,
    "1960" integer, "1961" integer, "1962" integer, "1963" integer, "1964" integer, "1965" integer, "1966" integer, "1967" integer, "1968" integer, "1969" integer, "1970" integer, "1971" integer, "1972" integer, "1973" integer, "1974" integer, "1975" integer, "1976" integer, "1977" integer, "1978" integer, "1979" integer, "1980" integer, "1981" integer, "1982" integer, "1983" integer, "1984" integer, "1985" integer, "1986" integer, "1987" integer, "1988" integer, "1989" integer, "1990" integer, "1991" integer, "1992" integer, "1993" integer, "1994" integer, "1995" integer, "1996" integer, "1997" integer, "1998" integer, "1999" integer, "2000" integer, "2001" integer, "2002" integer, "2003" integer, "2004" integer, "2005" integer, "2006" integer, "2007" integer, "2008" integer, "2009" integer, "2010" integer, "2011" integer, "2012" integer, "2013" integer, "2014" integer, "2015" integer, "2016" integer, "2017" integer, "2018" integer, "2019" integer, dummy text
) WITH (OIDS = FALSE);
ALTER TABLE covid19.raw_cases OWNER to postgres;

COPY covid19.world_population
FROM '/Users/hugh.saalmans/git/minus34/covid19/API_SP.POP.TOTL_DS2_en_csv_v2_821007.csv' WITH (HEADER, DELIMITER ',', FORMAT CSV);

analyse covid19.world_population







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