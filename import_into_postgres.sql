
-- load COVID-19 case data

DROP TABLE IF EXISTS covid19.cases;
CREATE TABLE covid19.cases
(
    status text,
	province_state text,
	country_region text,
	latitude numeric(8,6),
	longitude numeric(9,6),
	the_date date,
	persons integer
--    geom geometry(Point,4326)
) WITH (OIDS = FALSE);
ALTER TABLE covid19.cases OWNER to postgres;

COPY covid19.cases (status, province_state, country_region, latitude, longitude, the_date, persons)
FROM '/Users/hugh.saalmans/git/minus34/covid19/time_series_19-covid-reformatted.csv' WITH (HEADER, DELIMITER ',', FORMAT CSV);

--UPDATE covid19.cases
--    set geom = ST_SetSRID(ST_Makepoint(longitude, latitude), 4326);

ANALYSE covid19.cases;

with data as (
select province_state,
	   country_region,
	   the_date,
	   sum(case when status = 'confirmed' then persons else 0 end) as confirmed,
	   sum(case when status = 'deaths' then persons else 0 end) as deaths,
	   sum(case when status = 'recovered' then persons else 0 end )as recovered,
	   latitude,
	   longitude,
       ST_SetSRID(ST_Makepoint(longitude, latitude), 4326) as geom
from covid19.cases
group by province_state,
	     country_region,
	     the_date,
	     latitude,
	     longitude
order by the_date desc
);



select * from covid19.cases where province_state = 'King County, WA' order by the_date desc;










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