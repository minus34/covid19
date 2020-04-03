

-- percentage increases for Australia
with active as (
    select country_region,
           the_date,
           active,
           (active) - lag(active) over (partition by country_region order by the_date) as daily_change,
           (active) - lag(active, 7) over (partition by country_region order by the_date) as weekly_change
    from covid19.countries
    where country_region = 'Australia'
)
select *,
       (case when lag(active) over (partition by country_region order by the_date) > 0 then daily_change::float /
           (lag(active) over (partition by country_region order by the_date))::float * 100.0 end)::integer as daily_change_percent,
       (case when lag(active, 7) over (partition by country_region order by the_date) > 0 then weekly_change::float /
           (lag(active, 7) over (partition by country_region order by the_date))::float * 100.0 end)::integer as weekly_change_percent
from active
order by the_date;




-- it took Australia 7 days to go from 100 to 1,000 (if JHU data has correct dates?)
select *
from covid19.countries_100_plus
where country_region = 'Australia'
order by the_date;

select *
from covid19.countries_100_plus
where country_region = 'Italy'
order by the_date;


    select country_region
    from covid19.countries
    where population = 0




select country_region,
       confirmed,
       (confirmed::float / (population::float / 1000000.0)) as confirmed_per_million,
       the_date
from covid19.countries
where country_region = 'Australia'
;





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