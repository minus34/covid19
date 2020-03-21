

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