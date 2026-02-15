with cte as (
    select started_at,
    date(to_timestamp(started_at)) as date_started_at,
    hour(to_timestamp(started_at)) as hour,

    case
    when dayname(to_timestamp(started_at)) in ('sat','sun')
    then 'weekend'
    else 'bussiness day'
    end as day_type,

    case
    when month(to_timestamp(started_at)) in (12,1,2)
    then 'winter'
    when month(to_timestamp(started_at)) in (3,4,5)
    then 'spring'
    when month(to_timestamp(started_at)) in (6,7,8)
    then 'summer'
    else 'autumn'
    end as stationm_of_year

from {{ source('demo', 'bike') }}
where started_at != 'started_at'
)

select * from cte