with stack_data_0 as (
  select
        split(tags, "|") tags 
      , view_count 
      , date_trunc(date(creation_date), month) month_
      , date(creation_date) v_date
  from `bigquery-public-data.stackoverflow.posts_questions`
  where date(creation_date) = cast('{{ ds }}' as date)
  )
,
stack_data as (
  select
      v_date
    , month_
    , tag
    , sum(view_count) view_count
  from stack_data_0 ,
  UNNEST (tags) as tag
  group by 1, 2, 3
  )
,
stack_monthly_data as (
  select month_
  , tag
  , sum(view_count) monthly_view_count
from stack_data
group by 1, 2
)
,
final as (
  select 
      month_ 
    , tag
    , row_number() over (partition by month_ order by monthly_view_count desc) row_1
  from stack_monthly_data
  )
  
select 
    sd.v_date
  , sd.month_
  , sd.tag 
  , sd.view_count
  , ff.row_1
from stack_data  sd
inner join final ff on sd.tag = ff.tag and sd.month_ = ff.month_
where ff.row_1 <= 5


with stack_data_0 as (
  select
        split(tags, "|") tags 
      , view_count 
      , date_trunc(date(creation_date), month) month_
  from `bigquery-public-data.stackoverflow.posts_questions`
  where date(creation_date) >= '2020-01-01'
  and date(creation_date) <= cast('{{ ds }}' as date)
  )
,
stack_data as (
  select
      month_
    , tag
    , sum(view_count) view_count
  from stack_data_0 ,
  UNNEST (tags) as tag
  group by 1, 2
  )
,
final as (
  select 
      month_ 
    , tag
    , row_number() over (partition by month_ order by view_count desc) row_1
  from stack_data
  )
  
select 
    sd.month_
  , sd.tag 
  , sd.view_count
  , ff.row_1
from stack_data  sd
inner join final ff on sd.tag = ff.tag and sd.month_ = ff.month_
where ff.row_1 <= 5