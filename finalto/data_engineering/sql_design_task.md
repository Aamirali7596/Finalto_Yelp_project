# SQL Design
## Yelp Data Engineering Platform
**Author:** Aamir | **Version:** 1.0 | **Last Updated:** April 2026

---

## Table of Contents

1. [Rising Star Query — Silver Layer](#1-rising-star-query--silver-layer)
2. [Rising Star Query — Gold Layer](#2-rising-star-query--gold-layer)
3. [BI Scenario Queries](#3-bi-scenario-queries)

---

## 1. Rising Star Query — Silver Layer

```sql
with recent as (
    select
        business_id,
        avg(stars)          as avg_stars_recent,
        count(review_id)    as review_count_recent
    from silver.reviews
    where review_date >= dateadd(year, -1, current_date)
    group by business_id
),
historical as (
    select
        business_id,
        avg(stars)          as avg_stars_historical
    from silver.reviews
    where review_date < dateadd(year, -1, current_date)
    group by business_id
)
select
    b.business_id,
    b.name,
    round(r.avg_stars_recent,     2)                        as avg_stars_recent,
    round(h.avg_stars_historical, 2)                        as avg_stars_historical,
    round(r.avg_stars_recent - h.avg_stars_historical, 2)   as rating_improvement,
    r.review_count_recent
from recent r
inner join historical h      on r.business_id = h.business_id
inner join silver.business b on r.business_id = b.business_id
where r.review_count_recent  >= 10
  and r.avg_stars_recent     >= h.avg_stars_historical + 1.0
order by rating_improvement desc;
```

---

## 2. Rising Star Query — Gold Layer

```sql
with recent as (
    select
        f.business_id,
        avg(f.review_stars)     as avg_stars_recent,
        count(f.review_id)      as review_count_recent
    from gold.fact_reviews f
    join gold.dim_date d on f.date_id = d.date_id
    where d.full_date >= dateadd(year, -1, current_date)
    group by f.business_id
),
historical as (
    select
        f.business_id,
        avg(f.review_stars)     as avg_stars_historical
    from gold.fact_reviews f
    join gold.dim_date d on f.date_id = d.date_id
    where d.full_date < dateadd(year, -1, current_date)
    group by f.business_id
)
select
    b.business_id,
    b.name,
    round(r.avg_stars_recent,     2)                        as avg_stars_recent,
    round(h.avg_stars_historical, 2)                        as avg_stars_historical,
    round(r.avg_stars_recent - h.avg_stars_historical, 2)   as rating_improvement,
    r.review_count_recent
from recent r
inner join historical h        on r.business_id = h.business_id
inner join gold.dim_business b on r.business_id = b.business_id
where r.review_count_recent    >= 10
  and r.avg_stars_recent       >= h.avg_stars_historical + 1.0
order by rating_improvement desc;
```

---

## 3. BI Scenario Queries

### Q1 — Top Rated Businesses by City and Category

```sql
select
    b.city,
    b.primary_category,
    b.name,
    round(avg(f.review_stars), 2)   as avg_rating,
    count(f.review_id)              as total_reviews
from gold.fact_reviews f
join gold.dim_business b on f.business_id = b.business_id
join gold.dim_date d     on f.date_id     = d.date_id
where d.year = 2023
  and b.primary_category is not null
group by b.city, b.primary_category, b.name
having count(f.review_id) >= 10
order by avg_rating desc
limit 100;
```

### Q2 — Elite vs Non-Elite Reviewer Behaviour

```sql
select
    u.is_elite,
    count(f.review_id)                      as total_reviews,
    round(avg(f.review_stars),          2)  as avg_stars_given,
    round(avg(f.useful_votes),          2)  as avg_useful_votes,
    round(avg(f.review_length_chars),   0)  as avg_review_length_chars
from gold.fact_reviews f
join gold.dim_user u on f.user_id = u.user_id
group by u.is_elite
order by u.is_elite desc;
```

### Q3 — Peak Check-in Patterns by Business Category

```sql
select
    b.primary_category,
    c.checkin_day_of_week,
    c.checkin_hour,
    count(c.checkin_id)     as total_checkins
from gold.fact_checkins c
join gold.dim_business b on c.business_id = b.business_id
where b.primary_category is not null
group by b.primary_category, c.checkin_day_of_week, c.checkin_hour
order by b.primary_category, total_checkins desc;
```

### Q4 — Review Volume Trend Over Time

```sql
select
    d.year,
    d.month,
    count(f.review_id)              as total_reviews,
    round(avg(f.review_stars), 2)   as avg_rating
from gold.fact_reviews f
join gold.dim_date d on f.date_id = d.date_id
group by d.year, d.month
order by d.year, d.month;
```

### Q5 — Most Active Reviewers

```sql
select
    u.user_id,
    u.name,
    u.is_elite,
    u.fan_count,
    count(f.review_id)              as reviews_written,
    round(avg(f.review_stars), 2)   as avg_stars_given
from gold.fact_reviews f
join gold.dim_user u on f.user_id = u.user_id
group by u.user_id, u.name, u.is_elite, u.fan_count
order by reviews_written desc
limit 50;
```

### Q6 — Businesses With No Recent Reviews

```sql
select
    b.business_id,
    b.name,
    b.city,
    b.primary_category,
    b.review_count      as total_lifetime_reviews
from gold.dim_business b
left join (
    select distinct f.business_id
    from gold.fact_reviews f
    join gold.dim_date d on f.date_id = d.date_id
    where d.full_date >= dateadd(year, -1, current_date)
) recent on b.business_id = recent.business_id
where recent.business_id is null
  and b.is_open = true
order by b.review_count desc;
```

### Q7 — Running Average Rating Per Business (Window Function)

```sql
select
    f.business_id,
    b.name,
    d.full_date                                 as review_date,
    f.review_stars,
    round(
        avg(f.review_stars) over (
            partition by f.business_id
            order by d.full_date
            rows between unbounded preceding and current row
        ), 2
    )                                           as running_avg_stars,
    count(f.review_id) over (
        partition by f.business_id
        order by d.full_date
        rows between unbounded preceding and current row
    )                                           as cumulative_review_count
from gold.fact_reviews f
join gold.dim_business b on f.business_id = b.business_id
join gold.dim_date d     on f.date_id     = d.date_id
order by f.business_id, d.full_date;
```

---

*End of SQL Design*