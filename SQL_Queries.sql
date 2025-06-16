-- use the ecommerce_data database
use ecommerce_data;

-- phase 1: exploratory data understanding & cleaning

-- 1.1 preview of user events
select *
from ecommerce_user_events
limit 10;

-- 1.2 total distinct users
select count(distinct user_id) as total_no_of_users
from ecommerce_user_events;

-- 1.3 device type distribution
select 
    device_type, 
    count(device_type) as device_type_distribution
from ecommerce_user_events
group by device_type
order by device_type_distribution;

-- 1.4 user's most-used device
with user_device_event_counts as (
    select 
        user_id, 
        device_type, 
        count(*) as event_count
    from ecommerce_user_events
    where device_type is not null
    group by user_id, device_type
),
ranked_devices as (
    select 
        user_id, 
        device_type, 
        dense_rank() over(partition by user_id order by event_count desc) as device_rank
    from user_device_event_counts
)
select 
    user_id, 
    device_type 
from ranked_devices 
where device_rank = 1;

-- 1.5 product category interactions
select 
    product_category, 
    count(*) as product_category_interactions
from ecommerce_user_events
where product_category is not null
group by product_category
order by product_category_interactions desc;

-- 1.6 membership type distribution
with membership_distribution as (
    select 
        user_id, 
        membership_type
    from ecommerce_user_events
    where membership_type is not null
    group by user_id, membership_type
)
select 
    membership_type, 
    count(*) as user_count
from membership_distribution
group by membership_type
order by user_count desc;

-- 1.7 session duration by event type
select 
    event_type, 
    avg(session_duration) as average_time_spent_per_event, 
    max(session_duration) as max_time_spent_per_event, 
    min(session_duration) as min_time_spent_per_event
from ecommerce_user_events
group by event_type
order by average_time_spent_per_event desc;

-- 1.8 event type distribution
select 
    event_type, 
    count(event_type) as count
from ecommerce_user_events
group by event_type
order by count desc;

-- 1.9 session duration by device
select 
    device_type, 
    avg(session_duration) as average_time_spent_per_event, 
    max(session_duration) as max_time_spent_per_event, 
    min(session_duration) as min_time_spent_per_event
from ecommerce_user_events
group by device_type
order by average_time_spent_per_event desc;

-- 1.10 session duration by device and user segment
select 
    device_type, 
    user_segment, 
    avg(session_duration) as average_time_spent_per_event, 
    max(session_duration) as max_time_spent_per_event, 
    min(session_duration) as min_time_spent_per_event
from ecommerce_user_events
group by device_type, user_segment
order by average_time_spent_per_event desc;

-- 1.11 session duration by age group
with age_binned_sessions as (
    select 
        event_type, 
        session_duration, 
        case 
            when age >= 18 and age <= 25 then '18-25'
            when age >= 26 and age <= 35 then '26-35'
            when age >= 36 and age <= 45 then '36-45'
            when age >= 46 and age <= 60 then '46-60' 
        end as age_group
    from ecommerce_user_events
)
select 
    age_group, 
    avg(session_duration) as average_time_spent_per_event, 
    max(session_duration) as max_time_spent_per_event, 
    min(session_duration) as min_time_spent_per_event
from age_binned_sessions
group by age_group
order by average_time_spent_per_event desc;

-- 1.12 session duration by event type and age group
with event_age_group_sessions as (
    select 
        event_type, 
        session_duration, 
        case 
            when age >= 18 and age <= 25 then '18-25'
            when age >= 26 and age <= 35 then '26-35'
            when age >= 36 and age <= 45 then '36-45'
            when age >= 46 and age <= 60 then '46-60' 
        end as age_group
    from ecommerce_user_events
)
select 
    event_type, 
    age_group, 
    avg(session_duration) as average_time_spent_per_event, 
    max(session_duration) as max_time_spent_per_event, 
    min(session_duration) as min_time_spent_per_event
from event_age_group_sessions
group by event_type, age_group
order by average_time_spent_per_event desc;

-- phase 2: funnel & conversion analysis

-- 2.1 user funnel stage counts
with user_event_counts_per_stage as (
    select 
        user_id, 
        event_type, 
        count(event_type) as event_count
    from ecommerce_user_events
    where event_type in ('login', 'view_product', 'add_to_cart', 'checkout', 'purchase')
    group by user_id, event_type
)
select 
    event_type, 
    sum(event_count) as user_at_each_stage
from user_event_counts_per_stage
group by event_type
order by field(event_type, 'login', 'view_product', 'add_to_cart', 'checkout', 'purchase');

-- 2.2 funnel drop-off percentage
with event_counts_by_user_stage as (
    select 
        user_id, 
        event_type, 
        count(event_type) as event_count
    from ecommerce_user_events
    where event_type in ('login', 'view_product', 'add_to_cart', 'checkout', 'purchase')
    group by user_id, event_type
),
total_event_counts_per_stage as (
    select 
        event_type, 
        sum(event_count) as user_at_each_stage
    from event_counts_by_user_stage
    group by event_type
),
funnel_with_previous_stage as (
    select 
        event_type, 
        user_at_each_stage, 
        lag(user_at_each_stage) over (
            order by field(event_type, 'login', 'view_product', 'add_to_cart', 'checkout', 'purchase')
        ) as prev
    from total_event_counts_per_stage
)
select 
    event_type, 
    user_at_each_stage, 
    ifnull(round((prev - user_at_each_stage) * 100 / prev, 2), '-') as funnel_dropoff_percentage
from funnel_with_previous_stage
order by field(event_type, 'login', 'view_product', 'add_to_cart', 'checkout', 'purchase');

-- 2.3 cohort funnel analysis
with user_cohort as (
    select 
        user_id, 
        date_format(user_signup_date, '%Y-%m') as cohort_month
    from ecommerce_user_events
    group by user_id, cohort_month
),
user_funnel_steps as (
    select 
        user_id, 
        min(case when event_type = 'login' then event_time end) as login_time,
        min(case when event_type = 'view_product' then event_time end) as view_time,
        min(case when event_type = 'add_to_cart' then event_time end) as cart_time,
        min(case when event_type = 'checkout' then event_time end) as checkout_time,
        min(case when event_type = 'purchase' then event_time end) as purchase_time
    from ecommerce_user_events
    group by user_id
),
cohort_funnel as (
    select 
        uc.user_id as user_id, 
        uc.cohort_month as cohort_month, 
        (case when ufs.login_time is not null then 1 else 0 end) as login_time, 
        (case when ufs.view_time is not null then 1 else 0 end) as view_time, 
        (case when ufs.cart_time is not null then 1 else 0 end) as cart_time, 
        (case when ufs.checkout_time is not null then 1 else 0 end) as checkout_time, 
        (case when ufs.purchase_time is not null then 1 else 0 end) as purchase_time
    from user_cohort uc
    join user_funnel_steps ufs on uc.user_id = ufs.user_id
)
select 
    cohort_month, 
    count(distinct user_id) as total_users, 
    sum(login_time) as login_users, 
    sum(view_time) as viewed_users, 
    sum(cart_time) as cart_users, 
    sum(checkout_time) as checkout_users, 
    sum(purchase_time) as purchased_users
from cohort_funnel
group by cohort_month;

-- phase 3: sales trends & forecasting

-- 3.1 monthly sales
with monthly_sales as (
    select sales_amount, date_format(event_time, '%M') as month
    from ecommerce_user_events
)
select month, round(sum(sales_amount), 2) as sales
from monthly_sales
group by month
order by sales desc;

-- 3.2 weekly sales
with weekly_sales as (
    select sales_amount, date_format(event_time, '%W') as day_name
    from ecommerce_user_events
)
select day_name, round(sum(sales_amount), 2) as sales
from weekly_sales
group by day_name
order by sales desc;

-- 3.3 sales on particular day of month
with day_of_month as (
    select sales_amount, date_format(event_time, '%m') as day_of_month
    from ecommerce_user_events
)
select day_of_month, round(sum(sales_amount), 2) as sales
from day_of_month
group by day_of_month
order by sales desc;

-- 3.4 7-day moving average
with recursive calendar_range as (
    select min(date(event_time)) as date
    from ecommerce_user_events
    union all
    select date_add(date, interval 1 day)
    from calendar_range
    where date < (select max(date(event_time)) from ecommerce_user_events)
),
daily_sales_aggregated as (
    select date(event_time) as sales_date, round(sum(sales_amount), 2) as sales_amount
    from ecommerce_user_events
    group by sales_date
),
complete_sales_calendar as (
    select cr.date as sales_date, ifnull(ds.sales_amount, 0) as sales_amount
    from calendar_range cr
    left join daily_sales_aggregated ds on cr.date = ds.sales_date
),
sales_with_moving_average as (
    select 
        sales_date,
        sum(sales_amount) over (order by sales_date rows between 6 preceding and current row) as moving_7d_sales,
        row_number() over (order by sales_date) as rn
    from complete_sales_calendar
)
select sales_date, round(avg(moving_7d_sales), 2) as moving_7d_sales
from sales_with_moving_average
where rn % 7 = 0
order by sales_date;

-- 3.5 30-day moving average
with recursive calendar_range as (
    select min(date(event_time)) as date
    from ecommerce_user_events
    union all
    select date_add(date, interval 1 day)
    from calendar_range
    where date < (select max(date(event_time)) from ecommerce_user_events)
),
daily_sales_aggregated as (
    select date(event_time) as sales_date, round(sum(sales_amount), 2) as sales_amount
    from ecommerce_user_events
    group by sales_date
),
complete_sales_calendar as (
    select cr.date as sales_date, ifnull(ds.sales_amount, 0) as sales_amount
    from calendar_range cr
    left join daily_sales_aggregated ds on cr.date = ds.sales_date
),
sales_with_moving_average as (
    select 
        sales_date,
        sum(sales_amount) over (order by sales_date rows between 29 preceding and current row) as moving_30d_sales,
        row_number() over (order by sales_date) as rn
    from complete_sales_calendar
)
select sales_date, round(avg(moving_30d_sales), 2) as moving_30d_sales
from sales_with_moving_average
where rn % 30 = 0;

-- 3.6 7-day linear forecasting of daily sales
with daily_sales as (
    select date(event_time) as sales_date, round(sum(sales_amount), 2) as sales
    from ecommerce_user_events
    where event_type = 'purchase'
    group by sales_date
    order by sales_date desc
    limit 7
),
indexed_sales as (
    select sales_date, sales, row_number() over(order by sales_date) as x
    from daily_sales
),
stats as (
    select 
        sum(x) as sum_x,
        sum(sales) as sum_y,
        sum(x * sales) as sum_xy,
        sum(x * x) as sum_x2,
        count(*) as n
    from indexed_sales
),
regression as (
    select 
        (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x) as slope,
        (sum_y - ((n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)) * sum_x) / n as intercept
    from stats
)
select 
    slope,
    intercept,
    round(slope * 8 + intercept, 2) as forecast_sales_day_8
from regression;

-- 3.7 30-day linear forecasting of daily sales
with daily_sales as (
    select date(event_time) as sales_date, round(sum(sales_amount), 2) as sales
    from ecommerce_user_events
    where event_type = 'purchase'
    group by sales_date
    order by sales_date desc
    limit 30
),
indexed_sales as (
    select sales_date, sales, row_number() over(order by sales_date) as x
    from daily_sales
),
stats as (
    select 
        sum(x) as sum_x,
        sum(sales) as sum_y,
        sum(x * sales) as sum_xy,
        sum(x * x) as sum_x2,
        count(*) as n
    from indexed_sales
),
regression as (
    select 
        (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x) as slope,
        (sum_y - ((n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)) * sum_x) / n as intercept
    from stats
)
select 
    slope,
    intercept,
    round(slope * 31 + intercept, 2) as forecast_sales_day_31
from regression;

-- phase 4: product-based analytics

-- 4.1 products with highest/lowest sales
select product_category, round(sum(sales_amount), 2) as sales
from ecommerce_user_events
where product_category is not null
group by product_category
order by sales desc;

-- 4.2 best/worst product categories
select 
    product_category, 
    round(sum(sales_amount), 2) as sales, 
    round(avg(product_rating), 2) as average_rating, 
    count(case when event_type = 'purchase' then 1 else null end) as number_of_time_ofpurchase
from ecommerce_user_events
where product_category is not null
group by product_category
order by sales, average_rating, number_of_time_ofpurchase desc;

-- phase 5: customer segmentation & metrics

-- 5.1 customer lifetime value (ltv) - average revenue per user (arpu) monthly
with purchases as (
    select user_id, sales_amount, date_format(event_time, '%Y-%m') as purchase_month
    from ecommerce_user_events
),
monthly_revenue as (
    select purchase_month, sum(sales_amount) as total_revenue, count(distinct user_id) as active_users
    from purchases
    group by purchase_month
),
ltv_by_month as (
    select round(total_revenue/active_users, 2) as ltv_per_user
    from monthly_revenue
)
select *
from ltv_by_month
order by ltv_per_user desc;

-- 5.2 acquisition cost analysis
select channel, round(avg(acquisition_cost), 2) as acquisition_cost_bychannel
from ecommerce_user_events
group by channel
order by acquisition_cost_bychannel desc;

-- phase 6: rfm segmentation

-- 6.1 rfm segmentation
with latest_purchase as (
    select user_id, max(date(event_time)) as last_purchase_date
    from ecommerce_user_events
    where event_type = 'purchase'
    group by user_id
),
recency_scores as (
    select 
        user_id, 
        datediff(current_date(), last_purchase_date) as recency,
        ntile(5) over (order by datediff(current_date(), last_purchase_date)) as recency_score
    from latest_purchase
),
frequency_scores as (
    select 
        user_id, 
        count(*) as purchase_count,
        ntile(5) over (order by count(*) desc) as frequency_score
    from ecommerce_user_events
    where event_type = 'purchase'
    group by user_id
),
monetary_scores as (
    select 
        user_id, 
        round(sum(sales_amount), 2) as total_spent,
        ntile(5) over (order by round(sum(sales_amount), 2) desc) as monetary_score
    from ecommerce_user_events
    where event_type = 'purchase'
    group by user_id
),
rfm_combined as (
    select 
        rs.user_id,
        rs.recency_score,
        fs.frequency_score,
        ms.monetary_score
    from recency_scores rs
    join frequency_scores fs on rs.user_id = fs.user_id
    join monetary_scores ms on rs.user_id = ms.user_id
)
select 
    *, 
    recency_score * 100 + frequency_score * 10 + monetary_score as rfm_score
from rfm_combined
order by rfm_score desc;

-- 6.2 segmentation by spend
with user_total_spend as (
    select user_id, round(sum(sales_amount), 2) as total_spent
    from ecommerce_user_events
    where sales_amount is not null
    group by user_id
),
user_segments as (
    select user_id, total_spent, ntile(3) over (order by total_spent desc) as spend_segment
    from user_total_spend
)
select 
    user_id, 
    total_spent, 
    case 
        when spend_segment = 1 then 'highspender'
        when spend_segment = 2 then 'medium spender'
        when spend_segment = 3 then 'low spender' 
    end as spend_category
from user_segments;

-- phase 7: traffic & channel attribution

-- 7.1 users by traffic source
select 
    traffic_source, 
    count(distinct user_id) as number_of_users
from ecommerce_user_events
group by traffic_source
order by number_of_users desc;

-- 7.2 sessions by channel
select 
    channel, 
    count(distinct session_id) as sessions
from ecommerce_user_events
group by channel
order by sessions desc;

-- phase 8: location & logistics analysis

-- 8.1 number of users by city
select city, count(distinct user_id) as number_of_users
from ecommerce_user_events
group by city
order by number_of_users desc;

-- 8.2 city-wise revenue
select city, round(avg(sales_amount), 2)
from ecommerce_user_events
group by city
order by round(avg(sales_amount), 2) desc;

-- 8.3 membership breakup by city
select 
    city, 
    membership_type, 
    count(membership_type) as membership_breakup_bycity
from ecommerce_user_events
group by city, membership_type
order by count(membership_type) desc;

-- phase 9: payment & installment behavior

-- 9.1 payment method distribution for purchases
select 
    payment_method, 
    count(*) as payment_method_distribution
from ecommerce_user_events
where 
    event_type = 'purchase' 
    and sales_amount is not null 
    and payment_method is not null
group by payment_method
order by payment_method_distribution desc;

-- phase 10: a/b testing & group comparison

-- 10.1 comparative analysis of test and control groups - conversion & revenue metrics
with conversion_data as (
    select 
        user_id, 
        test_group, 
        sum(case when event_type = 'purchase' then 1 else 0 end) as converted, 
        round(sum(sales_amount), 2) as sales
    from ecommerce_user_events
    group by user_id, test_group
)
select 
    test_group, 
    count(user_id) as users, 
    sum(converted) as total_converted, 
    round(100.0 * sum(converted)/count(user_id), 2) as conversion_rate, 
    sum(sales) as total_revenue, 
    round(avg(sales), 2) as avg_revenue_per_user
from conversion_data
group by test_group;