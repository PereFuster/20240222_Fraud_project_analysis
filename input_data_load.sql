-- PAYMENT DISTRIBUTION  
-- Count payments
-- User payment

-- User payment volatility 
-- Maxium payment vs MLE payment
-- Maxium Percetage of revenue spent in one day 
-- Interquantile range 
-- Coefficient of variation 
-- Days paying out of total days since install 

-- PAYMENT TIME DYNAMICS 
-- Time since maximum payment： 
-- Time since last payment
-- Time since first payment 
-- Time to make the first payment (This can be a bit prone to generate overfit)
-- Payment growth rate
-- Ammount in payments growth 
-- Spending rate
-- Consecutive days paying 
-- Consecutive days not paying 
-- Hour of the day when paying 
-- Weekend payment share



-- Products
-- Dispute contextes and past occurences 
-- Balance at the moment of dispute 
-- Ammount Payment / hours played
-- Hours played before payment 
-- Hours played by now
-- Daily variance of ours played 
-- Hours played on its peack and also relaive to the daily hours played (In different contextes) 
-- Hours since install before payment 
-- Hours since install / payment 
-- Hours since install by now
-- Active days
-- Payment method
-- State
-- What did he pay 
-- Count distinct events 
-- Hours of the day paying
-- Payments outside normal time 
-- Normal Time
-- Share ammount payment outside normal time
-- Payments on weekends ratio


with payment_aux as (
select *
from ta.v_event_59
where "$part_event" = 'order_pay'
    and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2023-11-01'
    and ("is_true" is null or "is_true" = true)
    and "$part_date" is not null)

, ios_payments as (
select    
    a."#account_id"
    , b."register_time"
    , min(a."#event_time") over(partition by a."#account_id")                           as first_payment
    , min(c."#event_time") over(partition by a."#account_id")                           as first_dispute
    , a."pay_id"        
    , a."#event_time"                                                                   as "pay_time"
    , c."#event_time"                                                                   as "dispute_time"
    , a."pay_amount"

from payment_aux as a
  join ta.v_user_59 as b                                                                                          on a."#account_id" = b."#account_id"   and  a."#event_time" > b."register_time"
  left join (select * from ta.v_event_59 where "$part_event" = 'pay_dispute' and "$part_date" is not null) as c   on a.pay_id        = c.pay_id          and  a."#account_id" = c."#account_id"
where cast(date_format(b."register_time", '%Y-%m-%d') as varchar) > '2023-11-01'
  and b."bundle_id" = 'com.acorncasino.slots'
 )

, date_table as (
    select *  
    from (
            select 
                date_format("register_time", '%Y-%m-%d') as payment_date 
                from ios_payments
            group by 1 
        )
        cross join (select "#account_id",  "register_time" FROM ios_payments where cast(date_format("register_time", '%Y-%m-%d') as varchar) > '2023-11-01' group by 1,2) 
    where payment_date >= date_format("register_time", '%Y-%m-%d')
)

, final_out_pre_filter as (
select
    dt."#account_id"
    , dt."register_time"
    , dt.payment_date
    , date_diff('day', dt."register_time", cast(dt.payment_date as date))                                                                     as user_matuirity
                                                       
    , min(first_payment) over(partition by dt."#account_id")                                                                                  as first_payment_time
    , min(min("dispute_time")) over(partition by dt."#account_id")                                                                            as first_dispute_time
    
    , count(*) over()                                                                                                                         as pull_size 
    
    , date_diff('minute', dt."register_time", min(first_payment) over(partition by dt."#account_id"))/24                                      as time_to_payment
    , date_diff('hour', min(first_payment) over(partition by dt."#account_id"), min(first_dispute) over(partition by dt."#account_id"))       as time_pay_to_dispute
    , date_diff('hour', dt."register_time", min(first_dispute) over(partition by dt."#account_id"))                                           as time_to_dispute
    
    -- TOTAL PAYMENT
    , sum(sum(if(pay_amount > 0, 1, 0))) over (
                partition by dt."#account_id"                                                                  
                order by dt.payment_date rows between unbounded preceding and current row)                                                    as payments_to_date
                      
    , sum(sum(if(pay_amount > 0, 1, 0))) over (   
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between 7 preceding and current row)                                                            as payments_last_7_days
                      
    , sum(sum(pay_amount)) over (     
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between 7 preceding and current row)                                                            as spent_last_7_days
                      
    , sum(sum(pay_amount)) over (     
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between unbounded preceding and current row)                                                    as spent_to_date
    
    -- User payment volatility 
    , max((max(pay_amount))) over (   
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between unbounded preceding and current row)                                                    as max_payment

    , max((sum(pay_amount))) over (   
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between unbounded preceding and current row)                                                    as max_payment_in_day
                
    , stddev((sum(pay_amount))) over (   
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between unbounded preceding and current row)                                                    as std_payments


    -- RESPONSE: Number of disputes between the target date and following 30 days  
    -- , sum(if(date_diff('day', cast(dt.payment_date as date), current_date) <= 30, 1, 0))                                                      as disputes_in_30_days
    , if(date_diff('day', cast(dt.payment_date as date)
            , min(min("dispute_time")) over(partition by dt."#account_id")) < 30, 1, 0)                                                       as disputer_lt_30d -- What is the earlier dispute of each user? If difference between the current date and the dispute 
                                                                                                                                                                 -- is lower than 30, we can consider it a dispute, otherwise (Null or later), we consider it a non-dispute  
                                                                                                                                                       
    -- Time dynamics: 
    -- Time to maximum payment: Check 
    , if(max((sum(pay_amount))) over (partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between unbounded preceding and current row) 
                = sum(pay_amount), dt.payment_date, null)                                                                                     as max_payment_date 
    
    -- Hour of the day 
    -- Number of payments made between 7 am and 16 
    , max(max(extract(hour from pay_time))) over (     
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between 14 preceding and current row)                                                           as latest_hour_payment
                
    , avg(max(extract(hour from pay_time))) over (     
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between 14 preceding and current row)                                                           as average_hour_payment
    
    , min(min(extract(hour from pay_time))) over (     
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between 14 preceding and current row)                                                           as earliers_hour_payment    


    -- Time since last payment 
    -- , max(max("pay_time")) over(partition by dt."#account_id")                                                                                as last_payment_time
    , date_diff('hour', max(max("pay_time")) over(partition by dt."#account_id")
            ,  cast(payment_date as timestamp)) + 24                                                                                          as time_since_last_payment 

    , sum(sum(pay_amount)) over (     
            partition by dt."#account_id"                                                                     
            order by dt.payment_date rows between 14 preceding and 7 preceding)                                                                
        / 
      sum(sum(pay_amount)) over (     
                partition by dt."#account_id"                                                                     
                order by dt.payment_date rows between 7 preceding and current row)                                                           as spent_increase_last_week
                
    , array_agg(if(sum(pay_amount) > 0, '1', '0')) over (
        partition by dt."#account_id"                                                                  
        order by dt.payment_date rows between unbounded preceding and current row)                                                           as daily_payment_history_binary

    , array_agg(sum(if(pay_amount > 0, 1, 0))) over (
        partition by dt."#account_id"                                                                  
        order by dt.payment_date rows between unbounded preceding and current row)                                                           as daily_payment_history
                
    -- Day of the week 
    -- Share of weekend payments  
    
                                                                                                                                                                 
from date_table as dt
    left join ios_payments 
        on dt."#account_id" = ios_payments."#account_id"
        and dt.payment_date = date_format(ios_payments.pay_time, '%Y-%m-%d') 
-- from ios_payments
-- where (first_dispute is null or date_diff('day', register_time, first_dispute) <= 35) 
--     and dt.payment_date >= date_format("register_time", '%Y-%m-%d')
-- where dt.payment_date >= date_format("register_time", '%Y-%m-%d')
--     and dt.payment_date = '2023-10-06'
where date_diff('day', cast(dt.payment_date as date), current_date)                >= 30 -- I only want to consider dates that ocurred in the last 30 days
    -- and (date_diff('day', cast(dt.payment_date as date), dispute_time)             > 0 or dispute_time is null)  -- If the user has already disputed, I also don't want it
group by 1,2,3, first_payment, first_dispute
) 

, payments as (
select 
    *
from final_out_pre_filter
where  (date_diff('minute', cast(payment_date as timestamp), first_dispute_time) >= 24*60
    or  first_dispute_time is null) -- If the user has already disputed, I also don't want it
    and date_diff('minute', cast(payment_date as timestamp), first_payment_time) <= 0  -- If the user has not paid yet I also don't want him (So the first payment date should be before the date)
    and user_matuirity between 15 and 50
order by "#account_id", user_matuirity desc
) 

  , activity as (
select
 "#account_id"
 , date_format("#event_time", '%Y-%m-%d')                                               as payment_date 
 , count(distinct "spin_id")                                                            as games_played
 
 , count(distinct if("bet_money"  > 0, "spin_id", null))                                as money_games
 , count(distinct if("win_amount" > 0, "spin_id", null))                                as money_games_wins
 
--  , count(distinct if("bet_chips"  > 0, "spin_id", null))                                as chip_games
--  , count(distinct if("bet_chips" > 0, "spin_id", null))                                 as chip_games_wins
 
--  , min("#event_time")                                                                   as started_playing_time
--  , max("#event_time")                                                                   as finished_playing_time
 
--  , array_agg(extract(hour from "#event_time"))                                          as hourly_activity_distribution

from  ta.v_event_59
where "$part_event" = 'game_play'
  and   "$part_date" is not null
  and "#account_id" in (select '#account_id' from final_out_pre_filter group by 1)
group by 1, date_format("#event_time", '%Y-%m-%d')
-- limit 555
)

select * from activity limit 555

-- limit 5000



-- select 
--     user_matuirity
--     -- , '2'
--     , sum(if(disputer_lt_30d > 0, 1, 0)) / cast(count(*) as double)                                                     as d30_dispute_rate
--     , sum(sum(if(disputer_lt_30d > 0, 1, 0))) over (
--                 order by user_matuirity rows between unbounded preceding and current row
--                 ) / sum(cast(count(*) as double)) over( 
--                         order by user_matuirity rows between unbounded preceding and current row)             as cum_d30_dispute_rate
-- from final_out_pre_filter
-- where  (date_diff('day', cast(payment_date as date), first_dispute_time) >= 0 
--     or  first_dispute_time is null) -- If the user has already disputed, I also don't want it
--     and date_diff('day', cast(payment_date as date), first_payment_time) <= 0  -- If the user has not paid yet I also don't want him (So the first payment date should be before the date)
-- group by 1 


