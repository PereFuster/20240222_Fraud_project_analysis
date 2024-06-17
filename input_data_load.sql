
with payment_aux as (
select *
from ta.v_event_59
where "$part_event" = 'order_pay'
    and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2023-12-01'
    and ("is_true" is null or "is_true" = true)
    and "#event_time" < date_add('day', -30, current_date)
    and "$part_date" is not null)
    
, time_zone_mapping_tb_aux as (
select
    "#account_id"
    , "#zone_offset"
    , count() as total_users
*    , row_number() over (partition by "#account_id" order by count(*) desc) as zone_size_rank
from ta.v_event_59
where "$part_event" = 'ta_app_start'
    and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2023-12-01'
    and "#event_time" < date_add('day', -30, current_date)
    and ("is_true" is null or "is_true" = true)
    and "$part_date" is not null
    and "#account_id" in (select distinct "#account_id" from payment_aux)
group by 1,2 -- order by 2 desc
)

, time_zone_mapping_tb as (
select "#account_id", "#zone_offset" 
from time_zone_mapping_tb_aux 
where zone_size_rank = 1
)

, ios_payments as (
select    a."#account_id"
    , b."register_time"
    , min(a."#event_time") over(partition by a."#account_id")                                          as first_payment
    , min(c."#event_time") over(partition by a."#account_id")                                          as first_dispute
    , date_add('second', CAST(((0 - a."#zone_offset") * 3600) as integer), a."#event_time")            as "pay_local_time" 
    -- , if(extract(hour from date_add('second', CAST(((0 - a."#zone_offset") * 3600) AS integer), a."#event_time")))            as "pay_local_time"
    , if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), a."#event_time"))
                            < 6, 1, 0)                                                                 as local_time_before_6
    , if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), a."#event_time"))
                            between 6 and 12, 1, 0)                                                    as local_time_6_to_12 
    , if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), a."#event_time"))
                            between 12 and 18, 1, 0)                                                   as local_time_12_to_18

    , if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), a."#event_time"))
                            > 18, 1, 0)                                                                as local_time_after_18

    , json_extract_scalar(cast(a."ip_info" as json),'$.province')                                      as province
    , a."pay_id"    , a."#event_time"                                                                                  as "pay_time"
    , c."#event_time"                                                                                  as "dispute_time"
    , a."pay_amount"    , floor(date_diff('minute', "register_time", a."#event_time")/(24*60))                             as matuirity_day

from payment_aux as a
  join ta.v_user_59 as b                                                                                          on a."#account_id" = b."#account_id"   and  a."#event_time" > b."register_time"
  left join (select * from ta.v_event_59 where "$part_event" = 'pay_dispute' and "$part_date" is not null) as c   on a.pay_id        = c.pay_id          and  a."#account_id" = c."#account_id"
  left join time_zone_mapping_tb tz on a."#account_id" = tz."#account_id"
where cast(date_format(b."register_time", '%Y-%m-%d') as varchar) > '2023-12-01'
  and b."register_time" < date_add('day', -37, current_date)
  and b."bundle_id" = 'com.acorncasino.slots')

, activity as (
select    a."#account_id"                                                                                    as act_account_id
        , floor(date_diff('minute', "register_time", a."#event_time")/(24*60))                         as act_matuirity_day
    , min(a."#event_time")                                                                             as first_game
    , count(distinct "spin_id")                                                                        as games_played
    , count(distinct if("bet_chips"  > 0, "spin_id", null))                                            as chip_games -- This might be converted into a sum of 1s
    , count(distinct if("bet_money"  > 0, "spin_id", null))                                            as money_games -- This might be converted into a sum of 1s
    , count(distinct if("bet_money"  > 0 and "win_amount" > 0, "spin_id", null))                       as money_games_wins
    , min("#event_time")                                                                               as started_playing
    , max("#event_time")                                                                               as finished_playing
    , count(distinct
       if(extract(hour from "#event_time") between 9 and 17 and "bet_money"  > 0, "spin_id", null))    as money_games_working_hours
    , count(distinct
       if(extract(hour from "#event_time") between 6 and 23 and "bet_money"  > 0, null, "spin_id"))    as money_games_sleeptime
    , count(distinct
       if(dayofweek(date_format("#event_time",'%Y-%m-%d')) > 5 and "bet_money" > 0, null, "spin_id"))  as money_games_weekend
       
    , sum(if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), a."#event_time"))
                            < 6, 1, 0))                                                                as games_local_time_before_6
    , sum(if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), "#event_time"))
                            between 6 and 12, 1, 0))                                                   as games_local_time_6_to_12 
    , sum(if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), "#event_time"))
                            between 12 and 18, 1, 0))                                                  as games_local_time_12_to_18

    , sum(if(extract(hour from date_add('hour'
                            , cast(((0 - tz."#zone_offset")) as integer), "#event_time"))
                            > 18, 1, 0))                                                               as games_local_time_after_18
    , sum("bet_money")                                                                                 as bet_money
    , max("bet_money")                                                                                 as max_bet
    , sum(if("bet_money" > 0, "win_amount", 0))                                                        as money_win

from ta.v_event_59 as a
  join ta.v_user_59 as b
        on a."#account_id" = b."#account_id"   and  a."#event_time" > b."register_time"
  left join time_zone_mapping_tb tz on a."#account_id" = tz."#account_id"
where a."$part_event" = 'game_play'
   and a."$part_date" is not null
   and cast(date_format(b."register_time", '%Y-%m-%d') as varchar) > '2023-12-01'
   and a."#event_time" < date_add('day', -30, current_date)
   and b."bundle_id" = 'com.acorncasino.slots'
  and a."#account_id" in (select distinct "#account_id" from payment_aux)
 group by 1,2
 )
 
, cash_withdrawals_success as (
select "#account_id","withdraw_id","#event_time","amount","withdraw_fee"
from v_event_59 where "$part_event"='withdraw_success' and "$part_date" is not null and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2023-12-01')

, cash_withdrawals_applied as (
select "#account_id","withdraw_id","#event_time","amount","withdraw_fee"
from v_event_59 where "$part_event"='withdraw_apply' and "$part_date" is not null and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2023-12-01')

, withdrawals_aux as (
select
 a."#account_id"
 , a."withdraw_id"
 , a."#event_time"                                                                                     as withdrawal_apply_time
 , a."amount" - a."withdraw_fee"                                                                       as withdrawal_amount
 , b."#event_time"                                                                                     as ws_t
 , b."amount" - b."withdraw_fee"                                                                       as withdrawal_succes_amount
 , b."amount"                                                                                          as wa
from cash_withdrawals_applied a
  left join cash_withdrawals_success b on a."withdraw_id" = b."withdraw_id" and a."#account_id" = b."#account_id"
  )

, withdrawals as (
select    a."#account_id"                                                                                    as w_account_id
    , floor(date_diff('minute', "register_time", withdrawal_apply_time)/(24*60))                       as w_matuirity_day
    , min(withdrawal_apply_time)                                                                       as first_withdrawn_attempt
    , count(distinct withdrawal_apply_time)                                                            as count_withdraw_apply
    , sum(withdrawal_amount)                                                                           as total_withdrawn_applied
from withdrawals_aux as a
  join ta.v_user_59 as b
        on a."#account_id" = b."#account_id" and a.withdrawal_apply_time > b."register_time"
where cast(date_format(b."register_time", '%Y-%m-%d') as varchar) > '2023-12-01'
   and b."bundle_id" = 'com.acorncasino.slots'
 group by 1,2
 )

, base as (
    select *    from (select matuirity_day from ios_payments group by 1)
        cross join (select "#account_id","register_time" FROM ios_payments where cast(date_format("register_time", '%Y-%m-%d') as varchar) > '2023-11-01' group by 1,2) )

, final_out_pre_filter as (
select
    -- IMPORTANT NOTE: VALID DISPUTES ARE NOT THE SAME MEASURE, SINCE ONLY TAKE INTO ACCOUNT DISPUTES OF THE CURRENT PAYMENTS. HOWEVER DISPUTER, IT CAN ACCEPT NEW PAYMENTS.
    -- NOTE II: YOU NEED TO REMOVE DISPUTES OCCURRING BEFORE THE DATE
    base."#account_id"
    , base."register_time"
    , base.matuirity_day
    , date_add('day', cast(base.matuirity_day + 1 as bigint), base."register_time")                                                                     as limit_time
    , base.matuirity_day - ceiling(date_diff('day', base."register_time", max(first_payment) over (partition by base."#account_id")))                   as paying_matuirity
    , sum(if(pay_amount > 0, 1, 0))                                                                                                                     as new_payments
    , sum(if("dispute_time" is null, 0, pay_amount))                                                                                                    as disputed_payments
    , coalesce(sum(pay_amount), 0)                                                                                                                      as spent
    , min("pay_time")                                                                                                                                   as first_pay_tmp
    , min("dispute_time")                                                                                                                               as first_dispute_tmp
    , coalesce(max(pay_amount), 0)                                                                                                                      as highest_payment
    , max(extract(hour from pay_time))                                                                                                                  as last_payment_time
    , sum(if(extract(hour from "pay_time") between 9 and 17 and "pay_amount" > 0, 1, 0))                                                                as payments_working_hours
    , sum(if(extract(hour from "pay_time") between 6 and 23 and "pay_amount" > 0, 1, 0))                                                                as payments_sleeptime
    , sum(if(dayofweek(date_format("pay_time",'%Y-%m-%d')) > 5 and "pay_amount" > 0, 1, 0))                                                             as payments_weekend
    , sum(local_time_before_6)                                                                                                                          as local_time_before_6
    , sum(local_time_6_to_12)                                                                                                                           as local_time_6_to_12
    , sum(local_time_12_to_18)                                                                                                                          as local_time_12_to_18
    , sum(local_time_after_18)                                                                                                                          as local_time_after_18

from base
    left join ios_payments
        on base."#account_id" = ios_payments."#account_id"
        and base.matuirity_day = ios_payments.matuirity_day
    where date_add('day', cast(base.matuirity_day + 30 as bigint), cast(base."register_time" as date)) < current_date  -- I only want to consider dates that ocurred in the last 30 days
group by 1, 2, base.matuirity_day, first_payment
) 

, output_aux_2 as (
select
    -- final_out_pre_filter.*
    -- Payment
    "#account_id"
    , "register_time"
    , matuirity_day
    , paying_matuirity
    , date_diff('minute', "register_time", (min(first_game) over(partition by "#account_id")))                                                          as minutes_to_play
    , date_diff('minute', "register_time", (min(first_pay_tmp) over(partition by "#account_id")))                                                       as minutes_to_payment
    , date_diff('minute', "register_time", (min(first_pay_tmp) over(partition by "#account_id")))
        - date_diff('minute', "register_time", (min(first_game) over(partition by "#account_id")))                                                      as minutes_to_pay_since_first_game
    , matuirity_day - (date_diff('day', "register_time", (min(first_pay_tmp) over(partition by "#account_id"))))                                        as pay_matuirity

    , sum(new_payments) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                       as payments_to_date
    , sum(new_payments) over (partition by "#account_id" order by matuirity_day rows between 14 preceding and current row)                              as payments_last_14d
    , sum(new_payments) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row)                               as payments_last_7d
    , sum(new_payments) over (partition by "#account_id" order by matuirity_day rows between 3 preceding and current row)                               as payments_last_3d
    , sum(spent) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                              as spent_to_date
    , sum(spent) over (partition by "#account_id" order by matuirity_day rows between 14 preceding and current row)                                     as spent_last_14d
    , sum(spent) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row)                                      as spent_last_7d
    , sum(spent) over (partition by "#account_id" order by matuirity_day rows between 3 preceding and current row)                                      as spent_last_3d

    , round(avg(spent) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row) , 2)                   as daily_spent_to_date
    , round(avg(spent) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row)         , 2)                   as daily_spent_last_7_d
    , round(avg(spent) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and 13 following), 2)                   as daily_spent_first_14d
    , round(avg(spent) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and 7 following) , 2)                   as daily_spent_first_7d
    , round(avg(spent) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and 0 following) , 2)                   as daily_spent_first_1d

    , max(spent) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                              as max_daily_spent
    , max(highest_payment) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                    as max_payment
    -- , round(stddev(highest_payment) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row), 3)       as daily_pay_std
    , array_agg(if(new_payments > 0, 1, 0)) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)   as daily_payment_history_binary
    , array_agg(new_payments) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                 as daily_payment_history_count -- You can find the most common
    , sum(games_played) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                       as games_to_date
    , sum(games_played) over (partition by "#account_id" order by matuirity_day rows between 14 preceding and current row)                              as games_last_14d
    , sum(games_played) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row)                               as games_last_7d
    , max(games_played) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                       as max_games_daily_games

    , round(avg(games_played) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and 13 following), 2)            as daily_games_first_14d
    , round(avg(games_played) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and 7 following) , 2)            as daily_games_first_7d
    , round(avg(games_played) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and 0 following) , 2)            as daily_games_first_1d

    , array_agg(if(games_played > 0, 1, 0)) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)   as retention_sequence
    , array_agg(coalesce(games_played, 0)) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)    as games_sequence
    -- , array_agg(coalesce(bet_money, 0)) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)       as money_bet_sequence
    , array_agg(if(bet_money > 0, 1, 0)) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)      as days_betting_money_sequence
    , array_agg(if(bet_money > 50, 1, 0)) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)     as days_50_usd_bet_money_sequence

    , sum(money_games) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                        as money_games_to_date
    , sum(money_games) over (partition by "#account_id" order by matuirity_day rows between 14 preceding and current row)                               as money_games_last_14d
    , sum(money_games) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row)                                as money_games_last_7d

    , round(sum(bet_money) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row), 1)                as bet_money_to_date
    , round(sum(bet_money) over (partition by "#account_id" order by matuirity_day rows between 14 preceding and current row), 1)                       as bet_money_last_14d
    , round(sum(bet_money) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row), 1)                        as bet_money_last_7d
    , round(sum(money_win) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row), 2)                as win_amount_to_date
    , round(sum(money_win) over (partition by "#account_id" order by matuirity_day rows between 14 preceding and current row), 2)                       as win_amount_last_14d
    , round(sum(money_win) over (partition by "#account_id" order by matuirity_day rows between 7 preceding and current row), 2)                        as win_amount_last_7d
    , max(max_bet)   over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                          as max_bet
    -- , sum(payments_working_hours) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)             as working_hours_payments_to_date
    -- , sum(payments_sleeptime) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                 as sleep_hours_payments_to_date
    -- , sum(payments_weekend) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                   as weekend_payments_to_date
    -- , sum(money_games_working_hours) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)          as working_hours_bets_to_date
    -- , sum(money_games_sleeptime) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)              as sleep_hours_bets_to_date
    -- , sum(money_games_weekend) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                as weekend_bets_to_date

    -- , sum(count_withdraw_apply) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)               as withdrawn_attempt_to_date
    -- , sum(total_withdrawn_applied) over (partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)            as withdrawn_attempted_ammount_to_date
    -- , date_diff('minute'
    --     , min(first_pay_tmp) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)
    --     , min(first_withdrawn_attempt) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row))        as minutes_to_withdraw_since_first_payment
    , if(date_diff('minute', limit_time, min(first_dispute_tmp) over(partition by "#account_id")) < 0, 1, 0)                                            as past_disputer -- Describes any dispute made between the d21 and d 51 + ealier disputes
    , if(date_diff('minute', limit_time, min(first_dispute_tmp) over(partition by "#account_id")) between 0 and 30*24*60, 1, 0)                         as new_disputer_30d -- Describes any dispute made between the d21 and d 51 + ealier disputes
    -- , sum(local_time_before_6) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                 as local_time_before_6
    -- , sum(local_time_6_to_12)  over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                 as local_time_6_to_12
    -- , sum(local_time_12_to_18) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                 as local_time_12_to_18
    -- , sum(local_time_after_18) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)                 as local_time_after_18   
    , sum(games_local_time_before_6) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)           as local_time_before_6
    , sum(games_local_time_6_to_12)  over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)           as local_time_6_to_12
    , sum(games_local_time_12_to_18) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)           as local_time_12_to_18
    , sum(games_local_time_after_18) over(partition by "#account_id" order by matuirity_day rows between unbounded preceding and current row)           as local_time_after_18    -- , date_diff('minute', "register_time", (min(first_customer_service_reach) over(partition by "#account_id")))                                        as time_to_reach_customer_service
    -- , if(min(first_customer_service_reach) over(partition by "#account_id") > min(first_pay_tmp) over(partition by "#account_id"), 1, 0)                as reach_customer_service_after_payment
    -- , if(min(first_customer_service_reach) over(partition by "#account_id") is not null, 1, 0)                                                          as reach_customer_service_at_all 

from final_out_pre_filter
    left join activity
        on final_out_pre_filter."#account_id" = activity.act_account_id and final_out_pre_filter.matuirity_day = activity.act_matuirity_day
    left join withdrawals as w
        on final_out_pre_filter."#account_id" = w.w_account_id          and final_out_pre_filter.matuirity_day = w.w_matuirity_day

)

, user_countries as (
select
    "#account_id"
    -- , province
from ios_payments 
where "#account_id" in (select distinct "#account_id" from output_aux_2)
group by 1
)

select
    a.*
from output_aux_2 as a
where spent_to_date > 0
    and matuirity_day < 7    and past_disputer = 0
    -- and cast(date_format("register_time", '%Y-%m-%d') as varchar) > '2024-03-10'

    
