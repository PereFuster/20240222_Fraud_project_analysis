

with payment_aux as (
select 
    *
    , min("#event_time") over (partition by "#account_id") as first_pay_time
from ta.v_event_59
where "$part_event" = 'order_pay'
    -- and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2024-01-01'
    and ("is_true" is null or "is_true" = true)
    and "#event_time" between cast('2024-02-01' as date) and date_add('day', -30, current_date)
    and "$part_date" is not null
 )
 
 
, ios_payments as (
select    
    a."#account_id"
    , a."first_pay_time"
    , sum(case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then cast(a."net_amount" as double) else 0 end)                             as payment_d7
    , sum(case when date_diff('minute', a."first_pay_time", c."#event_time") <= 37*24*60 then 1 else 0 end)                                                         as disputes_d30
    -- , cast(ntile(100) over (order by 
    --         sum(case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then cast(a."net_amount" as double) else 0 end)) as double) /100      as monetary_consumption_score
    , sum(case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then cast(a."net_amount" as double) else 0 end)                             as monetary_consumption_score
    -- , cast(ntile(20) over (order by 
    --         count(distinct case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then a."payment_type" else null end)) as double) /100      as payment_methods_score
    , count(distinct case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then a."payment_type" else null end)                             as payment_methods_score
    -- , cast(ntile(100) over (order by 
    --         stddev(case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then cast(a."net_amount" as double) else 0 end)) as double) /100   as payment_impulsiveness_score
    , stddev(case when date_diff('minute', a."first_pay_time", a."#event_time") <= 7*24*60 then cast(a."net_amount" as double) else 0 end)                          as payment_impulsiveness_score
from payment_aux as a
  join ta.v_user_59 as b                                                                                          on a."#account_id" = b."#account_id"   and  a."#event_time" > b."register_time"
  left join (select * from ta.v_event_59 where "$part_event" = 'pay_dispute' and "$part_date" >= '2024-02-01' and "#event_time" >= cast('2024-02-01' as date)) as c   on a.pay_id        = c.pay_id          and  a."#account_id" = c."#account_id"
where b."register_time" >= cast('2024-02-01' as date)
  and a."first_pay_time" <  date_add('day', -37, current_date)
--   and a."#event_time"   < date_add('day', -7, current_date)
  and b."bundle_id" = 'com.acorncasino.slots'
  and (a.first_pay_time < date_add('day', -7, c."#event_time") or c."#event_time" is null) -- Exclude people already disputing in 7 days (You can ignore this)
 group by 1,2
  )
  
--   , time_zone_mapping_tb_aux as (
-- select 
--     "#account_id"
--     , "#zone_offset"
--     , count(*)                                                                  as total_users
--     , row_number() over (partition by "#account_id" order by count(*) desc)     as zone_size_rank
-- from ta.v_event_59
-- where "$part_event" = 'ta_app_start'
--     and cast(date_format("#event_time", '%Y-%m-%d') as varchar) >= '2024-01-01'
--     and "#event_time" < date_add('day', -30, current_date)
--     and ("is_true" is null or "is_true" = true)
--     and "$part_date" is not null
--     and "#account_id" in (select "#account_id" from ios_payments)
-- group by 1,2
-- )

-- , time_zone_mapping_tb as (
-- select "#account_id", "#zone_offset" 
-- from time_zone_mapping_tb_aux 
-- where zone_size_rank = 1
-- )

, tab_2 as (
select
    a."#account_id"
    , b."first_pay_time"
    , disputes_d30
    , monetary_consumption_score
    , payment_methods_score
    , payment_impulsiveness_score
    -- ,  cast(ntile(10) over(order by count(distinct 
    --         case when date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60 then "machine_id" else null end)) as double)/10                          as game_types_score 
    , count(distinct case when date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60 then "machine_id" else null end)                                as game_types_score 
            
    -- ,  cast(ntile(7) over(order by count(distinct 
    --         case when date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60 
    --                 then cast(date_format("#event_time", '%Y-%m-%d') as varchar) else null end)) as double)/10                                                     as days_played_score 
    ,  count(distinct 
            case when date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60 
                    then cast(date_format("#event_time", '%Y-%m-%d') as varchar) else null end)                                                                    as days_played_score 
    
    -- , round(sum(if(extract(hour from date_add('hour'
    --     , cast(((0 - tz."#zone_offset")) as integer), a."#event_time")) 
    --         <= 6 or date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60, 1.0, 0.0)) / sum(if(date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60, 1.0, 0.0)), 1)                                                                                                        as perc_night_games
    
    , 1 as perc_night_games

    , round(sum(if(date_diff('minute', b."first_pay_time", a."#event_time") between 7*24*60 and 14*24*60
              , "bet_money", 0))
      / sum(if(date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60
               , "bet_money", null)), 1)                                                                                                                           as increasing_tolerance
               
from ta.v_event_59 a 
    right join ios_payments b on a."#account_id" = b."#account_id"
    -- left join time_zone_mapping_tb tz on a."#account_id" = tz."#account_id"
where a."$part_event" = 'game_play'
  and a."$part_date" is not null
group by 1,2,3,4,5,6
) 

-- select *, if(is_nan(increasing_tolerance), null, increasing_tolerance) as increasing_tolerance from tab_2 limit 10000

, tab_3_aux as (
select 
    tab_2."#account_id"
    , "first_pay_time"
    , disputes_d30
    , monetary_consumption_score
    , payment_methods_score
    , payment_impulsiveness_score
    , game_types_score
    , days_played_score
    , perc_night_games
    , if(is_nan(increasing_tolerance) or is_infinite(increasing_tolerance), null, increasing_tolerance)                                             as increasing_tolerance
    , date_diff('second', a."#event_time", b."#event_time")                                                                                         as playtime
    , cast(ntile(20) over(order by  date_diff('second', a."#event_time", b."#event_time")) as double)                                               as playtime_bin
    , max(if(a."win_amount" - a."bet_money" > 0, null, a."#event_time")) over (partition by a."#account_id", a."machine_access_id"
        order by a."#event_time" rows between unbounded preceding and current row)                                                                  as "last_loss"
    , a."machine_access_id"
    , a."#event_time"
    , a."win_amount"    
    , a."bet_money" 
from (select "#account_id", "machine_access_id", "#event_time", "win_amount", "bet_money" from v_event_59 WHERE "$part_event"='game_play' AND "$part_date" > '2024-02-01') as a 
    join (select "#account_id", "machine_access_id", "#event_time" from v_event_59 WHERE "$part_event"='game_result' AND "$part_date"> '2024-02-01') as b
        on a."#account_id" = b."#account_id" and a."machine_access_id" = b."machine_access_id"
    right join tab_2 
        on a."#account_id" = tab_2."#account_id" 
        -- and tab_2."first_pay_time" < date_add('minute', -(7*24*60), b."#event_time")
        and b."#event_time" between tab_2."first_pay_time" and date_add('minute', 7*24*60, tab_2."first_pay_time")
)

, babuino_aux as (  
select  
    *   
    , "win_amount" - "bet_money"                                                                                                                                                                                        as net_win
    -- , sum(if("win_amount" between "last_win_time" and "#event_time"))    
    , sum(if("#event_time" > "last_loss", 1, 0)) over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between unbounded preceding and current row)                                         as win_streak_size
    , round(sum(if("#event_time" > "last_loss", "win_amount" - "bet_money", 0)) over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between unbounded preceding and current row), 2)      as win_streak_value
    -- , sum(if("#event_time" > "last_loss" and "win_amount" > 0, 1, 0)) over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between unbounded preceding and current row) a   s verification
    , round(sum("win_amount" - "bet_money") over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between 1 following and unbounded following), 2)                                          as subsequent_profit
    , 1 - (sum(if("#event_time" > "last_loss", "win_amount" - "bet_money", 0)) over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between unbounded preceding and current row) 
    + sum("win_amount" - "bet_money") over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between 1 following and unbounded following)) 
    / sum(if("#event_time" > "last_loss", "win_amount" - "bet_money", 0)) over (partition by "#account_id", "machine_access_id" order by "#event_time" rows between unbounded preceding and current row)                as percentual_loss
from tab_3_aux 
-- where "win_amount" > 0 
)

, tab_3 as (
select 
    "#account_id"
    , "first_pay_time"
    , disputes_d30
    , monetary_consumption_score
    , game_types_score
    , days_played_score
    , payment_methods_score
    , payment_impulsiveness_score
    , perc_night_games
    , increasing_tolerance
    , if(sum(if(playtime > 150, 1, 0)) = 0, 0, cast(ntile(100) over(order by sum(if(playtime > 180, 1, 0))) as double)/100)                                             as long_sessions_score
    , sum(if(playtime > 150, 1, 0))                                                                                                                                     as long_sessions_score_2_30
    , sum(if(playtime > 180, 1, 0))                                                                                                                                     as long_sessions_score_3_30
    , sum(if(playtime > 210, 1, 0))                                                                                                                                     as long_sessions_score_2_30
    , sum(if(playtime > 240, 1, 0))                                                                                                                                     as long_sessions_score_4
    , sum(if(percentual_loss > 0.5, 1, 0))                                                                                                                              as losses_of_winning
    , count(*)                                                                                                                                                          as cases  
    , cast(sum(if(percentual_loss > 0.5 and "win_amount" - "bet_money" > 10, 1.0, 0)) as double)/cast(sum(if("win_amount" - "bet_money" > 10, 1.0, null)) as double)    as losses_of_winning_score
from babuino_aux 
group by 1,2,3,4,5,6,7,8,9,10) 

-- select * from tab_3 limit 55
,  cash_withdrawals_applied as (
select "#account_id","withdraw_id","#event_time","amount","withdraw_fee"
from v_event_59 where "$part_event"='withdraw_apply' and "$part_date" is not null and cast(date_format("#event_time", '%Y-%m-%d') as varchar) > '2024-02-01')

, tab_4 as (
select    
    tab_3. "#account_id"
    , "first_pay_time"
    , disputes_d30
    , monetary_consumption_score
    , payment_methods_score
    , payment_impulsiveness_score
    , game_types_score
    , days_played_score
    , long_sessions_score
    , perc_night_games
    , increasing_tolerance
    , losses_of_winning_score
    , if(sum("amount") > 0, 1, 0)                                                             as user_withdrawed
from tab_3 
    left join cash_withdrawals_applied
      on cash_withdrawals_applied."#account_id" = tab_3."#account_id" 
    --   and tab_3."first_pay_time" < date_add('minute', -7*24*60, cash_withdrawals_applied."#event_time")
      and cash_withdrawals_applied."#event_time" between tab_3."first_pay_time" and date_add('minute', 7*24*60, tab_3."first_pay_time")
 group by 1,2,3,4,5,6,7,8,9,10,11,12
 )

 , tab_5 as (
select 
    tab_4. "#account_id"
    , "first_pay_time"
    , disputes_d30
    , monetary_consumption_score
    , payment_methods_score
    , payment_impulsiveness_score
    , game_types_score
    , days_played_score
    , long_sessions_score
    , user_withdrawed
    , perc_night_games
    , increasing_tolerance
    , losses_of_winning_score
    , round(stddev(if("bet_money" > 0, "bet_money", null)) / sum("bet_money"), 2) as bet_volatility
from tab_4 
    left join (select * from v_event_59 where "$part_event" = 'game_play' and "$part_date" is not null) tab_5_aux
        on tab_4."#account_id" = tab_5_aux."#account_id" 
        and tab_5_aux."#event_time" between tab_4."first_pay_time" and date_add('minute', 7*24*60, tab_4."first_pay_time")
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

, tab_6_aux as (
select 
    tab_5. "#account_id"
    , "machine_access_id"
    , "first_pay_time"
    , disputes_d30
    , monetary_consumption_score
    , payment_methods_score
    , payment_impulsiveness_score
    , game_types_score
    , days_played_score
    , long_sessions_score
    , user_withdrawed
    , bet_volatility
    , perc_night_games
    , increasing_tolerance
    , losses_of_winning_score
    -- , tab_6_aux."#event_time" -- doushi meiyou ne
    , min(tab_6_aux."#event_time")
    , max(tab_6_aux."#event_time")
    , date_diff('second', min(tab_6_aux."#event_time"), max(tab_6_aux."#event_time"))      as playtime
from tab_5
 left join (select "#account_id", "#event_time", "machine_access_id" from v_event_59 where "$part_event" in ('game_start', 'game_result') and "$part_date" >= '2024-02-01') tab_6_aux
     on tab_5."#account_id" = tab_6_aux."#account_id" 
    --  and tab_5."first_pay_time" < date_add('minute', -7*24*60, tab_6_aux."#event_time")
     and tab_6_aux."#event_time" between tab_5."first_pay_time" and date_add('minute', 7*24*60, tab_5."first_pay_time")

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

, output as (
select 
    "#account_id"
    -- , "first_pay_time"
    , disputes_d30
    , game_types_score          as game_types
    -- Increasing tolerance
    -- Repeated loss of winning
    , long_sessions_score       as binge_gaming
    -- Loss chasing 
    , monetary_consumption_score
    , user_withdrawed  
    , bet_volatility            as fluctuating_wagers
    , payment_methods_score
    , payment_impulsiveness_score
    , perc_night_games          as nightly_play
    , days_played_score
    , increasing_tolerance
    , losses_of_winning_score
    , sum(playtime)             as time_comsumption
from tab_6_aux 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)

select
    *
    -- if(disputes_d30 > 1, 'Disputers', 'Non disputers')  as disputers
    -- , count(*)                                          as users
    -- , count(distinct "#account_id")                     as users_verification
    -- , avg(game_types)                                   as game_types
    -- , avg(binge_gaming)                                 as binge_gaming
    -- , avg(monetary_consumption_score)                   as monetary_consumption_score 
    -- , avg(user_withdrawed)                              as user_withdrawed   
    -- , avg(fluctuating_wagers)                           as fluctuating_wagers 
    -- , avg(payment_methods_score)                        as payment_methods_score
    -- , avg(payment_impulsiveness_score)                  as payment_impulsiveness_score
    -- , avg(nightly_play)                                 as nightly_play
    -- , avg(days_played_score)                            as days_played_score
    -- , avg(increasing_tolerance)                         as increasing_tolerance
    -- , avg(losses_of_winning_score)                      as losses_of_winning_score
    -- , avg(time_comsumption)                             as time_comsumption
from output  
-- group by 1   -- This just make sure it is within 7 days from first payment then just aggregated by is disputer and fuck it 


-- , aux_tab_3 as (
-- select  
--     a."#account_id"
--     , a."spin_id"
--     , a."machine_access_id"
--     , a."#event_time"                                            as win_occurs
--     , b."#event_time"                                            as end_game
--     , date_diff('second', a."#event_time", b."#event_time")      as playtime
--     , a."bet_money"
--     , a."win_amount"
--     -- This is the number of metrics calculated within a certain amout of time. 
--     , (sum(a."win_amount" - a."bet_money") over(partition by 
--                 a."#account_id", a."machine_access_id" order by a."#event_time" desc) 
--           - (a."bet_money" - a."win_amount")) / (a."bet_money" - a."win_amount") 
--           as future_win_money_machine_rel
    
-- from (select * from v_event_59 WHERE "$part_event"='game_play' AND "$part_date"='2024-05-17') as a 
--     join (select * from v_event_59 WHERE "$part_event"='game_result' AND "$part_date"='2024-05-17') as b
--     on a."#account_id" = b."#account_id" and a."machine_access_id" = b."machine_access_id"
--     where a."$part_event" = 'game_play'
--         and a."$part_date" is not null
--         and a."win_amount" - a."bet_money" > 10 and a."bet_money" > 0
-- ) 

-- select
--     a."#account_id"
--     , b."first_pay_time"
--     ,monetary_consumption_score
--     , game_types_score
           
           
--     , sum(a."win_amount" - a."bet_money") over(partition by a."#account_id", a."machine_access_id" order by a."win_occurs" desc) as future_win_money_machine_rel
--         --  / if(
--         --         date_diff('minute', b."first_pay_time", a."#event_time") <= 7*24*60
--         --         , a."win_amount" - a."bet_money", null))                                                    
                
-- from aux_tab_3 a 
--     join tab_2 b on a."#account_id" = b."#account_id"
-- where date_diff('minute', b."first_pay_time", a."win_occurs") <= 7*24*60
-- -- group by 1,2,3,4, a."machine_access_id", a."win_occurs"  


        -- , if(date_diff('minute', b."first_pay_time", a."win_occurs") <= 7*24*60, a."win_amount" - a."bet_money", null)
-- limit 10000

-- select * 
-- from ios_payments 
-- limit 10000
