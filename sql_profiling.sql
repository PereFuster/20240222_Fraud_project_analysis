WITH
  payment_aux AS (
   SELECT
     "#account_id", "#event_time", "net_amount", "payment_type", pay_id
   , min("#event_time") OVER (PARTITION BY "#account_id") first_pay_time
   FROM
     ta.v_event_59
   WHERE (("$part_event" = 'order_pay') AND (("is_true" IS NULL) OR ("is_true" = true)) AND (date("#event_time") BETWEEN CAST('2023-12-01' AS date) AND date_add('day', -31, current_date)) 
        AND ${PartDate:date1} and payment_type in ('checkout_credit_api', 'checkout_apple_pay', 'paypal_wallet_slave'))
) 

-- payment_type in ('checkout_credit_api', 'checkout_apple_pay', 'paypal_wallet_slave') and "$part_event" in ('pay_dispute', 'fraud')
, ios_payments AS (
   SELECT
     a."#account_id"
   , a."first_pay_time"
   , b."bundle_id"
   , sum((CASE WHEN (date_diff('minute', a."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN CAST(a."net_amount" AS double) ELSE 0 END))     payment_d7
   , sum((CASE WHEN (date_diff('minute', a."first_pay_time", c."#event_time") <= ((37 * 24) * 60)) THEN 1 ELSE 0 END))                                 disputes_d30
   , sum((CASE WHEN (date_diff('minute', a."first_pay_time", c."#event_time") <= ((37 * 24) * 60)) THEN  CAST(a."net_amount" AS double) ELSE 0.0 END)) disputed_d30
  
   , coalesce(
        sum((CASE WHEN (date_diff('minute', a."first_pay_time", first_dispute_time) >= 0) THEN  CAST(a."net_amount" AS double) ELSE 0.0 END))
        , sum((CASE WHEN (date_diff('minute', a."first_pay_time", a."#event_time") <= 37*24*60) THEN CAST(a."net_amount" AS double) ELSE 0 END)) )     disputable_payment

    
   -- Find the moment of the first dispute and then get the payment untill then
   
   , sum((CASE WHEN (date_diff('minute', a."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN CAST(a."net_amount" AS double) ELSE 0 END))    monetary_consumption_score
   , count(DISTINCT (CASE WHEN (date_diff('minute', a."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN a."payment_type" ELSE null END))    payment_methods_score
  , array_agg(DISTINCT (CASE WHEN (date_diff('minute', a."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN a."payment_type" ELSE null END)) payment_methods_sequence
   , stddev((CASE WHEN (date_diff('minute', a."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN CAST(a."net_amount" AS double) ELSE 0 END)) payment_impulsiveness_score
   FROM
     ((payment_aux a
   INNER JOIN ta.v_user_59 b ON ((a."#account_id" = b."#account_id") AND (a."#event_time" > b."register_time")))
   LEFT JOIN (
      SELECT *, min("#event_time") over (partition by "#account_id") as first_dispute_time 
      FROM
        ta.v_event_59
      WHERE (("$part_event" in ('pay_dispute', 'fraud')) AND ${PartDate:date1})
   )  c ON ((a.pay_id = c.pay_id) AND (a."#account_id" = c."#account_id")))
   WHERE 
    (("register_time" >= cast('2023-12-01' as date)) -- register date on december of last year or after
    AND (a."first_pay_time" < date_add('day', -37, current_date)) -- Make sure there is enough bandwidth 
    AND (b."bundle_id" in ('com.acorncasino.slots', 'com.asselin.luckylegends', 'com.evl.woc')) -- bundle 
    AND ((a.first_pay_time < date_add('day', -7, c."#event_time")) OR (c."#event_time" IS NULL))) -- make sure either there is no dispute ro there has not been a dispute in 7 days since the first payment 
   GROUP BY 1, 2, 3
) 


, tab_2 AS (
  SELECT
     a."#account_id"
  , b."first_pay_time"
  , b."bundle_id"
  , payment_methods_sequence
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , monetary_consumption_score
  , payment_methods_score
  , payment_impulsiveness_score
  , count(DISTINCT (CASE WHEN (date_diff('minute', b."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN "machine_id" ELSE null END)) game_types_score
  , count(DISTINCT (CASE WHEN (date_diff('minute', b."first_pay_time", a."#event_time") <= ((7 * 24) * 60)) THEN CAST(date_format("#event_time", '%Y-%m-%d') AS varchar) ELSE null END)) days_played_score
  , 1 perc_night_games
  , round((sum(IF((date_diff('minute', b."first_pay_time", a."#event_time") BETWEEN ((7 * 24) * 60) AND ((14 * 24) * 60)), "bet_money", 0)) / sum(IF((date_diff('minute', b."first_pay_time", a."#event_time") <= ((7 * 24) * 60)), "bet_money", null))), 1) increasing_tolerance
  FROM
     (
        select * from ta.v_event_59 where "$part_event" = 'game_play' AND ${PartDate:date1}
        ) a
  RIGHT JOIN ios_payments b ON a."#account_id" = b."#account_id"
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
) 

-- select * from tab_2 limit 40000

, tab_3_aux AS (
  SELECT
     tab_2."#account_id"
  , tab_2."bundle_id"
  , "first_pay_time"
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , monetary_consumption_score
  , payment_methods_score
  , payment_methods_sequence
  , payment_impulsiveness_score
  , game_types_score
  , days_played_score
  , perc_night_games
  , IF((is_nan(increasing_tolerance) OR is_infinite(increasing_tolerance)), null, increasing_tolerance) increasing_tolerance
  , date_diff('second', a."#event_time", b."#event_time") playtime
--   , CAST(ntile(20) OVER (ORDER BY date_diff('second', a."#event_time", b."#event_time") ASC) AS double) playtime_bin
  , max(IF(((a."win_amount" - a."bet_money") > 0), null, a."#event_time")) OVER (PARTITION BY a."#account_id", a."machine_access_id" ORDER BY a."#event_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "last_loss"
  , a."machine_access_id"
  , a."#event_time"
  , a."win_amount"
  , a."bet_money"
  FROM
     (((
      SELECT *
      FROM
        v_event_59
      WHERE (("$part_event" = 'game_play') AND ("$part_date" > '2023-12-01'))
  )  a
  INNER JOIN (
      SELECT *
      FROM
        v_event_59
      WHERE (("$part_event" = 'game_result') AND ("$part_date" > '2023-12-01'))
  )  b ON ((a."#account_id" = b."#account_id") AND (a."machine_access_id" = b."machine_access_id")))
  RIGHT JOIN tab_2 ON ((a."#account_id" = tab_2."#account_id") AND (b."#event_time" BETWEEN tab_2."first_pay_time" AND date_add('minute', ((7 * 24) * 60), tab_2."first_pay_time"))))
) 
, babuino_aux AS (
  SELECT
     *
  , ("win_amount" - "bet_money") net_win
  , sum(IF(("#event_time" > "last_loss"), 1, 0)) OVER (PARTITION BY "#account_id", "machine_access_id" ORDER BY "#event_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) win_streak_size
  , round(sum(IF(("#event_time" > "last_loss"), ("win_amount" - "bet_money"), 0)) OVER (PARTITION BY "#account_id", "machine_access_id" ORDER BY "#event_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) win_streak_value
  , round(sum(("win_amount" - "bet_money")) OVER (PARTITION BY "#account_id", "machine_access_id" ORDER BY "#event_time" ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING), 2) subsequent_profit
  , (1 - ((sum(IF(("#event_time" > "last_loss"), ("win_amount" - "bet_money"), 0)) OVER (PARTITION BY "#account_id", "machine_access_id" ORDER BY "#event_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + sum(("win_amount" - "bet_money")) OVER (PARTITION BY "#account_id", "machine_access_id" ORDER BY "#event_time" ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)) / sum(IF(("#event_time" > "last_loss"), ("win_amount" - "bet_money"), 0)) OVER (PARTITION BY "#account_id", "machine_access_id" ORDER BY "#event_time" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))) percentual_loss
  FROM
     tab_3_aux
) 
, tab_3 AS (
  SELECT
     "#account_id"
  , "first_pay_time"
  , "bundle_id"
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , monetary_consumption_score
  , game_types_score
  , days_played_score
  , payment_methods_score
  , payment_methods_sequence
  , payment_impulsiveness_score
  , perc_night_games
  , increasing_tolerance
--   , IF((sum(IF((playtime > 150), 1, 0)) = 0), 0, (CAST(ntile(100) OVER (ORDER BY sum(IF((playtime > 180), 1, 0)) ASC) AS double) / 100)) as long_sessions_score
--   , IF((sum(IF((playtime > 240), 1, 0)) = 0), 0, (CAST(ntile(100) OVER (ORDER BY sum(IF((playtime > 180), 1, 0)) ASC) AS double) / 100)) as long_sessions_score_2
--   , count(distinct IF((playtime > 150*60), "machine_access_id", null)) as long_sessions_ps_score_150
  , count(distinct IF((playtime > 180*60), "machine_access_id", null)) as long_sessions_ps_score_180
  , count(distinct IF((playtime > 210*60), "machine_access_id", null)) as long_sessions_ps_score_210
  , count(distinct IF((playtime > 240*60), "machine_access_id", null)) as long_sessions_ps_score_240
  , count(distinct IF((percentual_loss > 5E-1), "machine_access_id", null)) as losses_of_winning
  , count(distinct "machine_access_id") cases
  , (CAST(sum(IF(((percentual_loss > 5E-1) AND (("win_amount" - "bet_money") > 10)), 1E0, 0)) AS double) / CAST(sum(IF((("win_amount" - "bet_money") > 10), 1E0, null)) AS double)) losses_of_winning_score
  FROM
     babuino_aux
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
) 
, cash_withdrawals_applied AS (
  SELECT
     "#account_id"
  , "bundle_id"
  , "withdraw_id"
  , "#event_time"
  , "amount"
  , "withdraw_fee"
  FROM
     v_event_59
  WHERE (("$part_event" = 'withdraw_apply') AND (date("$part_date") BETWEEN date('2023-12-01') AND date_add('day', -31, current_date)) AND (CAST(date_format("#event_time", '%Y-%m-%d') AS varchar) > '2023-12-01'))
) 
, tab_4 AS (
  SELECT
     tab_3."#account_id"
  , tab_3."bundle_id"
  , "first_pay_time"
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , monetary_consumption_score
  , payment_methods_score
  , payment_methods_sequence
  , payment_impulsiveness_score
  , game_types_score
  , days_played_score
  , perc_night_games
  , increasing_tolerance
  , losses_of_winning_score
--   , long_sessions_score
--   , long_sessions_score_2
--   , long_sessions_ps_score_150
  , long_sessions_ps_score_180
  , long_sessions_ps_score_210
  , long_sessions_ps_score_240
  , IF((sum("amount") > 0), 1, 0) user_withdrawed
  FROM
     (tab_3
  LEFT JOIN cash_withdrawals_applied ON ((cash_withdrawals_applied."#account_id" = tab_3."#account_id") AND (cash_withdrawals_applied."#event_time" BETWEEN tab_3."first_pay_time" AND date_add('minute', ((7 * 24) * 60), tab_3."first_pay_time"))))
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
) 
, tab_5 AS (
  SELECT
     tab_4."#account_id"
  , "first_pay_time"
  , tab_4."bundle_id"
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , monetary_consumption_score
  , payment_methods_score
  , payment_methods_sequence
  , payment_impulsiveness_score
  , game_types_score
  , days_played_score
--   , long_sessions_score
--   , long_sessions_score_2
--   , long_sessions_ps_score_150
  , long_sessions_ps_score_180
  , long_sessions_ps_score_210
  , long_sessions_ps_score_240
  , user_withdrawed
  , perc_night_games
  , increasing_tolerance
  , losses_of_winning_score
  , round((stddev(IF(("bet_money" > 0), "bet_money", null)) / sum("bet_money")), 2) bet_volatility
  FROM
     (tab_4
  LEFT JOIN (
      SELECT *
      FROM
        v_event_59
      WHERE (("$part_event" = 'game_play') AND (date("$part_date") BETWEEN date('2023-12-01') AND date_add('day', -31, current_date)))
  )  tab_5_aux ON ((tab_4."#account_id" = tab_5_aux."#account_id") AND (tab_5_aux."#event_time" BETWEEN tab_4."first_pay_time" AND date_add('minute', ((7 * 24) * 60), tab_4."first_pay_time"))))
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
) 
, tab_6_aux AS (
  SELECT
     tab_5."#account_id"
  , tab_5."bundle_id"
  , "machine_access_id"
  , "first_pay_time"
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , monetary_consumption_score
  , payment_methods_score
  , payment_methods_sequence
  , payment_impulsiveness_score
  , game_types_score
  , days_played_score
--   , long_sessions_score
--   , long_sessions_score_2
--   , long_sessions_ps_score_150
  , long_sessions_ps_score_180
  , long_sessions_ps_score_210
  , long_sessions_ps_score_240
  , user_withdrawed
  , bet_volatility
  , perc_night_games
  , increasing_tolerance
  , losses_of_winning_score
  , min(tab_6_aux."#event_time")
  , max(tab_6_aux."#event_time")
  , date_diff('second', min(tab_6_aux."#event_time"), max(tab_6_aux."#event_time")) playtime
  FROM
     (tab_5
  LEFT JOIN (
      SELECT
        "#account_id"
      , "#event_time"
      , "machine_access_id"
      FROM
        v_event_59
      WHERE (("$part_event" IN ('game_start', 'game_result')) AND ("$part_date" >= '2023-12-01'))
  )  tab_6_aux ON ((tab_5."#account_id" = tab_6_aux."#account_id") AND (tab_6_aux."#event_time" BETWEEN tab_5."first_pay_time" AND date_add('minute', ((7 * 24) * 60), tab_5."first_pay_time"))))
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
) 

  SELECT
     "#account_id"
  , "first_pay_time"
  , "bundle_id"
  , disputes_d30
  , disputed_d30
  , disputable_payment
  , game_types_score game_types
--   , long_sessions_score as binge_gaming
--   , long_sessions_score_2
--   , long_sessions_ps_score_150 
  , long_sessions_ps_score_180
  , long_sessions_ps_score_210
  , long_sessions_ps_score_240 
  , monetary_consumption_score
  , user_withdrawed
  , bet_volatility fluctuating_wagers
  , payment_methods_score
  , payment_methods_sequence
  , payment_impulsiveness_score
  , perc_night_games nightly_play
  , days_played_score
  , increasing_tolerance
  , losses_of_winning_score
  , sum(playtime) time_comsumption
  FROM
     tab_6_aux
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
