create or replace temporary table ANALYST_SANDBOX.to_eur_exchange_rates_SM as
select *
from (
         with eur_gbp_exchange_rate
                  as (
                 select currency_date,
                        inverse_exchange_rate
                 from reports.DAILY_AGGREGATED_RATES
                 where from_currency_id = 1
             )
         select dar.from_currency_code               as source_currency,
                eer.currency_date,
                eer.inverse_exchange_rate * dar.rate as exch_rate
         from reports.DAILY_AGGREGATED_RATES dar
                  left join eur_gbp_exchange_rate eer on eer.currency_date = dar.currency_date
     ) ;

--********************************************************************************************************************--
--PERSONAL_PAYMENT recalculation
--********************************************************************************************************************--
drop table if exists ANALYST_SANDBOX.invoice_value_entitlement_recalculation_SM;
create or replace transient table ANALYST_SANDBOX.invoice_value_entitlement_recalculation_SM as
    select
       xyz.risk_level
     , xyz.profile_id
     , date_trunc(month,min(xyz.date_created)) as date_limit_reached
from (
         select t.user_profile_id as profile_id
              , t.risk_level     as risk_level
              , SOURCE_CURRENCY as source_currency
              , t.action_creation_time as date_created
              , case when t.invoice_value_eur  >= 1000 then true else false end as transfer_over_1k
         from ANALYST_SANDBOX.temp_verification_threshold_dataset t
     ) xyz
--         left join ANALYST_SANDBOX.profiles_verified_borderless_LTV vb on xyz.PROFILE_ID = vb.user_profile_id
--         left join ANALYST_SANDBOX.receive_and_plastic_users_SM rpu on xyz.PROFILE_ID = rpu.profile_id
        where 1=1
--         and iff (vb.dateoflimit is not null, xyz.date_created <= vb.dateoflimit, xyz.date_created <= '2021-01-01'::date)
--         and iff (rpu.account_details_order_time is not null, xyz.date_created <= rpu.account_details_order_time, xyz.date_created <= '2021-01-01'::date)
--         and iff (rpu.card_order_time is not null, xyz.date_created <= rpu.card_order_time, xyz.date_created <= '2021-01-01'::date)
        and transfer_over_1k = true and source_currency
       in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB', 'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK', 'AUD', 'HKD','GBP')
group by 1,2
;

select * from ANALYST_SANDBOX.invoice_value_entitlement_recalculation_SM where profile_id = 10507793

select risk_level, date_limit_reached, count(profile_id)
from ANALYST_SANDBOX.invoice_value_entitlement_recalculation_SM
where date_limit_reached >= '2019-01-01' group by 1,2 order by 2,1;

--********************************************************************************************************************--
--PERSONAL_GLOBAL_LIFETIME and LINKED_TRANSACTIONS recalculation
--********************************************************************************************************************--

create or replace temporary table ANALYST_SANDBOX.temp_verification_threshold_dataset as
select ras.USER_PROFILE_ID
     , ras.REQUEST_ID
     , ras.ACTION_CREATION_TIME
     , ras.SOURCE_CURRENCY
     , t.TARGET_RECIPIENT_ID
     , rb.RISK_LEVEL
     , ras.INVOICE_VALUE_LOCAL * er.EXCH_RATE as invoice_value_eur
from reports.REPORT_ACTION_STEP ras
         left join TRANSFER.TRANSFER t
                   on ras.REQUEST_ID = t.ID
         left join fx.USER_PROFILE up
                   on ras.USER_PROFILE_ID = up.ID
                  LEFT JOIN ANALYST_SANDBOX.to_eur_exchange_rates_SM er on er.source_currency = ras.source_currency and
                                                                           er.currency_date = date_trunc('day', ras.action_creation_time)
         LEFT JOIN analyst_sandbox.user_profile_risk_score_level rb
                   on rb.user_profile_id = ras.USER_PROFILE_ID
where ras.flag_for_aggregations = 1                              -- getting unique payments
  and up.CLASS != 'com.transferwise.fx.user.BusinessUserProfile' -- always need to be verified before transfers
  and up.date_created >= '2016-01-01'
  and ras.product_type = 'SENDMONEY'                             -- only send money as upfront verification of customers
  and t.STATE not in ('CANCELED_WITHOUT_REFUND', 'REFUNDED', 'CHARGEDBACK_WITH_LOSS', 'CHARGEDBACK_WITHOUT_LOSS',
                      'CANCELED_WITH_REFUND')                    -- these don't count towards lifetime value
  and ras.ACTION_CREATION_TIME >= '2016-01-01'                    -- making sure we have exchange rates
  and invoice_value_eur is not null
;


drop table if exists ANALYST_SANDBOX.temp_transfers_linked_recipients_SM;
create temporary table ANALYST_SANDBOX.temp_transfers_linked_recipients_SM as
--this table just helps for later filtering. It selects all profile-recipient pairs who've reached 1000 in total in their lifetime
select a.ACTION_CREATION_TIME as refdate,
       a.USER_PROFILE_ID,
       a.target_recipient_id as recipient_id,
       a.source_currency     as source_currency,
       a.risk_level,
       sum(case when b.ACTION_CREATION_TIME <= refdate then b.invoice_value_eur else 0 end) as lifetime_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -90, refdate) and b.ACTION_CREATION_TIME <= refdate
                   and a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID
                   then b.invoice_value_eur else 0 end)      last_90d_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -180, refdate) and b.ACTION_CREATION_TIME <= refdate
                   and a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID
                   then b.invoice_value_eur else 0 end)      last_180d_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -135, refdate) and b.ACTION_CREATION_TIME <= refdate
                   and a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID
                   then b.invoice_value_eur else 0 end)      last_135d_volume
from ANALYST_SANDBOX.temp_verification_threshold_dataset a
left join ANALYST_SANDBOX.temp_verification_threshold_dataset b
    on a.USER_PROFILE_ID = b.USER_PROFILE_ID
where a.ACTION_CREATION_TIME >= '2016-01-01'
group by 1,2,3,4,5;

create or replace temporary table ANALYST_SANDBOX.new_rules_volume_based_on_risk_profiles as
    select
        lr.risk_level,
        lr.user_profile_id,
        lr.source_currency,
        lr.recipient_id,
        refdate,
        case
          when risk_level = 'LOW' then last_90D_volume
          when risk_level = 'MEDIUM' then last_135d_volume
          when risk_level = 'HIGH' then last_180d_volume end as volume_at_day
        from ANALYST_SANDBOX.temp_transfers_linked_recipients_SM lr;

create or replace temporary table ANALYST_SANDBOX.date_when_cst_hits_limit_new_rules as
select lr.risk_level
        , lr.user_profile_id
        , lr.source_currency
        , min(lr.REFDATE) as dateoflimit
        from ANALYST_SANDBOX.new_rules_volume_based_on_risk_profiles lr
        where volume_at_day >= 1000
        group by 1,2,3;

create or replace temporary table ANALYST_SANDBOX.date_when_cst_hits_limit_old_rules as
        select lr.risk_level
        , lr.user_profile_id
        , lr.source_currency
        , min(lr.refdate) as dateoflimit
        from ANALYST_SANDBOX.temp_transfers_linked_recipients_SM lr
        where lr.lifetime_volume >= 1000
        group by 1,2,3;

create or replace transient table ANALYST_SANDBOX.old_rules_verifications_bnp_precondition as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_old_rules a
        left join ANALYST_SANDBOX.profiles_verified_borderless_LTV vb on a.user_PROFILE_ID = vb.user_profile_id
        left join ANALYST_SANDBOX.receive_and_plastic_users_SM rpu on a.user_PROFILE_ID = rpu.profile_id
        where 1=1
        and iff (vb.dateoflimit is not null, a.dateoflimit <= vb.dateoflimit, a.dateoflimit <= '2021-01-01'::date)
        and iff (rpu.account_details_order_time is not null, a.dateoflimit <= rpu.account_details_order_time, a.dateoflimit <= '2021-01-01'::date)
        and iff (rpu.card_order_time is not null, a.dateoflimit <= rpu.card_order_time, a.dateoflimit <= '2021-01-01'::date)
        and a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
        group by 2,3;

create or replace transient table ANALYST_SANDBOX.old_rules_verifications as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_old_rules a
        where 1=1
        and a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
        group by 2,3;

create or replace transient table ANALYST_SANDBOX.new_rule_verifications_bnp_precondition as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_new_rules a
        left join ANALYST_SANDBOX.profiles_verified_borderless_LTV vb on a.user_PROFILE_ID = vb.user_profile_id
        left join ANALYST_SANDBOX.receive_and_plastic_users_SM rpu on a.user_PROFILE_ID = rpu.profile_id
        where 1=1
        and iff (vb.dateoflimit is not null, a.dateoflimit <= vb.dateoflimit, a.dateoflimit <= '2021-01-01'::date)
        and iff (rpu.account_details_order_time is not null, a.dateoflimit <= rpu.account_details_order_time, a.dateoflimit <= '2021-01-01'::date)
        and iff (rpu.card_order_time is not null, a.dateoflimit <= rpu.card_order_time, a.dateoflimit <= '2021-01-01'::date)
        and a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
        group by 2,3;

create or replace transient table ANALYST_SANDBOX.new_rule_verifications as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_new_rules a
        where 1=1
        and a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
        group by 2,3;

select min(month_rule_hit) from ANALYST_SANDBOX.new_rule_verifications

select * from ANALYST_SANDBOX.new_rule_verifications where user_profile_id = 10507793

select * from (
select user_profile_id, count(distinct risk_level) as cr, count(distinct month_rule_hit) as cm
from ANALYST_SANDBOX.old_rules_verifications group by 1) where cr > 1 or cm > 1

select month_rule_hit,
       risk_level,
       count(user_profile_id)
from ANALYST_SANDBOX.new_rule_verifications group by 1,2 order by month_rule_hit, risk_level;

select month_rule_hit,
       risk_level,
       count(user_profile_id)
from ANALYST_SANDBOX.old_rules_verifications group by 1,2 order by month_rule_hit, risk_level;


--********************************************************************************************************************--
--CALCULATE THE DIFFERENCE IN FRAUD AND RETENTION RATES BETWEEN THE DIFFERENT ENTITLEMENTS
--********************************************************************************************************************--

--code taken from the looker fraud suspension explore
with verified_customers as (
select a.user_profile_id
from ANALYST_SANDBOX.old_rules_verifications a
    where a.user_profile_id
    not in (select b.user_profile_id from ANALYST_SANDBOX.new_rule_verifications b)
    )
SELECT
	DATE_TRUNC('month', CAST(transfers.ACTION_CREATION_TIME AS TIMESTAMP_NTZ)) AS transaction_creation_month,
-- 	vc.verification_method,
	COUNT(DISTINCT case when (model_scoring.NEEDS_REVIEW) = 1 then (model_scoring.REQUEST_ID) end)
    /COUNT(DISTINCT model_scoring.REQUEST_ID)  AS percent_transfer_suspended,
    COUNT(DISTINCT case when (model_scoring.NEEDS_REVIEW) = 1 then (model_scoring.REQUEST_ID) end) as total_transfers_suspended
FROM verified_customers vc
left join REPORTS.FRAUD_LOOKER_TRANSFERS  AS transfers on vc.user_profile_id = transfers.user_profile_id
LEFT JOIN REPORTS_TEST.FRAUD_MODELS_PREDICTIONS_AND_SCORES AS model_scoring ON (transfers.REQUEST_ID) = (model_scoring.REQUEST_ID)
WHERE transfers.ACTION_CREATION_TIME >= '2019-01-01'::date
and transfers.INVOICE_VALUE_GBP  < 1000.0
GROUP BY 1
HAVING
	NOT (COUNT(DISTINCT transfers.REQUEST_ID) IS NULL)
ORDER BY 1 DESC;

--***************************************************FRAUD RATES******************************************************--


with verified_customers as (
select a.user_profile_id, a.risk_level
from ANALYST_SANDBOX.old_rules_verifications a
    where a.user_profile_id
    not in (select b.user_profile_id from ANALYST_SANDBOX.new_rule_verifications b)
    )
SELECT
	DATE_TRUNC('month',transfers.ACTION_CREATION_TIME) AS transaction_creation_month,
	NULLIF((COUNT(DISTINCT fraudulent_transfer_data.REQUEST_ID)),0) / NULLIF((COUNT(DISTINCT case when (transfers.ACTION_STATE)='TRANSFERRED' then  transfers.REQUEST_ID else null end)),0)  AS fraud_rate_transfer_numbers,
	COUNT(DISTINCT fraudulent_transfer_data.REQUEST_ID) AS count_of_fraudulent_transfers
FROM verified_customers vc
left join REPORTS.FRAUD_LOOKER_TRANSFERS  AS transfers on vc.user_profile_id = transfers.user_profile_id
LEFT JOIN reports_test.fraud_metrics AS fraudulent_transfer_data ON (transfers.REQUEST_ID) = (fraudulent_transfer_data.REQUEST_ID)
LEFT JOIN REPORTS_TEST.FRAUD_MODELS_PREDICTIONS_AND_SCORES  AS model_scoring ON (transfers.REQUEST_ID) = (model_scoring.REQUEST_ID)
where transfers.ACTION_CREATION_TIME >= '2019-01-01' AND (transfers.PAYMENT_METHOD <> 'balance' OR transfers.PAYMENT_METHOD IS NULL)
and transfers.INVOICE_VALUE_GBP  <= 1000.0
GROUP BY DATE_TRUNC('month', transfers.ACTION_CREATION_TIME)
HAVING ((NULLIF( (COUNT(DISTINCT case when transfers.ENGINE_SUSPEND_FLAG = 1 then transfers.REQUEST_ID else null end)) , 0 ) /
         (COUNT(DISTINCT transfers.REQUEST_ID))   > 0)) AND (NOT (( (COUNT(DISTINCT case when (model_scoring.NEEDS_REVIEW)=1 then (model_scoring.REQUEST_ID) else null end )) )
        / ( NULLIF((COUNT(DISTINCT (model_scoring.REQUEST_ID) )),0) )  IS NULL))
ORDER BY 1 DESC;


--********************************************************************************************************************--
--CALCULATE THE DIFFERENCE IN FRAUD RATES BETWEEN THE TWO OLD ENTITLEMENTS
--********************************************************************************************************************--
create or replace transient table analyst_sandbox.global_lifetime_entitlement_verified_customers_SM as
select
    up.id as profile_id,
    date_trunc(month, te.date_created) as entitlement_month,
    pvh.verification_method,
    rb.risk_level,
    te.entitlement_id
from fx.user_profile up
inner join transfer.transfer t on up.id = t.user_profile_id
left join fx.triggered_entitlement te on t.id = te.request_id
left join analyst_sandbox.user_profile_risk_score_level rb on rb.user_profile_id = up.id
left join ANALYST_SANDBOX.profile_verification_history_SM pvh on pvh.user_profile_id = up.id
and pvh.first_verification_time > dateadd(day,-1,te.date_created) and pvh.first_verification_time <= dateadd(day, 7, te.date_created)
where te.entitlement_id in ('97') --97 is the PERSONAL_GLOBAL_LIFETIME; 2 is PERSONAL_PAYMENT
  --select only profiles who only underwent successful verification after the entitlement was triggered
and te.date_created >= '2018-08-01'
--and up.date_created >= '2018-01-01'
and up.class ilike '%personal%';

create or replace transient table analyst_sandbox.global_lifetime_entitlement_triggered_SM as
select
    up.id as profile_id,
    date_trunc(month, te.date_created) as entitlement_month,
    rb.risk_level
from fx.user_profile up
inner join transfer.transfer t on up.id = t.user_profile_id
left join fx.triggered_entitlement te on t.id = te.request_id
left join analyst_sandbox.user_profile_risk_score_level rb on rb.user_profile_id = up.id
where te.entitlement_id in ('97') --97 is the PERSONAL_GLOBAL_LIFETIME; 2 is PERSONAL_PAYMENT
  --select only profiles who only underwent successful verification after the entitlement was triggered
and te.date_created >= '2019-08-01'
--and up.date_created >= '2018-01-01'
and up.class ilike '%personal%';


create or replace transient table analyst_sandbox.personal_payment_entitlement_verified_customers_SM as
select
    up.id as profile_id,
    date_trunc(month, te.date_created) as entitlement_month,
    pvh.verification_method,
    rb.risk_level
from fx.user_profile up
inner join transfer.transfer t on up.id = t.user_profile_id
inner join fx.triggered_entitlement te on t.id = te.request_id
left join analyst_sandbox.user_profile_risk_score_level rb on rb.user_profile_id = up.id
left join ANALYST_SANDBOX.profile_verification_history_SM pvh on pvh.user_profile_id = up.id
and pvh.first_verification_time > dateadd(day,-1,te.date_created) and pvh.first_verification_time <= dateadd(day, 7, te.date_created)
where te.entitlement_id in ('2') --97 is the PERSONAL_GLOBAL_LIFETIME; is PERSONAL_PAYMENT
  --select only profiles who only underwent successful verification after the entitlement was triggered
and te.date_created < '2019-08-01'
--and up.date_created >= '2018-01-01'
and up.class ilike '%personal%';

create or replace transient table analyst_sandbox.personal_payment_entitlement_triggered_SM as
select
    up.id as profile_id,
    date_trunc(month, te.date_created) as entitlement_month,
    rb.risk_level
from fx.user_profile up
inner join transfer.transfer t on up.id = t.user_profile_id
inner join fx.triggered_entitlement te on t.id = te.request_id
left join analyst_sandbox.user_profile_risk_score_level rb on rb.user_profile_id = up.id
where te.entitlement_id in ('2') --97 is the PERSONAL_GLOBAL_LIFETIME; is PERSONAL_PAYMENT
  --select only profiles who only underwent successful verification after the entitlement was triggered
and te.date_created < '2019-08-01'
--and up.date_created >= '2018-01-01'
and up.class ilike '%personal%';
