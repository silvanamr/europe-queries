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


create or replace transient table ANALYST_SANDBOX.temp_verification_threshold_dataset as
select ras.USER_PROFILE_ID
     , ras.REQUEST_ID
     , ras.ACTION_CREATION_TIME
     , ras.SOURCE_CURRENCY
     , ras.product_type
     , t.TARGET_RECIPIENT_ID
     , rb.RISK_LEVEL
     , ras.INVOICE_VALUE_LOCAL * er.EXCH_RATE as invoice_value_eur
     , case when (a.country_code = 'usa') then true else false end as has_us_address
     , case when a.country_code not in ('afg', 'ala', 'alb', 'aus', 'bel',	'vgb',	'bdi',	'can',	'caf',	'tcd',	'hkg',	'cog',	'cod',	'cub',	'eri',
                                        'ind', 'irn', 'irq', 'jpn', 'lby',	'nzl',	'pse',	'sgp',	'som',	'ssd',	'sdn',	'syr',	'usa',	'umi')
         then true else false end as tw_ltd_customer
from reports.REPORT_ACTION_STEP ras
         left join TRANSFER.TRANSFER t
                   on ras.REQUEST_ID = t.ID
         left join reports.lookup_request_history lrh
                   on t.id = lrh.request_id
         LEFT JOIN ANALYST_SANDBOX.to_eur_exchange_rates_SM er
                   on er.source_currency = ras.source_currency and
                      er.currency_date = date_trunc('day', ras.action_creation_time)
         LEFT JOIN FX.USER_PROFILE UP ON UP.ID = RAS.USER_PROFILE_ID
         LEFT JOIN FX.USER_PROFILE_ADDRESS a on up.address_id = a.id
         LEFT JOIN analyst_sandbox.user_profile_risk_score_level rb
                   on rb.user_profile_id = ras.USER_PROFILE_ID
where ras.flag_for_aggregations = 1                              -- getting unique payments
  and up.CLASS != 'com.transferwise.fx.user.BusinessUserProfile' -- always need to be verified before transfers
  --and up.date_created >= '2019-01-01'
  and ras.product_type in ('SENDMONEY', 'BALANCE')                          -- only send money as upfront verification of customers
  and t.STATE not in ('CANCELED_WITHOUT_REFUND', 'REFUNDED', 'CHARGEDBACK_WITH_LOSS', 'CHARGEDBACK_WITHOUT_LOSS',
                      'CANCELED_WITH_REFUND')                    -- these don't count towards lifetime value
  and invoice_value_eur is not null
  and lrh.first_success_submit_date >= '2019-01-01'::date
  and ras.action_creation_time >= lrh.first_success_submit_date;



--********************************************************************************************************************--
--LINKED RECIPIENTS ONLY
--********************************************************************************************************************--

drop table if exists ANALYST_SANDBOX.temp_transfers_linked_recipients_only_SM;
create temporary table ANALYST_SANDBOX.temp_transfers_linked_recipients_only_SM as
--this table just helps for later filtering. It selects all profile-recipient pairs who've reached 1000 in total in their lifetime
select a.ACTION_CREATION_TIME as refdate,
       a.USER_PROFILE_ID,
       a.target_recipient_id as recipient_id,
       a.source_currency     as source_currency,
       a.product_type,
       a.risk_level,
       a.has_us_address,
       a.tw_ltd_customer,
       sum(case when b.ACTION_CREATION_TIME <= refdate then b.invoice_value_eur else 0 end) over (partition by a.user_profile_id, a.ACTION_CREATION_TIME) as lifetime_volume,
       sum(case when a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID and b.ACTION_CREATION_TIME <= refdate then b.invoice_value_eur else 0 end)
           over (partition by a.user_profile_id, a.TARGET_RECIPIENT_ID, a.ACTION_CREATION_TIME) same_recipient_volume
from ANALYST_SANDBOX.temp_verification_threshold_dataset a
left join ANALYST_SANDBOX.temp_verification_threshold_dataset b
    on a.USER_PROFILE_ID = b.USER_PROFILE_ID;

select * from ANALYST_SANDBOX.temp_transfers_linked_recipients_only_SM order by USER_PROFILE_ID asc, refdate limit 500

create temporary table ANALYST_SANDBOX.temp_transfers_linked_recipients_SM2 as
select refdate,
       USER_PROFILE_ID,
       recipient_id,
       source_currency,
       product_type,
       risk_level,
       has_us_address,
       tw_ltd_customer,
       lifetime_volume,
       same_recipient_volume
from ANALYST_SANDBOX.temp_transfers_linked_recipients_only_SM
group by 1,2,3,4,5,6,7,8,9,10;


create or replace temporary table ANALYST_SANDBOX.date_when_cst_hits_limit_PGL as
select
          risk_level
        , user_profile_id
        , source_currency
        , product_type
        , has_us_address
        , tw_ltd_customer
        , min(REFDATE) as dateoflimit
         from ANALYST_SANDBOX.temp_transfers_linked_recipients_SM2 lr
        where lifetime_volume >= 1000
        group by 1, 2, 3, 4, 5, 6;


create or replace temporary table ANALYST_SANDBOX.date_when_cst_hits_limit_LRO as
        select
          lr.risk_level
        , lr.user_profile_id
        , lr.source_currency
        , product_type
        , has_us_address
        , tw_ltd_customer
        , min(lr.refdate) as dateoflimit
        from ANALYST_SANDBOX.temp_transfers_linked_recipients_SM2 lr
        where lr.same_recipient_volume >= 1000
        group by 1, 2, 3, 4, 5, 6;

--here we make sure that the customers are assigned to tw ltd: if the transaction that is making them reach 1.000 is from Sendmoney, they are assigned by currency
--if the transaction is balance, they are assigned by their address
create or replace transient table ANALYST_SANDBOX.PGL_verifications as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_PGL a
        where 1=1
        and case when product_type = 'SENDMONEY' and a.source_currency = 'USD' then has_us_address = false
                 when product_type = 'SENDMONEY' then a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
                 when product_type = 'BALANCE' then tw_ltd_customer = true end
        group by 2,3;

select count(distinct user_profile_id) from ANALYST_SANDBOX.PGL_verifications;


create or replace transient table ANALYST_SANDBOX.LRO_verifications as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_LRO a
        where 1=1
        and case when product_type = 'SENDMONEY' and a.source_currency = 'USD' then has_us_address = false
                 when product_type = 'SENDMONEY' then a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
                 when product_type = 'BALANCE' then tw_ltd_customer = true end
        group by 2,3;

--for the explore in Looker: join everything above to user profile and set columns: theoretical LT trigger date;
--theoretical PGL trigger date; actual LT trigger date (will be the actual PGL by then)

select * from ANALYST_SANDBOX.new_logic_PGL_SM
where lto_reached_month is not null and pgl_reached_month is null
order by user_profile_id;


--********************************************************************************************************************--
--LINKED RECIPIENTS WITH TIME WINDOW
--********************************************************************************************************************--

drop table if exists ANALYST_SANDBOX.temp_transfers_linked_recipients_time_window_SM;
create temporary table ANALYST_SANDBOX.temp_transfers_linked_recipients_time_window_SM as
--this table just helps for later filtering. It selects all profile-recipient pairs who've reached 1000 in total in their lifetime
select a.ACTION_CREATION_TIME as refdate,
       a.USER_PROFILE_ID,
       a.target_recipient_id as recipient_id,
       a.source_currency     as source_currency,
       a.product_type,
       a.risk_level,
       a.has_us_address,
       a.tw_ltd_customer,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -90, refdate) and b.ACTION_CREATION_TIME <= refdate
                   and a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID
                   then b.invoice_value_eur else 0 end) over (partition by a.USER_PROFILE_ID, a.ACTION_CREATION_TIME, a.TARGET_RECIPIENT_ID)      last_90d_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -180, refdate) and b.ACTION_CREATION_TIME <= refdate
                   and a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID
                   then b.invoice_value_eur else 0 end) over (partition by a.USER_PROFILE_ID, a.ACTION_CREATION_TIME, a.TARGET_RECIPIENT_ID)       last_180d_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -135, refdate) and b.ACTION_CREATION_TIME <= refdate
                   and a.TARGET_RECIPIENT_ID = b.TARGET_RECIPIENT_ID
                   then b.invoice_value_eur else 0 end) over (partition by a.USER_PROFILE_ID, a.ACTION_CREATION_TIME, a.TARGET_RECIPIENT_ID)      last_135d_volume
from ANALYST_SANDBOX.temp_verification_threshold_dataset a
left join ANALYST_SANDBOX.temp_verification_threshold_dataset b
    on a.USER_PROFILE_ID = b.USER_PROFILE_ID;

create or replace transient table ANALYST_SANDBOX.temp_transfers_linked_recipients_time_window_SM2 clone ANALYST_SANDBOX.temp_transfers_linked_recipients_time_window_SM;

create or replace temporary table ANALYST_SANDBOX.LTTW_volume_based_on_risk_profiles as
    select
        lr.risk_level,
        lr.user_profile_id,
        lr.source_currency,
        lr.product_type,
        lr.recipient_id,
        lr.has_us_address,
        lr.tw_ltd_customer as tw_ltd_customer,
        refdate,
        case
          when risk_level = 'LOW' then last_90D_volume
          when risk_level = 'MEDIUM' then last_135d_volume
          when risk_level = 'HIGH' then last_180d_volume end as volume_at_day
        from ANALYST_SANDBOX.temp_transfers_linked_recipients_time_window_SM lr
group by 1,2,3,4,5,6,7,8,9;


select * from ANALYST_SANDBOX.LTTW_volume_based_on_risk_profiles where user_profile_id = 359 order by recipient_id, refdate

select * from ANALYST_SANDBOX.LTTW_volume_based_on_risk_profiles order by user_profile_id, refdate asc limit 300

create or replace temporary table ANALYST_SANDBOX.date_when_cst_hits_limit_LTTW as
select    lr.risk_level
        , lr.user_profile_id
        , lr.source_currency
        , lr.product_type
        , lr.has_us_address
        , lr.tw_ltd_customer
        , min(lr.REFDATE) as dateoflimit
        from ANALYST_SANDBOX.LTTW_volume_based_on_risk_profiles lr
        where volume_at_day >= 1000
        group by 1,2,3,4,5,6;

select * from ANALYST_SANDBOX.date_when_cst_hits_limit_LTTW where user_profile_id = 359

create or replace transient table ANALYST_SANDBOX.LTTW_verifications as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_LTTW a
        where 1=1
        and case when product_type = 'SENDMONEY' and a.source_currency = 'USD' then has_us_address = false
                 when product_type = 'SENDMONEY' then a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
                 when product_type = 'BALANCE' then tw_ltd_customer = true end
        group by 2,3;

select * from ANALYST_SANDBOX.LTTW_verifications where user_profile_id = 359

--********************************************************************************************************************--
--ONLY TIME WINDOW
--********************************************************************************************************************--

drop table if exists ANALYST_SANDBOX.temp_transfers_only_time_window_SM;
create temporary table ANALYST_SANDBOX.temp_transfers_only_time_window_SM as
--this table just helps for later filtering. It selects all profile-recipient pairs who've reached 1000 in total in their lifetime
select a.ACTION_CREATION_TIME as refdate,
       a.USER_PROFILE_ID,
       a.source_currency     as source_currency,
       a.product_type,
       a.risk_level,
       a.has_us_address,
       a.tw_ltd_customer,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -90, refdate) and b.ACTION_CREATION_TIME <= refdate
                   then b.invoice_value_eur else 0 end) over (partition by a.USER_PROFILE_ID, a.ACTION_CREATION_TIME)     last_90d_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -180, refdate) and b.ACTION_CREATION_TIME <= refdate
                   then b.invoice_value_eur else 0 end) over (partition by a.USER_PROFILE_ID, a.ACTION_CREATION_TIME)     last_180d_volume,
       sum(case
               when b.ACTION_CREATION_TIME > dateadd(day, -135, refdate) and b.ACTION_CREATION_TIME <= refdate
                   then b.invoice_value_eur else 0 end) over (partition by a.USER_PROFILE_ID, a.ACTION_CREATION_TIME)     last_135d_volume
from ANALYST_SANDBOX.temp_verification_threshold_dataset a
left join ANALYST_SANDBOX.temp_verification_threshold_dataset b
    on a.USER_PROFILE_ID = b.USER_PROFILE_ID;

create or replace temporary table ANALYST_SANDBOX.LTTO_volume_based_on_risk_profiles as
    select
        lr.risk_level,
        lr.user_profile_id,
        lr.source_currency,
        lr.product_type,
        lr.has_us_address,
        lr.tw_ltd_customer as tw_ltd_customer,
        refdate,
        case
          when risk_level = 'LOW' then last_90D_volume
          when risk_level = 'MEDIUM' then last_135d_volume
          when risk_level = 'HIGH' then last_180d_volume end as volume_at_day
        from ANALYST_SANDBOX.temp_transfers_only_time_window_SM lr
        group by 1,2,3,4,5,6,7,8;

select * from ANALYST_SANDBOX.LTTO_volume_based_on_risk_profiles where USER_PROFILE_ID = 359 order by refdate

select sum(invoice_value_gbp) from reports.report_action_step where user_profile_id = 359 and flag_for_aggregations = true

create or replace temporary table ANALYST_SANDBOX.date_when_cst_hits_limit_LTTO as
select    lr.risk_level
        , lr.user_profile_id
        , lr.source_currency
        , lr.product_type
        , lr.has_us_address
        , lr.tw_ltd_customer
        , min(lr.REFDATE) as dateoflimit
        from ANALYST_SANDBOX.LTTO_volume_based_on_risk_profiles lr
        where volume_at_day >= 1000
        group by 1,2,3,4,5,6;

-- select * from ANALYST_SANDBOX.LTTO_volume_based_on_risk_profiles where USER_PROFILE_ID = 10507793

create or replace transient table ANALYST_SANDBOX.LTTO_verifications as
          select min(date_trunc('month', a.dateoflimit)) as month_rule_hit
        , a.risk_level
        , a.user_profile_id
        from ANALYST_SANDBOX.date_when_cst_hits_limit_LTTO a
        where 1=1
        and case when product_type = 'SENDMONEY' and a.source_currency = 'USD' then has_us_address = false
                 when product_type = 'SENDMONEY' then a.source_currency in ('EUR', 'CHF', 'HUF', 'RON', 'BGN', 'UAH', 'RUB',
                  'GEL', 'HRK', 'PLN', 'SEK', 'NOK', 'DKK','AUD', 'HKD' , 'GBP')
                 when product_type = 'BALANCE' then tw_ltd_customer = true end
        group by 2,3;

--********************************************************************************************************************--
--create table with the different entitlement dates for each person

drop table ANALYST_SANDBOX.new_logic_PGL_SM;
create table ANALYST_SANDBOX.new_logic_PGL_SM as
with all_customers_with_entitlement as
    (
    select risk_level,user_profile_id from ANALYST_SANDBOX.PGL_verifications
    union
    select risk_level,user_profile_id from ANALYST_SANDBOX.LRO_verifications
    union
    select risk_level,user_profile_id from ANALYST_SANDBOX.LTTW_verifications
    )

select ace.user_profile_id,
       ace.risk_level,
       pgl.month_rule_hit as pgl_reached_month,
       lro.month_rule_hit as lro_reached_month,
       lttw.month_rule_hit as lttw_reached_month,
       ltto.month_rule_hit as ltto_reached_month
from all_customers_with_entitlement ace
left join ANALYST_SANDBOX.PGL_verifications as pgl on ace.user_profile_id = pgl.user_profile_id
left join ANALYST_SANDBOX.LRO_verifications lro on ace.user_profile_id = lro.user_profile_id
left join ANALYST_SANDBOX.LTTW_verifications lttw on ace.user_profile_id = lttw.user_profile_id
left join ANALYST_SANDBOX.LTTO_verifications ltto on ace.user_profile_id = ltto.user_profile_id;

select * from ANALYST_SANDBOX.new_logic_PGL_SM where pgl_reached_month <> lttw_reached_month and  pgl_reached_month <> ltto_reached_month order by user_profile_id limit 100


select count(case when ltto_reached_month is not null then user_profile_id end) as customers_reached_only_time_window,
       count(case when lttw_reached_month is not null then user_profile_id end) as customers_reached_time_window_and_same_recipient,
       count(case when pgl_reached_month is not null then user_profile_id end) as customer_reached_personal_global_lifetime
from ANALYST_SANDBOX.new_logic_PGL_SM;



select count(distinct t.user_profile_id)
from fx.triggered_entitlement te
left join transfer.transfer t on te.request_id = t.id
where te.date_created >= '2019-01-01'

select * from fx.triggered_entitlement limit 5

