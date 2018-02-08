

WITH time_params  AS(

--SELECT
--
--(date_trunc('month', NOW())  - '2 month'::interval)::DATE as former_month_start,
--(date_trunc('month', NOW())  - '1 month'::interval-'1 day'::interval)::DATE as former_month_end,
--(date_trunc('month', NOW())  - '1 month'::interval)::DATE as deal_start_month,
--(date_trunc('month', NOW())  - '1 day'::interval)::DATE as deal_end_month


SELECT
'2017-12-01'::DATE as former_month_start,
'2017-12-31'::DATE as former_month_end,
'2018-01-01'::DATE as deal_start_month,
'2018-01-31'::DATE as deal_end_month
)



, emails as (

SELECT *, 
iso3166a3||'_'||sbubscriberkey_end AS subscriberkey

FROM (
        select
        sf."Country" as country,
        sf."Partner Backend Code" as vendor_code, 
        sf."Email" as email,
        sf."Partner Backend Code"||'_'||row_number() over (partition by "Country", "Partner Backend Code") as sbubscriberkey_end,
        cow.iso3166a3,
        row_number() over (partition by "Country", "Partner Backend Code") as row
        
        FROM salesforce_fo.foodora_all_partner_billing_contacts sf --has FO and FP
                
        LEFT JOIN dwh_metadata.countries_of_the_world cow    --SELECT * FROM dwh_metadata.countries_of_the_world
        on sf."Country"=cow.isoen_name 
        
        WHERE "Exclude From All Newsletters"=0

        
        
)a 
where row=1

)




, banner as (


SELECT v.rdbms_id,  v.vendor_id, --v.vendor_name, v.vendor_code, v.city_id, v.vendor_active, v.vendor_deleted, v.vendor_testing, v.pickup_time, email,
CASE WHEN cu.globalcuisine_id IN (19,18,17,83,54,3,81) THEN 'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_burger_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (30,76,82,72,64,106) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_arabic_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (51,67,29,80,124,79,43) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_argentinian_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (7,46,28,92,68,41,88,125,6,8) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_asian_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (9,128,22,38,60,16,42) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_cake_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (15,13,66,97,37,89,84,34,49,47) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_european_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (65,26,116,104,10,25,131,21) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_healthyfood_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (12,91,69,101) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_indian_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (11,52,122,27,39,112,40,57,32,48,86,93) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_international_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (1,35,5) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_italian_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (2,53) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_mexicanfood_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (44,110,45,33,63,55,31,23) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_oriental_foodpanda_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (14,50) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_foodpanda_fish_Partners_Relations_V01_NC.jpg'
WHEN cu.globalcuisine_id IN (20,4) THEN'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_sushi_foodpanda_Partners_Relations_V01_NC.jpg'
ELSE 'https://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/SG/Global_NL_european_foodpanda_Partners_Relations_V01_NC.jpg'
END AS banner_url

FROM dwh_il.dim_vendors v
LEFT JOIN dwh_il.dim_vendorscuisines_fp cu  
ON cu.rdbms_id=v.rdbms_id AND v.vendor_id=cu.vendor_id AND globalcuisine_title=v.main_cuisine

WHERE v.rdbms_id IN (7,42,19,16,20,15,18,17,12)
AND v.vendor_active=1 AND vendor_deleted=0 AND v.vendor_code is not null         

)


, data_set as (    -- rdbms, v_id, v_code, v_name, city, week, country, 

SELECT DISTINCT 
v.rdbms_id,
v.vendor_id, 
v.vendor_code,
c.city_id, 
co.common_name as country_name,
c.city_name as city_name,
v.vendor_name,
a.account_id,
v.activation_date,
b.banner_url

FROM  dwh_il.dim_vendors v --vendor                  select * from dwh_il.dim_vendors where rdbms_id=16

LEFT JOIN dwh_il.dim_city c --city                   
ON v.rdbms_id=c.rdbms_id and v.city_id=c.city_id

LEFT JOIN dwh_il.dim_countries co --country             select * from dwh_il.dim_countries
ON v.rdbms_id = co.rdbms_id

LEFT JOIN salesforce.il_dim_accounts a --account --(has no rdbms_id)     select * from salesforce.il_dim_accounts where "vendor_name" in ('DV Ristorante Pizzeria')     
ON v.vendor_code=a.vendor_code                                          

left join salesforce.dwh_all_contacts_for_active_accounts con -- select * from salesforce.dwh_all_contacts_for_active_accounts
on a.account_id=con."18 Char Account ID"

LEFT JOIN salesforce.il_dim_opportunities sf  --salesforce.dwh_all_opportunities sf --                 select * from salesforce.il_dim_opportunities where "Type" IN ('Deals')
ON sf.account_id = a.account_id

LEFT JOIN banner b
on v.rdbms_id=b.rdbms_id AND v.vendor_id=b.vendor_id --AND d.main_cuisine=b.globalcuisine_title

WHERE sf.created_date BETWEEN (SELECT former_month_start from time_params) AND (SELECT former_month_end from time_params) AND sf.type IN ('Deals') AND
AND sf.opportunity_stage IN ('Closed Won') and v.rdbms_id=15 
AND a.account_status='Active'

AND  v.rdbms_id IN (15) AND  v.vendor_id in (     
'9993',
'9989',
'9961',
'9911',
'9836',
'9835',
'9802',
'9793',
'9750',
'9537',
'9535',
'9503',
'9094',
'8823',
'8817',
'8792',
'8765',
'869',
'8623',
'8600',
'8591',
'8578',
'8547',
'8478',
'8409',
'8105',
'8103',
'8098',
'8053',
'8021',
'7863',
'7856',
'7693',
'7444',
'7429',
'7345',
'7248',
'7118',
'7032',
'7021',
'7000',
'6926',
'6659',
'6562',
'6401',
'6216',
'6207',
'5940',
'5939',
'5618',
'5429',
'5192',
'5091',
'4918',
'4710',
'4615',
'4240',
'4154',
'4145',
'3793',
'3737',
'3609',
'3308',
'2031',
'1650',
'1183',
'1149',
'10825',
'10760',
'10661',
'10658',
'10621',
'10589',
'10507',
'10506',
'10436',
'10425',
'10419',
'10289',
'10279',
'10273',
'10181',
'10118',
'10088',
'10083',
'10071',
'10062',
'10059',
'10054')

)




, fp_orders AS(
SELECT * FROM dwh_il.fct_orders WHERE order_date::date BETWEEN (SELECT former_month_start from time_params) and (SELECT deal_end_month from time_params)

--CHECK IF THE # OF ORDERS IS CORRECT:
--WHERE rdbms_id=15 and vendor_ID = 7000

)


, penultimate  AS (  -- operations  VALID ORDERS, GROSS, ACTIONABLE, AUTOMATED, PROCESSING_TIME, VENDOR DELAY SUM

 
SELECT 
o.rdbms_id,
d.vendor_code,
d.activation_date::date,

COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (SELECT deal_start_month from time_params) AND (SELECT deal_end_month from time_params)) as orders_deals_month, 
COUNT(DISTINCT o.order_id) FILTER (WHERE o.order_date::date BETWEEN (SELECT former_month_start from time_params) AND (SELECT deal_start_month from time_params)) as orders_former_month, 

COUNT(DISTINCT o.customer_ident) FILTER (WHERE  o.order_date::date BETWEEN (SELECT deal_start_month from time_params) AND (SELECT deal_end_month from time_params)) as distinct_cust_deals_month, 
COUNT(DISTINCT o.customer_ident) FILTER (WHERE o.order_date::date BETWEEN (SELECT former_month_start from time_params) AND (SELECT deal_start_month from time_params)) as distinct_cust_former_month, 

SUM(o.gfv_eur*fx) FILTER (WHERE o.order_date::date BETWEEN (SELECT deal_start_month from time_params) AND (SELECT deal_end_month from time_params)) AS sales_deals_month,--, -- GROSS FOOD VALUE the order price - delivery fee
SUM(o.gfv_eur*fx) FILTER (WHERE o.order_date::date BETWEEN (SELECT former_month_start from time_params) AND (SELECT deal_start_month from time_params)) AS sales_former_month--, -- GROSS FOOD VALUE the order price - delivery fee


FROM data_set d
LEFT JOIN fp_orders o --orders                       select * from dwh_il.fct_orders
ON d.rdbms_id=o.rdbms_id and d.vendor_id=o.vendor_id
 
LEFT JOIN dwh_il.meta_order_status s --order status    select * from dwh_il.meta_order_status
ON o.rdbms_id=s.rdbms_id AND s.status_id=o.status_id 
WHERE s.valid_order=1  


GROUP BY 1,2,3



)



, final as(

SELECT  DISTINCT

d.rdbms_id,
d.vendor_code,
d.vendor_id,
d.vendor_name,
d.country_name,
d.city_name,
e.email,

e.subscriberkey,
to_char((SELECT deal_start_month::date from time_params), 'iyyy-mm') as deals_month,

case when d.vendor_code in (select vendor_code from dwh_st.zhan_Confirmation_Month where rdbms_id=15 AND deals_month IN ('2017-12'))      --SELECT * FROM dwh_st.zhan_Confirmation_Month
     then 'True' 
     else 'False' 
     end as deal_former_month,
     
p.activation_date::date,
round(p.orders_deals_month) as orders_deals_month,
round(p.orders_former_month) as orders_former_month,
round(p.sales_deals_month) as sales_deals_month,
round(p.sales_former_month) as sales_former_month,
p.distinct_cust_deals_month,
p.distinct_cust_former_month


(p.orders_deals_month::double precision / nullif(p.orders_former_month,0)::double precision) -1 as times_or_perc_growth_orders,
(p.sales_deals_month::double precision / nullif(p.orders_deals_month,0)::double precision)::int as AFV_deal_month,
d.banner_url

FROM data_set d
LEFT JOIN penultimate p
ON d.rdbms_id=p.rdbms_id and d.vendor_code=p.vendor_code

LEFT JOIN emails e 
on e.country=d.country_name AND e.vendor_code=p.vendor_code




)

select *, 
case when times_or_perc_growth_orders > 2 then round(((times_or_perc_growth_orders)::int))::text||' times'
     else round((((times_or_perc_growth_orders)::decimal(20,2))*100::double precision))::text||' %'  
     end as growth_orders,
case when times_or_perc_growth_orders >= 0.10 then 'True'
     else 'False'
     end as display_growth

from final






