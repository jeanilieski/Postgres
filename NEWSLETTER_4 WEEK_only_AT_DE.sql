


DROP TABLE IF EXISTS eligible_vendors;
CREATE TEMPORARY TABLE eligible_vendors AS (
--select vendor_name, address_line1, address_line2, postcode 
--from dwh_il_fo.dim_Vendors
select rdbms_id, country_name, count(*)
from (
select z."Street Name" as street_name,  z."Post Code" as post_code, z."Activated Date" as activation_date, v.rdbms_id, z."Account Name", 
a.country_name    --,  v.vendor_name --, "Account Name", "Account ID", "18 Char Account ID" , * --"Shipping Zip/Postal Code", a.preferred_contact_language, 

from salesforce_fo.foodora_all_accounts z   -- select * from salesforce_fo.foodora_all_accounts limit 5

left join salesforce_fo.il_dim_accounts a  -- select * from salesforce_fo.il_dim_accounts (has not preffered language)
on a.account_id=z."18 Char Account ID"  

left join dwh_il_fo.dim_vendors v
ON v.vendor_code=a.vendor_code  and v.rdbms_id=a.rdbms_id 

LEFT JOIN dwh_il.dim_countries co
on a.country_name=co.common_name                                 

where z."Account Status" in ('Active') 
AND z."Activated Date" BETWEEN ( current_date -'6 week'::interval) AND ( current_date -'5 week'::interval - '1 day'::interval) --2017-12-17 and 2017-12-24   
AND v.rdbms_id in (80,81,88,86,87,84,137,85,83,95) 
order by 3 desc 
--limit 7
--select current_date -'4 week'::interval - '1 day'::interval--2017-12-17 and 2017-12-23
)sq
group by 1 ,2

--BETWEEN 25 AND 24:  349
--BETWEEN 6 AND 5 W: 440
--BETWEEN 5 AND 4:  389  --2017-12-17 and 2017-12-24
--BETWEEN 4 AND 3:  137  --2017-12-24 and 2017-12-31
--BETWEEN 3 AND 2:  195  --2017-12-24 and 2017-12-31

; --SELECT * FROM eligible_vendors




DROP TABLE IF EXISTS time_params;
CREATE TEMPORARY TABLE time_params AS ( --SELECT * FROM  time_params
 select *, 
 case when        
        
        SELECT
        '1 hour'::INTERVAL as period,
        '07:00:00'::TIME as start_time,
        '24:00:00'::TIME as end_time,
        date_trunc('week', current_date -'4 week'::interval)::date as start_date, -- (2017-12-18)
        --date_trunc('week', current_date -'0 week'::interval)::date - interval '1' day as end_date   (2018-01-08)
        (current_date - interval '2' day)::date AS end_date                         --select current_date - 1 (2018-01-08)
        ( current_date -'5 week'::interval) AND ( current_date -'4 week'::interval - '1 day'::interval) 
);


     
SELECT NOW() AS time_params;

ANALYZE time_params; 

DROP TABLE IF EXISTS dates;
CREATE TEMPORARY TABLE dates as(   --SELECT * FROM  dates
        
        SELECT  
        DISTINCT
        iso_date,
        iso_full_week_string AS week,
        iso_full_month_string AS month,
        iso_digit_week,
        iso_digit_day_of_week
        FROM dwh_il.dim_date
        WHERE iso_date::date BETWEEN (SELECT start_date from time_params) AND (SELECT end_date FROM time_params)
     
     
);



SELECT NOW() as dates;
ANALYZE dates; 

                   
               

-----------------------------------------------------------------------
----------------------ORDER DATA------------------ --------------------
-----------------------------------------------------------------------

DROP TABLE IF EXISTS hurrier_info;
CREATE TEMPORARY TABLE hurrier_info AS ( --HURRIER DATA AND ZD DATA

WITH hurrier_info AS(
SELECT                       
ops.backend_rdbms_id as rdbms_id, 
ops.order_id,
ops.order_code as order_code_google,

-----------------------------------------------CALCULATION OF VENDOR DELAYS---------------------------------------------------------------
CASE WHEN 
------------case when rider earlier than Expected PU time ---------------------------------------
(case when redelivery IS FALSE then courier_late end) < 0
THEN EXTRACT (EPOCH FROM (ops.food_picked_up - ops.dispatcher_expected_pick_up_time)/60)
WHEN 
------------case when rider lated than Expected PU time, but not too late (10 min)---------------------------
(case when redelivery IS FALSE then courier_late end) BETWEEN 0 and 10
THEN EXTRACT (EPOCH FROM (ops.food_picked_up - ops.dispatcher_expected_pick_up_time)/60) - courier_late
END AS vendor_late_cleaned

FROM dm_ops_fo.fct_orders ops
LEFT JOIN dm_ops_fo.fct_deliveries d USING (rdbms_id, disp_order_id)
WHERE d.delivery_status='completed' 
--- TIMEFRAME, ADJUST AS NEEDED
and  date_trunc('week' , ops.created_at::date) >= date_trunc('week', current_date - interval '1 week')
)

----FOODORA DATA
SELECT
o.rdbms_id, 
o.vendor_id, 
to_char(o.order_date,'iyyy-iw') as week, --ATTENTION
COUNT (DISTINCT o.order_id) AS valid_orders,
COUNT (DISTINCT o.order_id) FILTER (WHERE h.vendor_late_cleaned>5) as vendor_late_cleaned_5_min,
COUNT (DISTINCT o.order_id) FILTER (WHERE h.vendor_late_cleaned>10) as vendor_late_cleaned_10_min,
---NUMBER OF OBSERVATIONS --> USED TO DETERMINE % VENDOR DELAY
COUNT (DISTINCT o.order_id) FILTER (WHERE h.vendor_late_cleaned IS NOT NULL) as vendor_delay_observations  

FROM dwh_il_fo.fct_orders o
LEFT JOIN hurrier_info h USING (rdbms_id,order_code_google)
LEFT JOIN dwh_il_fo.meta_order_status s USING (rdbms_id, status_id)

WHERE s.valid_order=1 
--- TIMEFRAME, ADJUST AS NEEDED
and  date_trunc('week' , o.order_date::date) >= date_trunc('week', current_date - interval '1 week')
and o.rdbms_id in (80,81)
GROUP BY 1,2,3      
                       
        
);




SELECT NOW() AS hurrier_info;

ANALYZE hurrier_info; 



DROP TABLE IF EXISTS fo_fct_daily_orders;
CREATE TEMPORARY TABLE fo_fct_daily_orders AS ( 
        SELECT
            d.4_weeks, --ATTENTON !!!
            o.rdbms_id,
            v.vendor_id,
            c.currency_code,
            d.iso_date,
            d.iso_digit_week, --ATTENTON !!!
            o.order_id,
            o.order_code_google,
            s.valid_order,
            a.products_plus_vat AS gfv_local,
            s.failed_order_vendor,
            h.vendor_late,
            o.first_order,
            h.order_id as order_id_hu,

            COUNT (DISTINCT o.order_id) FILTER (WHERE s.gross_order=1 AND '53' = ANY (ot.code_array) ) AS overdue_order,
            COUNT(DISTINCT o.order_id ) FILTER (WHERE customer_contact_reason_updated ~* '3B.' and s.valid_order=1) as missing_items,
            COUNT(DISTINCT o.order_id ) FILTER (WHERE customer_contact_reason_updated ~* '3C.' and s.valid_order=1) as wrong_items,
            COUNT(DISTINCT o.order_id ) FILTER (WHERE customer_contact_reason_updated ~* '3E.' and s.valid_order=1) as damaged_packaging
            
            
        FROM dates d
        LEFT JOIN dwh_il_fo.fct_orders o ON d.iso_date=o.order_date::date 
        LEFT JOIN dwh_il.dim_countries c USING (rdbms_id)
        LEFT JOIN hurrier_info h USING (rdbms_id, order_id)
        LEFT JOIN dwh_il_fo.fct_accounting a ON a.rdbms_id=o.rdbms_id AND o.order_id=a.order_id
        LEFT JOIN dwh_il_fo.fct_zendesk z ON o.rdbms_id=z.rdbms_id AND o.order_code_google=z.order_code
        LEFT JOIN dwh_il_fo.meta_order_status s ON o.rdbms_id=s.rdbms_id AND s.status_id=o.status_id 
        LEFT JOIN dwh_il_fo.dim_vendors v ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id
        LEFT JOIN dwh_il_fo.dim_ops_timestamps ot ON o.rdbms_id=ot.rdbms_id AND o.order_id=ot.order_id


        WHERE d.iso_date >=  (SELECT start_date FROM time_params) 
        AND d.iso_date < (SELECT end_date FROM time_params) and s.gross_order = 1 
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
);
        
        
        
SELECT NOW() AS fo_fct_daily_orders;

ANALYZE fo_fct_daily_orders; 


DROP TABLE IF EXISTS top_dishes;
CREATE TEMPORARY TABLE top_dishes AS(  --TOP DISHES
SELECT 
rdbms_id,
vendor_id,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=1 then product end  ),',') as FIRST_title,   
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=2 then product end  ),',') as SECOND_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=3 then product end  ),',') as THIRD_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=4 then product end  ),',') as FOURTH_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=5 then product end  ),',') as FIFTH_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=1 then perc ELSE null end  ),',') as FIRST_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=2 then perc ELSE null end  ),',') as SECOND_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=3 then perc ELSE null end  ),',') as THIRD_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=4 then perc ELSE null end  ),',') as FOURTH_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=5 then perc ELSE null end  ),',') as FIFTH_quant,
SUM (perc) as total_perc

FROM (

        SELECT 
        rdbms_id,
        vendor_id,
        product,
        quantity,
        ROW_NUMBER() OVER( PARTITION BY rdbms_id, vendor_id ORDER BY quantity desc) as rank,
        trunc((quantity/SUM(quantity) OVER( PARTITION BY rdbms_id, vendor_id)::decimal)*100, 1) as perc,
        SUM(quantity) OVER( PARTITION BY rdbms_id, vendor_id)
        
        FROM (

                SELECT
                o.rdbms_id,
                o.vendor_id,
                product_name AS product,
                SUM(quantity) as quantity
                
                FROM fp_fct_daily_orders o
                LEFT JOIN dwh_il.dim_orderproducts p  
                ON o.rdbms_id=p.rdbms_id AND o.order_id=p.order_id AND p.rdbms_id IN (7,42,19,16,20,15,18,17,12)
                WHERE  to_char(o.iso_date, 'iyyy-im')=to_char(current_date - 30, 'iyyy-im') AND valid_order=1 AND product_name!~'Cutlery' AND product_name!~'Napkins'
                
                GROUP BY 1,2,3
                ) a
        ) b where rank <=5
                
group by 1,2

UNION ALL 


SELECT 
rdbms_id,
vendor_id,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=1 then product end  ),',') as FIRST_title,   
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=2 then product end  ),',') as SECOND_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=3 then product end  ),',') as THIRD_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=4 then product end  ),',') as FOURTH_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=5 then product end  ),',') as FIFTH_title,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=1 then perc ELSE null end  ),',') as FIRST_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=2 then perc ELSE null end  ),',') as SECOND_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=3 then perc ELSE null end  ),',') as THIRD_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=4 then perc ELSE null end  ),',') as FOURTH_quant,
ARRAY_TO_STRING(ARRAY_AGG( CASE WHEN rank=5 then perc ELSE null end  ),',') as FIFTH_quant,
SUM (perc) as total_perc

FROM (

        SELECT 
        rdbms_id,
        vendor_id,
        product,
        quantity,
        ROW_NUMBER() OVER( PARTITION BY rdbms_id, vendor_id ORDER BY quantity desc) as rank,
        trunc((quantity/SUM(quantity) OVER( PARTITION BY rdbms_id, vendor_id)::decimal)*100, 1) as perc,
        SUM(quantity) OVER( PARTITION BY rdbms_id, vendor_id)
        
        FROM (
        
                SELECT
                o.rdbms_id,
                o.vendor_id,
                orderproduct_title AS product,
                SUM(quantity) as quantity
                
                FROM fo_fct_daily_orders o
                LEFT JOIN  dwh_il_fo.fct_orderproducts p 
                ON o.rdbms_id=p.rdbms_id AND o.order_id=p.order_id 
                WHERE  to_char(o.iso_date, 'iyyy-im')=to_char(current_date - 30, 'iyyy-im') 
                AND valid_order=1 AND orderproduct_title!~'Cutlery' AND orderproduct_title!~'Napkins'
                
                GROUP BY 1,2,3
                ) a
        ) b where rank <=5

group by 1,2

);




SELECT NOW() AS top_dishes; 

ANALYZE top_dishes; 
--SELECT * FROM TOP_DISHES WHERE RDBMS_ID=80 AND vendor_id IN (7124)
--




DROP TABLE IF EXISTS agg_order_data;
CREATE TEMPORARY TABLE agg_order_data AS(--AGGREGATED ORDER DATA   --SELECT * FROM agg_order_data where rdbms_id =42

        
        SELECT 
        o.4_weeks, --ATTENTION !!!
        o.rdbms_id,
        o.vendor_id,
        o.currency_code,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.valid_order= 1) as valid_orders,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.valid_order= 1 AND (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) AS orders_w_1,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.valid_order= 1 AND (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) AS orders_w_2,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.valid_order= 1 AND (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params))) orders_w_3,
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.valid_order= 1 AND (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params))) AS orders_w_4,
    

        SUM(o.gfv_local) FILTER (WHERE o.valid_order= 1) AS gmv_local,
        AVG (o.gfv_local) FILTER (WHERE o.valid_order= 1) AS afv,
        
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.failed_order_vendor=1) AS declined_orders,
            COUNT(DISTINCT o.order_id) FILTER (WHERE o.failed_order_vendor=1 AND (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) AS declined_orders_w_1,
            COUNT(DISTINCT o.order_id) FILTER (WHERE o.failed_order_vendor=1 AND (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) AS declined_orders_w_2,
            COUNT(DISTINCT o.order_id) FILTER (WHERE o.failed_order_vendor=1 AND (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params))) AS declined_orders_w_3,
            COUNT(DISTINCT o.order_id) FILTER (WHERE o.failed_order_vendor=1 AND (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params)))  AS declined_orders_w_4,

        SUM(o.gfv_local) FILTER (WHERE o.failed_order_vendor=1) AS lost_gfv_all_declined,
        
        COUNT(DISTINCT o.order_id) FILTER (WHERE overdue_order=1) as overdue_orders,
        COUNT(DISTINCT o.order_id ) FILTER (WHERE missing_items=1) as missing_items,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE missing_items=1 AND (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) as missing_items_w_1,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE missing_items=1 AND (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) as missing_items_w_2,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE missing_items=1 AND (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params)))  as missing_items_w_3,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE missing_items=1 AND (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params)))  as missing_items_w_4,
                
        COUNT(DISTINCT o.order_id ) FILTER (WHERE wrong_items=1 ) as wrong_items,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE wrong_items=1 AND (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) as wrong_items_w_1,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE wrong_items=1 AND (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) as wrong_items_w_2,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE wrong_items=1 AND (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params)))  as wrong_items_w_3,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE wrong_items=1 AND (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params)))  as wrong_items_w_4,
                
        COUNT(DISTINCT o.order_id ) FILTER (WHERE damaged_packaging=1 ) as damaged_packaging,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE damaged_packaging=1 AND (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) as damaged_packaging_w_1,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE damaged_packaging=1 AND (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) as damaged_packaging_w_2,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE damaged_packaging=1 AND (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params)))  as damaged_packaging_w_3,
                COUNT(DISTINCT o.order_id ) FILTER (WHERE damaged_packaging=1 AND (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params)))  as damaged_packaging_w_4,
                
        
        COUNT(o.vendor_late) AS vendor_late,
                COUNT(o.vendor_late) FILTER (WHERE (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) AS vendor_late_w_1,
                COUNT(o.vendor_late) FILTER (WHERE (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) AS vendor_late_w_2,
                COUNT(o.vendor_late) FILTER (WHERE (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params))) AS vendor_late_w_3,
                COUNT(o.vendor_late) FILTER (WHERE (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params))) AS vendor_late_w_4,
                
                
        COUNT(DISTINCT o.order_id) FILTER (WHERE o.first_order= 1) as new_cust,
        COUNT(DISTINCT order_id_hu) AS valid_orders_hu
        

        FROM fo_fct_daily_orders o
        GROUP BY 1,2,3,4

);



SELECT NOW() AS agg_order_data; 

ANALYZE agg_order_data; 


DROP TABLE IF EXISTS monthly_data; --weekly_data;  --ATTENTION !!!
CREATE TEMPORARY TABLE monthly_data AS (
        select
            d.rdbms_id,
            d.vendor_id,
            4_weeks, --ATTENTION !!!
            --closed_hours_num_order_declined, closed_hours_num_vbe, closed_hours_num_internal (we close them)
            
--            SUM(closed_hours_num) AS offline_hours,
--            sum(closed_hours_num_order_declined) as declined, 
--            sum(closed_hours_num_vbe) as vbe, 
--            sum(closed_hours_num_internal) as internal,
--            SUM(closed_hours_num - closed_hours_num_order_declined) as offline_hours,
            SUM(closed_hours_num) - sum(closed_hours_num_order_declined) as offline_hours,
            --closed_hours_num_order_declined, closed_hours_num_vbe, closed_hours_num_internal (we close them)
            SUM(open_hours_num) AS open_hours
        
            SUM(open_hours_num) AS open_hours_num, --why open_hours_num is 1 even when in the same row we have e.g. offline h 27 min?
            SUM(offline_hours) FILTER (WHERE (o.iso_date between (select start_date from time_params) and (select start_date + interval '6' day from time_params))) AS offline_hours_w_1,
            SUM(offline_hours) FILTER (WHERE (o.iso_date between (select start_date + interval '7' day from time_params) and (select start_date + interval '13' day from time_params))) AS offline_hours_w_2,
            SUM(offline_hours) FILTER (WHERE (o.iso_date between (select start_date + interval '14' day from time_params) and (select start_date + interval '20' day from time_params))) AS offline_hours_w_3,
            SUM(offline_hours) FILTER (WHERE (o.iso_date between (select start_date + interval '21' day from time_params) and (select start_date + interval '27' day from time_params))) AS offline_hours_w_4,
                
        
        from dwh_bl.restaurant_offline_report 
        WHERE closed_hours_num < open_hours_num AND closed_hours_num > 0 and rdbms_id in (80, 81)
        group by 1,2,3

--select * from dwh_bl.restaurant_offline_report where closed_hours_num_vbe is not null   
--coalesce(every metric of offline report) --we do COALESCE FURTHER DOWN, SHOULD i DO IT HERE AS WELL?

);

SELECT NOW() AS monthly_data; 
ANALYZE monthly_data; 




-----------------------------------------------------------------------
---------------------------OFFLINE & CLOSED EVENTS --------------------
-----------------------------------------------------------------------

DROP TABLE IF EXISTS vendors_config_data;
CREATE TEMPORARY TABLE vendors_config_data AS( --VENDORS CONFIGURATION DATA

        SELECT
        v.rdbms_id,
        v.vendor_id,
        v.city_id,
        v.vendor_code,
        v.vendor_name,
        v.banner_url,
        v.email as backend_email,
        CASE WHEN AVG (p.prep_time) IS NULL THEN CASE WHEN AVG(pickup_time) < 5 THEN 10 ELSE AVG(pickup_time) END ELSE AVG(p.prep_time) END AS prep_time_avg
        FROM (
                SELECT v.rdbms_id, v.vendor_name, v.vendor_id, v.vendor_code, v.city_id, v.vendor_active, v.vendor_deleted, v.vendor_testing, v.pickup_time, email,
                CASE WHEN cu.globalcuisine_id IN (19,18,17,83,54,3,81) THEN 'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/american_banner.jpg'
                WHEN cu.globalcuisine_id IN (30,76,82,72,64,106) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/arabic_banner.jpg'
                WHEN cu.globalcuisine_id IN (51,67,29,80,124,79,43) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/argentinian_banner.jpg'
                WHEN cu.globalcuisine_id IN (7,46,28,92,68,41,88,125,6,8) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/asian_banner.jpg'
                WHEN cu.globalcuisine_id IN (9,128,22,38,60,16,42) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/cakes_bakery_banner.jpg'
                WHEN cu.globalcuisine_id IN (15,13,66,97,37,89,84,34,49,47) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/european_banner.jpg'
                WHEN cu.globalcuisine_id IN (65,26,116,104,10,25,131,21) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/healthy_food_banner.jpg'
                WHEN cu.globalcuisine_id IN (12,91,69,101) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/indian_banner.jpg'
                WHEN cu.globalcuisine_id IN (11,52,122,27,39,112,40,57,32,48,86,93) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/international_banner.jpg'
                WHEN cu.globalcuisine_id IN (1,35,5) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/italian_banner.jpg'
                WHEN cu.globalcuisine_id IN (2,53) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/mexican_banner.jpg'
                WHEN cu.globalcuisine_id IN (44,110,45,33,63,55,31,23) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/oriental_banner.jpg'
                WHEN cu.globalcuisine_id IN (14,50) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/seafood_banner.jpg'
                WHEN cu.globalcuisine_id IN (20,4) THEN'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/sushi_banner.jpg'
                ELSE 'http://volo-images.s3.amazonaws.com/CRM/partners/performance/banners/european_banner.jpg'
                END AS banner_url
                FROM dwh_il_fo.dim_vendors v
                LEFT JOIN dwh_il_fo.dim_vendorscuisines_fp cu 
                ON cu.rdbms_id=v.rdbms_id AND v.vendor_id=cu.vendor_id AND v.primary_cuisine_id=cu.cuisine_id
                
            ) v 
        LEFT JOIN dwh_il.dim_countries co 
        ON v.rdbms_id=co.rdbms_id
        LEFT JOIN dwh_metadata.countries_of_the_world cow 
        on co.common_name=cow.isoen_name
        LEFT JOIN quant_fo.dpt_bucket_table p 
        ON p.country_code=cow.iso3166a2 AND p.vendor_code=v.vendor_code 
        WHERE v.vendor_active=1 AND vendor_deleted=0 AND v.vendor_code is not null 
        GROUP BY 1,2,3,4,5,6,7
);
SELECT NOW() AS vendors_config_data;

ANALYZE weekly_data; 




DROP TABLE IF EXISTS emails; --EMAILS
CREATE TEMPORARY TABLE emails AS(

SELECT 
* 
FROM (
        select
        sf."Country" as country,
        sf."Partner Backend Code" as vendor_code, 
        sf."Email",
        sf."Partner Backend Code"||'_'||row_number() over (partition by "Country", "Partner Backend Code") as sbubscriberkey_end,
        row_number() over (partition by "Country", "Partner Backend Code") as row
        
        FROM salesforce_fo.foodora_all_partner_billing_contacts sf 
        WHERE "Exclude From All Newsletters"=0
        
        
)a where row=1

);


DROP TABLE IF EXISTS partner_sendout;
CREATE TEMPORARY TABLE partner_sendout AS(  -- SENDOUT

       SELECT
        d.week,
        d.rdbms_id,
        cow.iso3166a3||'_'||CASE WHEN e.sbubscriberkey_end IS NULL THEN p.vendor_code else  e.sbubscriberkey_end end AS "SubscriberKey",
        co.common_name AS country,
        ci.city_name,
        d.vendor_id,
        p.vendor_name,
        p.vendor_code,
        CASE WHEN "Email" IS NULL THEN backend_email ELSE "Email" END AS "Email",
        d.currency_code,
        banner_url,
        round(d.afv) AS afv,
        new_cust,
        ------------------------------------------------------ORDERS---------------------------------
        CASE WHEN d.valid_orders <= 10 AND prep_time_avg<10 THEN 10 ELSE ROUND(p.prep_time_avg::numeric, 0)  END AS prep_time_avg,
        d.valid_orders,
        d.orders_w_1,
        d.orders_w_2,
        d.orders_w_3,
        d.orders_w_4,
        
--        d.New_cust,
        trunc(d.gmv_local::numeric, 2) as gmv_local,
        
        ------------------------------------------------------VENDOR DELAYS---------------------------------
        d.vendor_late,
        d.vendor_late_w_1,
        d.vendor_late_w_2,
        d.vendor_late_w_3,
        d.vendor_late_w_4,
        
        
        CASE WHEN (d.valid_orders>0) 
                THEN round(d.vendor_late::numeric/d.valid_orders,2)
                ELSE NULL END AS vendor_late_percentage,
        
        ------------------------------------------------------RIDER DELAYS---------------------------------
        FIRST_title,   
        SECOND_title,
        THIRD_title,
        FOURTH_title,
        FIFTH_title,
        FIRST_quant,
        SECOND_quant,
        THIRD_quant,
        FOURTH_quant,
        FIFTH_quant,
        total_perc,
        ------------------------------------------------------ALL CANCELLATIONS---------------------------------
        d.declined_orders,
        d.declined_orders_w_1,
        d.declined_orders_w_2,
        d.declined_orders_w_3,
        d.declined_orders_w_4, 
        -- cancel_order_perc    -- %
                    
        COALESCE (d.lost_gfv_all_declined,0) AS declined_gmv,
        d.overdue_orders,
          
        ----------------------------------------------------PACKAGING, MISSING AND WRONG ITEMS---------------------------------
        d.damaged_packaging,
        d.damaged_packaging_w_1,
        d.damaged_packaging_w_2,
        d.damaged_packaging_w_3,
        d.damaged_packaging_w_4,
               
        d.wrong_items + d.missing_items AS missing_wrong_items,  --missing_wrong_items
        d.wrong_items_w_1 + d.missing_items_w_1 AS missing_wrong_items_w_1,
        d.wrong_items_w_2 + d.missing_items_w_2 AS missing_wrong_items_w_2,
        d.wrong_items_w_3 + d.missing_items_w_3 AS missing_wrong_items_w_3,
        d.wrong_items_w_4 + d.missing_items_w_4 AS missing_wrong_items_w_4,
          
        d.wrong_items + d.missing_items + d.damaged_packaging as cust_compl, -- Customer complaints include orders with wrong items, missing items and/or damaged packaging
        d.wrong_items_w_1 + d.missing_items_w_1 +  d.damaged_packaging_w_1  as cust_compl_w_1,
        d.wrong_items_w_2 + d.missing_items_w_2 +  d.damaged_packaging_w_2  as cust_compl_w_2,
        d.wrong_items_w_3 + d.missing_items_w_3 +  d.damaged_packaging_w_3  as cust_compl_w_3,
        d.wrong_items_w_4 + d.missing_items_w_4 +  d.damaged_packaging_w_4  as cust_compl_w_4,
        
        CASE WHEN d.valid_orders_hu>0 AND (wrong_items + missing_items)>0  --_HU OUT, RIGHT?
                THEN Round((d.wrong_items::numeric +d.missing_items::numeric)/d.valid_orders_hu,2)
                ELSE 0 END AS total_missing_wrong_items_percentage,  -- %
                
        CASE WHEN d.valid_orders_hu>0 AND damaged_packaging>0
                THEN Round(d.damaged_packaging::numeric/d.valid_orders_hu,2)
                ELSE 0 END AS total_damaged_packaging_percentage,  -- %         
--I CREADED:               
        CASE WHEN d.valid_orders_hu>0 AND declined_orders>0
                THEN Round(d.declined_orders::numeric/d.valid_orders_hu,2)
                ELSE 0 END AS cancel_order_perc,  --total_declined_orders_percentage --%


        ----------------------------------------------------CLOSING HOURS---------------------------------
        COALESCE(offline_hours,0) AS offline_hours,          
        COALESCE(offline_hours_w_1,0) AS offline_hours_w_1,
        COALESCE(offline_hours_w_2,0) AS offline_hours_w_2,
        COALESCE(offline_hours_w_3,0) AS offline_hours_w_3,
        COALESCE(offline_hours_w_4,0) AS offline_hours_w_4,
           
        
        
        FROM agg_order_data d
        LEFT JOIN vendors_config_data p --p
        USING (rdbms_id, vendor_id)
        LEFT JOIN (
                SELECT rdbms_id, city_id, city_name 
                FROM dwh_il_fo.dim_city 
        ) ci 
        ON ci.rdbms_id=p.rdbms_id AND ci.city_id=p.city_id
        
        LEFT JOIN dwh_il.dim_countries co 
        ON co.rdbms_id=d.rdbms_id
        LEFT JOIN dwh_metadata.countries_of_the_world cow 
        on co.common_name=cow.isoen_name 
        LEFT JOIN emails e 
        on co.common_name=e.country AND e.vendor_code=p.vendor_code
        LEFT JOIN top_dishes td 
        ON  d.rdbms_id=td.rdbms_id AND td.vendor_id=d.vendor_id
        LEFT JOIN weekly_data w 
        ON d.rdbms_id=w.rdbms_id AND w.vendor_id=d.vendor_id AND w.week=d.week  --ATTENTION !!!
        WHERE d.valid_orders>0 

);
SELECT NOW();
DROP TABLE IF EXISTS city_max;
CREATE TEMPORARY TABLE city_max AS(
        SELECT DISTINCT
        week,
        rdbms_id,
        city_name,
        MAX (valid_orders) OVER (PARTITION BY rdbms_id, week, city_name) AS max_orders
        
        FROM partner_sendout
);

SELECT NOW();
DROP TABLE IF EXISTS city_bencmark;
CREATE TEMPORARY TABLE city_bencmark AS (

       SELECT 
        week,
        rdbms_id, 
        city_name,
        bench_vendors,
        CASE    WHEN benchmark_valid_orders < 10 THEN benchmark_valid_orders *4 
                WHEN benchmark_valid_orders < 20 THEN benchmark_valid_orders *2
                ELSE benchmark_valid_orders  END 
        AS benchmark_valid_orders, 
        CASE    WHEN benchmark_cancellation > 6 THEN 3 
                WHEN benchmark_cancellation > 3 AND benchmark_cancellation <= 6 THEN ROUND (benchmark_cancellation *.5, 0) 
                ELSE benchmark_cancellation END
        AS benchmark_cancellation_num,
        CASE    WHEN benchmark_cancellation > 6 THEN benchmark_cancellations_gmv_per_order * 3
                WHEN benchmark_cancellation > 3 THEN benchmark_cancellations_gmv *.5
                ELSE benchmark_cancellations_gmv END
        AS benchmark_cancellations_gmv,
        CASE   WHEN benchmark_delay::numeric / benchmark_valid_orders::numeric > 0.1 
               THEN ROUND ((benchmark_valid_orders*((random()*(11-8)+8)/100))::numeric,0)
               ELSE benchmark_delay END
        AS benchmark_delay_num,
        CASE   WHEN benchmark_packaging > 2 THEN round((random()*(3-1)+1)::numeric, 0)
               ELSE benchmark_packaging END
        AS benchmark_packaging_num,
        CASE   WHEN benchmark_missing_wrong > 3 THEN round((random()*(4-2)+2)::numeric, 0)
               ELSE benchmark_missing_wrong END
        AS benchmark_missing_wrong_num,
        CASE   WHEN benchmak_offline > 3 THEN round((random()*(3-1)+1)::numeric, 1)
               ELSE benchmak_offline END
        AS benchmak_offline_num,
        CASE    WHEN benchmark_overdue > 6 THEN 3 
                WHEN benchmark_overdue > 3 AND benchmark_overdue <= 6 THEN ROUND (benchmark_overdue *.5, 0) 
                ELSE benchmark_overdue END
        AS benchmark_overdue_num ,
        

--I CREATED THESE:
        CASE WHEN benchmark_valid_orders>0 AND benchmark_missing_wrong>0   
                THEN Round(d.benchmark_missing_wrong::numeric/benchmark_valid_orders,2)
                ELSE 0 END AS benchmark_missing_wrong_perc,

        CASE WHEN benchmark_valid_orders>0 AND benchmark_packaging>0
                THEN Round(d.benchmark_packaging::numeric/benchmark_valid_orders,2)
                ELSE 0 END AS benchmark_packaging_perc,
                
        CASE WHEN benchmark_valid_orders>0 AND benchmark_cancellation>0
                THEN Round(d.benchmark_cancellation::numeric/benchmark_valid_orders,2)
                ELSE 0 END AS benchmark_cancellation_perc,        
                
        
        
        FROM (
                SELECT 
                week,
                rdbms_id, 
                city_name,
                bench_vendors,
                ROUND (SUM ( valid_orders) / MAX(bench_vendors), 0) AS benchmark_valid_orders, 
                ROUND (SUM ( declined_orders) / MAX(bench_vendors), 0) AS benchmark_cancellation,
                ROUND (SUM ( declined_gmv::numeric) / MAX(bench_vendors), 1) AS benchmark_cancellations_gmv,
                CASE    WHEN SUM(declined_orders)>0 THEN 
                        ROUND (SUM ( declined_gmv::numeric) / SUM(declined_orders), 1) 
                        ELSE 0 END AS benchmark_cancellations_gmv_per_order,
                ROUND (SUM ( vendor_late) / MAX(bench_vendors), 0) AS benchmark_delay,
                ROUND (SUM ( overdue_orders) / MAX(bench_vendors), 0) AS benchmark_overdue,
                ROUND (SUM ( damaged_packaging) / MAX(bench_vendors), 0) AS benchmark_packaging, 
                ROUND (SUM ( missing_wrong_items) / MAX(bench_vendors), 0) AS benchmark_missing_wrong, 
                ROUND (SUM ( restaurant_closed_hours_per_double_offline) / MAX(bench_vendors), 1) AS benchmak_offline
                
                FROM (
                        
                        SELECT *, 
                        MAX(rank_vendors) OVER (PARTITION BY rdbms_id, week, city_name) AS bench_vendors 
                        FROM (
                                SELECT 
                                s.week,
                                s.rdbms_id, 
                                s.city_name,
                                vendor_id,
                                valid_orders,
                                declined_orders,
                                declined_gmv,
                                vendor_late, 
                                damaged_packaging,
                                missing_wrong_items,
                                overdue_orders,
                                COALESCE (restaurant_closed_hours_per_double_offline,0.0) AS restaurant_closed_hours_per_double_offline,
                                row_number() OVER (PARTITION BY s.rdbms_id, s.week, s.city_name ORDER BY valid_orders DESC ) as rank_vendors
        
                                from partner_sendout s
                                LEFT JOIN city_max c USING (rdbms_id, week, city_name) 
                                WHERE valid_orders > CASE WHEN max_orders > 250 THEN 75 ELSE (max_orders*.3) END order by 1,2,3
                                 ) a 
                         ) b GROUP BY 1,2,3,4 )c
);



SELECT NOW();
DROP TABLE IF EXISTS final;
CREATE TEMPORARY TABLE final AS(
         SELECT 
        *, 
        CASE WHEN prep_time_avg < 15 AND prep_time_avg >= 12 THEN prep_time_avg-2 
        WHEN prep_time_avg < 12 THEN 10
        ELSE 15 END AS prep_time_benchmark
        
         FROM partner_sendout
);

SELECT NOW();
DROP TABLE IF EXISTS scoring;
CREATE TEMPORARY TABLE scoring AS(

        SELECT 
        rdbms_id, city_name, week, vendor_id,
        CASE  WHEN quad_canc * 5 >= (quad_delay + quad_missing_wrong + quad_pack + quad_off + quad_prep) AND quad_canc> 0 THEN 1
        WHEN quad_delay * 5 >= (quad_canc + quad_missing_wrong + quad_pack + quad_off + quad_prep ) AND vendor_late_percentage> .03 THEN 2
        WHEN quad_missing_wrong * 5 >= (quad_canc + quad_delay + quad_pack + quad_off + quad_prep )AND quad_missing_wrong> 0 THEN 3
        WHEN quad_pack * 5 >= (quad_canc + quad_delay + quad_missing_wrong + quad_off + quad_prep )AND quad_pack> 0 THEN  4
        WHEN quad_off * 5 >= (quad_canc + quad_delay + quad_missing_wrong + quad_pack + quad_prep )AND   quad_off >0 THEN 5
        WHEN quad_prep > 0 AND (quad_pack+ quad_missing_wrong+ quad_canc + quad_off )> 0 THEN 6
        ELSE 7
        END AS improve_opportunity_num
        
        from (
                SELECT  rdbms_id, city_name, week, vendor_id,vendor_late_percentage, 
                (GREATEST(prep_time_avg,15)-15)/300*100 as quad_prep, 
                (declined_orders::numeric/valid_orders::numeric)*100 as quad_canc, 
                ((restaurant_closed_hours_per_double_offline/150))*100 as quad_off, 
                vendor_late_percentage*100 as quad_delay, 
                total_missing_wrong_items_percentage*100 as quad_missing_wrong,
                total_damaged_packaging_percentage*100 as quad_pack
                
                FROM final f 
                where f.week = to_char(current_date-7, 'iyyy-iw') ) a 
);

SELECT NOW();

--TRUNCATE TABLE crm_campaigns_fo.et_vendor_performance_sendout;
--INSERT INTO crm_campaigns_fo.et_vendor_performance_sendout

--------------------------FROM HERE DELETE
--DROP TABLE IF EXISTS crm_campaigns_fo.et_vendor_performance_sendout;
--CREATE TABLE crm_campaigns_fo.et_vendor_performance_sendout AS (

DROP TABLE IF EXISTS dwh_st.zhan_vendor_performance_sendout_AU_DE;

CREATE TABLE dwh_st.zhan_vendor_performance_sendout_AU_DE AS (
SELECT DISTINCT
date_trunc('week', current_date -'1 week'::interval)::date as start_date,
(date_trunc('week', current_date) - '1sec'::interval)::date as end_date,
dp.type,
"SubscriberKey",
"Email",
f.week,
f.rdbms_id,
f.country,
f.city_name,
f.vendor_id,
f.vendor_name,
f.currency_code,
f.banner_url,
f.afv,
f.new_cust,
f.prep_time_avg,
f.valid_orders,
f.orders_w_1,
f.orders_w_2,
f.orders_w_3,
f.orders_w_4,
f.gmv_local,
f.vendor_late,
f.vendor_late_w_1,
f.vendor_late_w_2,
f.vendor_late_w_3,
f.vendor_late_w_4,
f.vendor_late_percentage,
f.first_title,
f.second_title,
f.third_title,
f.fourth_title,
f.fifth_title,
f.first_quant,
f.second_quant,
f.third_quant,
f.fourth_quant,
f.fifth_quant,
f.total_perc,
f.declined_orders,
f.declined_orders_w_1,
f.declined_orders_w_2,
f.declined_orders_w_3,
f.declined_orders_w_4,
f.declined_gmv,
f.overdue_orders,
f.damaged_packaging,
f.damaged_packaging_w_1,
f.damaged_packaging_w_2,
f.damaged_packaging_w_3,
f.damaged_packaging_w_4,
f.total_damaged_packaging_percentage, --found
f.missing_wrong_items,
f.missing_wrong_items_w_1,
f.missing_wrong_items_w_2,
f.missing_wrong_items_w_3,
f.missing_wrong_items_w_4,
f.total_missing_wrong_items_percentage, --found
f.offline_hours,
f.offline_hours_w_1,
f.offline_hours_w_2,
f.offline_hours_w_3,
f.offline_hours_w_4,

f.prep_time_benchmark,
f.orders_1,
f.declined_orders_1,
f.vendor_late_1,
f.damaged_packaging_1,
f.missing_wrong_items_1,
f.offline_1,
f.orders_2,
f.declined_orders_2,
f.vendor_late_2,
f.damaged_packaging_2,
f.missing_wrong_items_2,
f.offline_2,
benchmark_valid_orders,
benchmark_cancellation_num,
CASE WHEN benchmark_cancellation_num <1 THEN 0::NUMERIC ELSE benchmark_cancellations_gmv END AS benchmark_cancellations_gmv,
benchmark_delay_num,
benchmark_packaging_num,
benchmark_missing_wrong_num,
benchmak_offline_num,
benchmark_overdue_num,
CASE WHEN declined_orders<benchmark_cancellation_num AND vendor_late<benchmark_delay_num AND improve_opportunity_num IN (1, 2) THEN 7 ELSE improve_opportunity_num END AS improve_opportunity_num,
--Case                    WHEN    (f.rdbms_id=19 AND sf.preferred_contact_language='Cantonese') THEN 'Cantonese'
--                        WHEN    (f.rdbms_id=19 AND sf.preferred_contact_language='English') THEN 'English'
--                        WHEN    (f.rdbms_id=18 AND sf.preferred_contact_language='Chinese') THEN 'Chinese'
--                        WHEN    (f.rdbms_id=18 AND sf.preferred_contact_language='English') THEN 'English'
--                        WHEN    (f.rdbms_id=17 AND sf.preferred_contact_language='Thai') THEN 'Thai'
--                        WHEN    (f.rdbms_id=17 AND sf.preferred_contact_language='English') THEN 'English'
--                        --WHEN    (f.rdbms_id=80 or f.rdbms_id=81) THEN 'German' --added   --select preferred_contact_language, * from salesforce.il_dim_accounts (salesforce_accounts)
--                                ELSE 'English' END AS preferred_language
                                
                                
--new entries in the exl 
a.street_name,  
a.post_code,
--resto_name_team,
f.cust_compl, -- Customer complaints include orders with wrong items, missing items and/or damaged packaging
f.cust_compl_w_1,
f.cust_compl_w_2,
f.cust_compl_w_3,
f.cust_compl_w_4,
cancel_order_perc, 
benchmark_missing_wrong_perc,
benchmark_packaging_perc,
benchmark_cancellation_perc
 
 
 
FROM  final f
LEFT JOIN city_bencmark USING (rdbms_id, city_name, week)
LEFT JOIN scoring q USING (rdbms_id, city_name, week, vendor_id)
LEFT JOIN dwh_il.dim_vendors v USING (rdbms_id, vendor_id)
LEFT JOIN dwh_il.dim_deliveryprovider dp ON dp.deliveryprovider_id=v.deliveryprovider_id
--LEFT JOIN salesforce_accounts sf ON sf.rdbms_id=f.rdbms_id AND sf.vendor_code=f.vendor_code   

LEFT JOIN address a USING (rdbms_id, vendor_code)


--where f.week = to_char(current_date-7, 'iyyy-iw') AND valid_orders>0 AND "Email" is not null AND banner_url is not null
where f.week =  to_char(current_date-30, 'iyyy-mm') AND valid_orders>0 AND "Email" is not null AND banner_url is not null --mm
and f.rdbms_id in (80,81)


--DROP TABLE IF EXISTS eligible_vendors;
--CREATE TEMPORARY TABLE eligible_vendors AS ( --SELECT * FROM eligible_vendors
--SELECT activation_date 
--FROM dwh_il.dim_vendors
--where activation_date BETWEEN ( current_date -'5 week'::interval) AND ( current_date -'4 week'::interval) --2017-12-05 - 2017-12-12
--);
);
--);

--\copy crm_campaigns_fo.et_vendor_performance_sendout TO '/home/etl/output/crm_campaigns_fo/exact_target/sftp_home/upload_to_sftp/foodora_vendor_performance_sendout.csv' WITH DELIMITER  ',' NULL AS ' '  CSV QUOTE '"' HEADER   force quote *;


SELECT * FROM dwh_st.zhan_vendor_performance_sendout_AU_DE

; 

 select 
            rdbms_id,
            vendor_id, --,
            --d.report_week as week, --ATTENTION !!!
            -------CLOSES RESTAURANTS
            SUM(closed_hours_num) AS offline_hours,
            sum(closed_hours_num_order_declined) as declined, 
            sum( closed_hours_num_vbe) as vbe, 
            sum(closed_hours_num_internal) as internal,
            SUM(closed_hours_num - closed_hours_num_order_declined) as test,
            SUM(closed_hours_num) - sum(closed_hours_num_order_declined) as test2,
            --closed_hours_num_order_declined, closed_hours_num_vbe, closed_hours_num_internal (we close them)
            SUM(open_hours_num) AS open_hours
        
        from dwh_bl.restaurant_offline_report 
        WHERE closed_hours_num < open_hours_num AND closed_hours_num > 0 and rdbms_id in (80, 81)
        group by 1,2
        
        
        
        
        
