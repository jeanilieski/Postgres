
--CHECK BUFFER AND IF PT IS "ACTIVE"
--SELECT * FROM quant_fo.dpt_bucket_table where country_code='AU' --and vendor_code='s7eq'  
      
----dynamic_prep_time false means that the v is new and no historiacl data is considered by the algorithm

--
--SELECT * --CURRENT DEEP-DIVE
--FROM dwh_bl_fo.vendor_delay_deepdive 
--LEFT JOIN dwh_il.dim_countries USING (rdbms_id)
--WHERE company_name='Foodora' AND rdbms_id NOT IN (80,81) AND week >= to_char (current_date -'3week'::interval, 'iyyy-ww')








--HOW WE CREATE dwh_bl_fo.vendor_delay_deepdive:


DROP TABLE IF EXISTS time_params;
CREATE TEMPORARY TABLE time_params as( 
        SELECT 
        (date_trunc('week', NOW()) - '8 week'::interval)::DATE as start_date,
        (date_trunc('week', NOW()))::DATE -1 as end_date

);


SELECT NOW();
DROP TABLE IF EXISTS dates;
CREATE TEMPORARY TABLE dates as( 
        SELECT  
        DISTINCT
        iso_date,
        iso_full_week_string AS week,
        iso_digit_day_of_week as day
        FROM dwh_il.dim_date
        WHERE iso_date::date BETWEEN (SELECT start_date from time_params) AND (SELECT end_date FROM time_params)

);



SELECT NOW();
DROP TABLE IF EXISTS prep_times;
CREATE TEMPORARY TABLE prep_times as( 
        SELECT DISTINCT 
        cou.rdbms_id,
        b.city_id,      
        b.day,
        b.hour AS order_hour,
        b.day_bucket,
        b.hour_bucket              
        FROM quant_fo.dpt_time_bucket_definitions b             
        LEFT JOIN dwh_il.dim_countries cou ON cou.country_iso = b.country_code

);

--Hirsch & Eber - s4ni
--Nihombashi - s5ig

SELECT NOW();
DROP TABLE IF EXISTS vendors_cities_fo;
CREATE TEMPORARY TABLE vendors_cities_fo as( 

SELECT 
v.rdbms_id,
v.vendor_id, 
v.vendor_code, 
v.vendor_name,
ci.city_name

FROM dwh_il_fo.dim_vendors v
LEFT JOIN dwh_il_fo.dim_city ci USING (rdbms_id, city_id)
WHERE v.vendor_deleted=0

);  --  SELECT * FROM vendors_cities_fo WHERE rdbms_id IN (80) AND vendor_code IN ('s7pm')





SELECT NOW();
DROP TABLE IF EXISTS vendors_cities_fp;
CREATE TEMPORARY TABLE vendors_cities_fp as( 
SELECT 
v.rdbms_id,
v.vendor_id, 
v.vendor_code, 
v.vendor_name,
ci.city_name

FROM dwh_il.dim_vendors v
LEFT JOIN dwh_il.dim_city ci USING (rdbms_id, city_id)
WHERE v.vendor_deleted=0 
);





SELECT NOW();
DROP TABLE IF EXISTS dates_ops_fct_orders;
CREATE TEMPORARY TABLE dates_ops_fct_orders as( 
SELECT
o.food_delivered as order_date, 
o.backend_rdbms_id AS rdbms_id, 
o.order_id, 
o.order_code, 
CASE    WHEN (fo.rdbms_id = 95 and fo.city_id = 6) THEN o.food_picked_up  at time zone 'Australia/Brisbane' at time zone 'Australia/Sydney' 
        WHEN (fo.rdbms_id = 137 and fo.city_id = 5) THEN o.food_picked_up at time zone 'America/Vancouver' at time zone 'America/Toronto' 
        ELSE o.food_picked_up 
        END AS food_picked_up, 
        
COALESCE( fo.gfv_local, fp.gfv_eur*fp.fx)AS gfv_local,
o.courier_late, 
o.vendor_late, 
o.Estimated_prep_duration,
o.estimated_prep_buffer,
COALESCE( fo.vendor_id , fp.vendor_id) AS vendor_id, 
COALESCE( fo.preorder , fp.preorder) AS preorder, 
COALESCE( fot.vendor_confirmation_start ,fpt.vendor_confirmation_start)AS vendor_confirmation_start, 
COALESCE (vfp.vendor_name, vfo.vendor_name) as vendor_name, 
COALESCE (vfp.vendor_code, vfo.vendor_code) as vendor_code, 
COALESCE (fo.city_id, fp.city_id ) as city_id, 
COALESCE (vfp.city_name, vfo.city_name) as city_name


FROM dates d
LEFT JOIN dm_ops_fo.fct_orders o -- o   --select distinct platform from dm_ops_fo.fct_orders
ON o.food_delivered::date=d.iso_date 
LEFT JOIN dwh_il_fo.fct_orders fo -- fo 
ON  o.backend_rdbms_id =fo.rdbms_id AND  o.order_code=fo.order_code_google 
LEFT JOIN dwh_il.fct_orders fp -- fp 
ON  o.backend_rdbms_id =fp.rdbms_id AND  o.order_code=fp.order_code_google 
LEFT JOIN vendors_cities_fp vfp --vfp
ON fp.rdbms_id=vfp.rdbms_id AND fp.vendor_id=vfp.vendor_id
LEFT JOIN vendors_cities_fo vfo --vfo
ON fo.rdbms_id=vfo.rdbms_id AND fo.vendor_id=vfo.vendor_id
LEFT JOIN dwh_il_fo.dim_ops_timestamps fot --fot
ON fo.rdbms_id = fot.rdbms_id AND fo.order_id = fot.order_id
LEFT JOIN dwh_il.dim_ops_timestamps fpt --fpt
ON fp.rdbms_id = fpt.rdbms_id AND fp.order_id = fpt.order_id

WHERE  order_status='completed' and platform IN ('foodpanda','foodora', 'FOODORA')


--select * from dwh_il_fo.fct_orders where rdbms_id=80 and order_code_google like 's5ig%'   --vendor_id=10550
--select min(order_date), max(order_date), max(expected_delivery_time) from dwh_il_fo.fct_orders where rdbms_id=80 and order_code_google like 's5ig%' --2017-12-23
--select * from dwh_il_fo.fct_orders --where order_code_google::date BETWEEN (SELECT start_date from time_params) AND (SELECT end_date FROM time_params)
--select *  from dwh_il_fo.dim_vendors where vendor_name='Habba Habba'




);  -- SELECT * FROM dates_ops_fct_orders 





SELECT NOW();


DROP TABLE IF EXISTS ops_info;
CREATE TEMPORARY TABLE ops_ifo as( 
        SELECT DISTINCT
        o.order_date,
        o.rdbms_id,
        o.vendor_id, 
        o.order_code, 
        o.order_id,
        (EXTRACT(HOUR FROM o.order_date)) + 0.5 * ROUND(EXTRACT(MINUTE FROM o.order_date)/30) as order_hour,
        o.gfv_local,
        o.food_picked_up,
        o.vendor_confirmation_start,
        o.courier_late ,
        o.estimated_prep_duration AS prep_time,
        o.estimated_prep_buffer,
        o.vendor_name,
        o.vendor_code,
        o.city_id,
        o.city_name,
            CASE WHEN /*redelivery is true or */o.courier_late > 10 or o.preorder=1 THEN o.Estimated_prep_duration ELSE 
                      ROUND(EXTRACT ( EPOCH FROM (o.food_picked_up- o.vendor_confirmation_start)/60)::NUMERIC,2) 
            END  AS cooking_time,
        o.vendor_late 
        
        
        FROM dates_ops_fct_orders o
         
 );  
 
 -- it is a kind of sq I should use for the delays table below. it is still open for adjustments. it should match the delayed orders and the total orders in th exl file summary.
 


 

SELECT NOW();
DROP TABLE IF EXISTS delays;
CREATE TEMPORARY TABLE delays as(                

        SELECT 
        *,
        ---RIDER DELAYS---
        courier_late AS rider_delay_time,
            CASE WHEN courier_late >= 5 THEN 1 ELSE 0 
        END AS rider_late_5,                
      
        ---VENDOR DELAYS---
        vendor_late AS vendor_delay_time_2,
            CASE WHEN vendor_late >= 5 THEN 1 ELSE 0
        END AS vendor_delay_5_no_rider
        
        FROM ops_ifo
    
    

        
);






SELECT NOW();


DROP TABLE IF EXISTS dwh_st.zhan_vendor_delay_deepdive;
CREATE TABLE dwh_st.zhan_vendor_delay_deepdive AS(
--TRUNCATE TABLE dwh_bl_fo.vendor_delay_deepdive;
--INSERT INTO dwh_bl_fo.vendor_delay_deepdive

SELECT 

    d.iso_date, 
    d.week,
    d.day,
    o.rdbms_id, 
    co.common_name AS Country, 
    o.city_name,
    o.vendor_name, 
    o.vendor_code,
    p.hour_bucket,
    trunc(o.order_hour) AS order_hour,
    order_code,
    gfv_local,
    CASE WHEN rider_delay_time < 0 THEN 0 ELSE rider_delay_time END AS net_rider_delay_time, 
    CASE WHEN vendor_delay_time_2 < 0 THEN 0 ELSE vendor_delay_time_2 END AS net_vendor_delay_time, 
    CASE WHEN vendor_delay_5_no_rider =1 AND rider_late_5 = 1 THEN 1 ELSE 0 END AS delay_mix, --both
    CASE WHEN vendor_delay_5_no_rider =0 AND rider_late_5 = 1 THEN 1 ELSE 0 END AS delay_rider, --rider delay
    CASE WHEN vendor_delay_5_no_rider =1 AND rider_late_5 = 0 THEN 1 ELSE 0 END AS delay_vendor, -- partner delay (not "vendor_mix"-misleading name)
    o.prep_time,
    o.estimated_prep_buffer AS prep_buffer,
    o.cooking_time AS cooking_time,
    1 AS valid_orders,
    CASE WHEN vendor_delay_time_2>0 then 1 else 0 end as delay_flag
    
 

    FROM dates d --dates
    LEFT JOIN delays o --delays
    ON d.iso_date=o.order_date::date
    LEFT JOIN dwh_il.dim_countries co --countries
    ON o.rdbms_id=co.rdbms_id
    LEFT JOIN prep_times p --preptimes
    ON o.rdbms_id=p.rdbms_id AND d.day=p.day AND p.order_hour=o.order_hour 
    
    WHERE CASE WHEN co.company_name='Foodpanda' THEN  (date_trunc('week',d.iso_date)) >= (date_trunc('week', NOW()) - '4 week'::interval) 
    ELSE (date_trunc('week',d.iso_date)) >= (date_trunc('week', NOW()) - '8 week'::interval)end 

)
;

--SELECT * FROM dwh_st.zhan_vendor_delay_deepdive where rdbms_id =19 and vendor_code in ('s7kx')


--SELECT * FROM crm.et_sendJobs 
--WHERE (subject = 'Hyvä ruoka tekee onnelliseksi - ja meillä on tarjota sinulle erinomainen diili.'
--
--OR subject = 'Her kommer et smakfullt tilbud som vil gjøre deg god og mett!')
--
--;
--
--
--SELECT * FROM crm.et_sendJobs 
--WHERE emailname like 'NO_Partner_Promotion%';



--DROP TABLE IF EXISTS eligible_vendors;
--CREATE TEMPORARY TABLE eligible_vendors AS (
----select vendor_name, address_line1, address_line2, postcode 
----from dwh_il_fo.dim_Vendors
--select rdbms_id, country_name, count(*)
--from (
--select z."Street Name" as street_name,  z."Post Code" as post_code, z."Activated Date" as activation_date, v.rdbms_id, z."Account Name", 
--a.country_name    --,  v.vendor_name --, "Account Name", "Account ID", "18 Char Account ID" , * --"Shipping Zip/Postal Code", a.preferred_contact_language, 
--
--from salesforce_fo.foodora_all_accounts z   -- select * from salesforce_fo.foodora_all_accounts limit 5
--
--left join salesforce_fo.il_dim_accounts a  -- select * from salesforce_fo.il_dim_accounts (has not preffered language)
--on a.account_id=z."18 Char Account ID"  
--
--left join dwh_il_fo.dim_vendors v
--ON v.vendor_code=a.vendor_code  and v.rdbms_id=a.rdbms_id 
--
--LEFT JOIN dwh_il.dim_countries co
--on a.country_name=co.common_name                                 
--
--where z."Account Status" in ('Active') 
--AND z."Activated Date" BETWEEN ( '2018-01-08'::date) AND ( '2018-01-14'::date) --2017-12-17 and 2017-12-24   
--AND v.rdbms_id in (80,81,88,86,87,84,85,83) 
--order by 3 desc 
----limit 7
----select current_date -'4 week'::interval - '1 day'::interval--2017-12-17 and 2017-12-23
--)sq
--group by 1 ,2

--BETWEEN 25 AND 24:  349
--BETWEEN 6 AND 5 W: 440
--BETWEEN 5 AND 4:  389  --2017-12-17 and 2017-12-24
--BETWEEN 4 AND 3:  137  --2017-12-24 and 2017-12-31
--BETWEEN 3 AND 2:  195  --2018-01-01 and 2018-01-07

; --SELECT * FROM eligible_vendors

SELECT * --CURRENT DEEP-DIVE
FROM dwh_bl_fo.vendor_delay_deepdive 
LEFT JOIN dwh_il.dim_countries USING (rdbms_id)
WHERE  rdbms_id NOT IN (80,81) AND week >= to_char (current_date -'3week'::interval, 'iyyy-ww')
AND  company_name='Foodora' AND 'foodpanda','foodora', 'FOODORA'
