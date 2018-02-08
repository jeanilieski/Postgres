--dwh_st.zhan_test

SELECT now() as time_params; --defining the period of analysis
DROP TABLE IF EXISTS time_params ;
CREATE TEMPORARY TABLE time_params  AS 

SELECT

(date_trunc('month', NOW())  - '1 month'::interval)::DATE as start_month,
(date_trunc('month', NOW())  - '1 day'::interval)::DATE as end_month

;


--SELECT DATE_TRUNC('hour', TIMESTAMP '2017-03-17 02:09:30');

--SELECT
--now (), date_trunc('month', NOW()), '2 month'::interval, '1 hour'::interval,
--(date_trunc('month', NOW())  - '1 month'::interval)::DATE as start_month, --trunk
--(date_trunc('month', NOW())  - '1 day'::interval)::DATE as end_month




SELECT now() as dates; 
DROP TABLE IF EXISTS dates ;
CREATE TEMPORARY TABLE dates  AS 

SELECT DISTINCT
iso_date, -- iso
iso_full_week_string,
us_full_month_string, --us time starts with sunday as 1
iso_digit_day_of_week
FROM dwh_il.dim_date --date table
WHERE iso_date BETWEEN (SELECT start_month::date from time_params) AND (SELECT end_month::date from time_params) ORDER BY 1
;

--check:
--SELECT * from dwh_il.dim_date

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------DATA SET--------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT now() as data_set;
DROP TABLE IF EXISTS data_set ;
CREATE TEMPORARY TABLE data_set  AS  --data set is related to vendor information

SELECT DISTINCT v.rdbms_id,
v.vendor_id, v.vendor_code,
c.city_id, to_char((SELECT start_month::date from time_params), 'iyyy-mm') as report_month,
co.common_name as country_name,
c.city_name as city_name,
v.vendor_name, v.accept_pickup

FROM  dwh_il_fo.dim_vendors v --vendor
LEFT JOIN salesforce_fo.il_dim_accounts a --account
USING(rdbms_id, vendor_code)
LEFT JOIN dwh_il_fo.dim_city c --city
ON v.rdbms_id=c.rdbms_id and v.city_id=c.city_id
LEFT JOIN dwh_il.dim_countries co --country
ON v.rdbms_id = co.rdbms_id
WHERE co.live=1 and c.active=1 and co.company_name='Foodora' and a.account_status='Active' and a.account_type ='Partner Account' 
GROUP BY 1,2,3,4,5,6,7,8,9 --it is possible to group without agg col on the top (why we do it?) 
;

--checks:
--select * from data_set
--select * from salesforce_fo.il_dim_accounts
--select * from dwh_il.dim_countries


---------------------------------------------------ORDERS
SELECT now() as valid_orders;
DROP TABLE IF EXISTS valid_orders ;
CREATE TEMPORARY TABLE valid_orders  AS 

SELECT o.* 
FROM dwh_il_fo.fct_orders o --orders 
LEFT JOIN dwh_il.meta_order_status os --order status
ON o.rdbms_id=os.rdbms_id AND o.status_id=os.status_id 
WHERE os.valid_order=1 and o.order_id>0 -- only valid orders
;


SELECT now() as ops_order_info; ---------------------------OPERATIONS
DROP TABLE IF EXISTS ops_order_info ;
CREATE TEMPORARY TABLE ops_order_info  AS
 
SELECT    -- order per co, c, c_name, v_code, month, 
o.rdbms_id,
o.city_id,
o.vendor_id,
v.vendor_code,
us_full_month_string as report_month,
c.city_name,
COUNT(DISTINCT o.order_id) FILTER (WHERE s.valid_order=1) as valid_orders, -- count valid orders
COUNT(DISTINCT o.order_id) FILTER (WHERE s.gross_order=1) as gross_orders, --gross order to calc failed order %
COUNT(DISTINCT o.order_id) FILTER (WHERE s.failed_order_vendor=1) as failed_orders_vendor, --count failed /due to vendor?
COUNT(DISTINCT o.order_id) FILTER(WHERE valid_order=1 and o.expedition_type = 'pickup') as pickup_orders, -- count pick up orders
COUNT(DISTINCT o.order_id) FILTER(WHERE valid_order=1 and o.expedition_type != 'pickup') as delivery_orders, --count delivery orders

SUM(o.gmv_eur) FILTER (WHERE s.valid_order=1) AS gmv_eur, -- the order price the cust pays
SUM(o.gfv_eur) FILTER (WHERE s.valid_order=1) AS gfv_eur, -- the order price minus delivery fee

SUM(EXTRACT (EPOCH FROM ot.vendor_confirmation_end::TIMESTAMP-ot.vendor_confirmation_start::TIMESTAMP)::DECIMAL(20,2)/60) 
AS processing_time, -- in min per order
-- EXTRACT EPOCH: the total number of seconds in an interval

COUNT(o.order_id) FILTER (WHERE ot.vendor_confirmation_start IS NOT NULL AND ot.vendor_confirmation_end IS NOT NULL) 
AS processing_time_count, 

COUNT(o.order_id) FILTER (WHERE ot.code_array && ARRAY[51,52,53,55,56,57,59,591,592]) AS actionable_orders,
--51 - open, call vendor
--52 - order declined, contact customer
--53 - confirmation overdue, call vendor
--54 - order cancelled, call vendor
--55 - error, call vendor or customer
--56 - Potential duplicate order, call customer
--57 - Verify Order/Customer Information
--59 - Traffic manager declined, call customer
--591 - Traffic manager declined, call customer and vendor
--592 - Traffic manager cancelled, call customer and vendor


--valid actionable / total number of valid orders = automated orders (0 to 1)
COUNT(o.order_id) FILTER (WHERE ot.code_array && ARRAY[51,52,53,55,56,57,59,591,592] and s.valid_order=1)::double precision/
NULLIF(COUNT(o.order_id) FILTER (WHERE s.valid_order=1),0)::double precision AS automated_orders,  
-- if the total number of valid orders is not=0 return the number, otherway return "NULL"

COUNT(o.order_id) FILTER (WHERE bl.vendor_late >= '00:05:00' and bl.courier_late < '00:05:00')as vendor_delay_sum  
--how many times the vendor was late

FROM dates d
LEFT JOIN dwh_il_fo.fct_orders o --factual orders 
ON d.iso_date=o.order_date::date                          -- turn timestamp into date 
LEFT JOIN dwh_il.dim_countries co --countries 
ON o.rdbms_id=co.rdbms_id 
LEFT JOIN dwh_il_fo.dim_vendors v --vendors
ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id 
LEFT JOIN dwh_il_fo.dim_city c --city
ON o.rdbms_id=c.rdbms_id AND o.city_id=c.city_id 
LEFT JOIN dwh_il_fo.meta_order_status s --order status s
ON o.rdbms_id=s.rdbms_id AND s.status_id=o.status_id 
LEFT JOIN dwh_il_fo.dim_ops_timestamps ot --timestamps ot --online pay, cust verif, cc handle, v conf, dispatch, o complet
ON o.rdbms_id=ot.rdbms_id AND o.order_id=ot.order_id    
-- LEFT JOIN dwh_il_fo.fct_zendesk z ON o.rdbms_id=z.rdbms_id AND o.order_id=z.order_id  
   

LEFT JOIN (                                                             --from delivery timing: vendor_late, courier_late
            SELECT DISTINCT ops_o.backend_rdbms_id as rdbms_id, ops_o.order_id, t.vendor_late, t.courier_late 
      
            FROM dm_ops_fo.fct_orders ops_o --orders
            LEFT JOIN dm_ops_fo.fct_deliveries d --deliveries
            USING (disp_order_id, rdbms_id)
            LEFT JOIN dm_ops_fo.hu_fct_delivery_timings t --delivery timings
            ON d.rdbms_id=t.rdbms_id AND d.delivery_id = t.delivery_id
            WHERE ops_o.created_at BETWEEN (SELECT start_month from time_params) AND (SELECT end_month from time_params)
       )  bl --bl
ON o.rdbms_id=bl.rdbms_id AND o.order_id=bl.order_id

WHERE  co.live=1 and not c.is_test
AND iso_date BETWEEN (SELECT start_month from time_params) AND (SELECT end_month from time_params)
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6 DESC
; 




-------------------------------------------------------------  CONVERSION RATES    
SELECT now() as conversion_rate;
DROP TABLE IF EXISTS conversion_rate ;
CREATE TEMPORARY TABLE conversion_rate  AS                
 SELECT * FROM
            (   
            SELECT 
            v.rdbms_id, to_char(report_date::date,'iyyy-mm') as report_month, v.vendor_id, 
            SUM(cr3_end) / SUM(cr3_start)  as cr3 -- cr1 restaurant choice, cr2 interaction with the menu, cr3 click on check-out, --start, end (0,1)

            FROM dwh_bl_fo.ga_vendor_conversion_report v 
            LEFT JOIN dwh_il_fo.dim_vendors ve 
            ON v.rdbms_id=ve.rdbms_id AND v.vendor_code=ve.vendor_code
            WHERE v.vendor_code is not null and report_date BETWEEN (SELECT start_month from time_params) AND (SELECT end_month from time_params)
            GROUP BY 1,2,3
            
            ) g
    
    WHERE cr3 is not null
;

--select * FROM dwh_bl_fo.ga_vendor_conversion_report order by 3 desc  



SELECT now() as product;---------------------------------------------------------------------PRODUCT
DROP TABLE IF EXISTS product ;
CREATE TEMPORARY TABLE product  AS  
 
SELECT 
vc.rdbms_id,ci.city_id, vc.vendor_id, vc.menucategories, vc.products, vc.p_descriptions
FROM  dwh_il_fo.dim_vendors v --vendros
LEFT JOIN dwh_il_fo.dim_city ci --city
ON ci.rdbms_id=v.rdbms_id AND ci.city_id=v.city_id
LEFT JOIN dwh_il_fo.dim_vendor_content vc --vendor content 
ON v.rdbms_id=vc.rdbms_id AND v.vendor_id=vc.vendor_id

;




SELECT now() as first_order;---------------------------------------------------VENDOR LEVEL REORDER RATES
DROP TABLE IF EXISTS first_order ;


CREATE TEMPORARY TABLE first_order  AS -----------------------------------------------------------------------------------first_order 

SELECT o.rdbms_id, o.vendor_id ,o.customer_id, MIN(o.order_date::date) as first_order_date
FROM valid_orders o  
GROUP BY 1,2,3;

SELECT now() as first_order_report_month;
DROP TABLE IF EXISTS first_order_report_month ;
CREATE TEMPORARY TABLE first_order_report_month  AS  ------------------------------------------------------first_order_report_month  

        SELECT o.rdbms_id, o.vendor_id, o.customer_id, MIN(o.order_date::date) AS first_order_date
        FROM dwh_il_fo.fct_orders o --orders
        LEFT JOIN dwh_il_fo.meta_order_status os --order status
        ON o.rdbms_id = os.rdbms_id AND o.status_id = os.status_id
        WHERE  os.valid_order=1 AND to_char(o.order_date,'yyyy-mm')=to_char((SELECT start_month::date from time_params), 'iyyy-mm')
        GROUP BY 1,2,3
        
        

      
;
SELECT now() as all_customers_reorder_rates_vendor; -----------------------------------------all_customers_reorder_rates_vendor
DROP TABLE IF EXISTS all_customers_reorder_rates_vendor ;
CREATE TEMPORARY TABLE all_customers_reorder_rates_vendor  AS  

SELECT
f.rdbms_id, f.vendor_id,
COUNT(DISTINCT(CASE WHEN date_part('day', CURRENT_DATE - f.first_order_date::timestamp)>28 THEN f.customer_id ELSE NULL END)) 
AS base_cust, --count distinct customers in the last 4 weeks 

COUNT(DISTINCT(CASE WHEN date_part('day', CURRENT_DATE - f.first_order_date::timestamp)>28
AND date_part('day', o.order_date::timestamp - first_order_date::timestamp)>0 
AND date_part('day', o.order_date::timestamp - first_order_date::timestamp)<=28 AND o.vendor_id=f.vendor_id THEN f.customer_id END)) 
AS cust_4w --four weeks

FROM first_order_report_month f --first order
LEFT JOIN dwh_il_fo.fct_orders o --orders
ON f.rdbms_id=o.rdbms_id AND f.customer_id=o.customer_id
LEFT JOIN dwh_il_fo.meta_order_status os --status
ON o.rdbms_id = os.rdbms_id AND o.status_id = os.status_id
WHERE  os.valid_order=1
GROUP BY 1,2


;

--check
select * from dwh_il_fo.fct_orders 

select current_date, order_date, CURRENT_DATE - order_date, date_part('day', CURRENT_DATE - order_date) as days, 
date_part('day', CURRENT_DATE - order_date::timestamp) as timesta  
FROM dwh_il_fo.fct_orders 

select  rdbms_id, order_id, order_date, count(date_part('day', CURRENT_DATE - order_date::timestamp)) 
FROM dwh_il_fo.fct_orders 
group by 1,2,3



-----------------------

SELECT now() as rr_wow;
DROP TABLE IF EXISTS rr_wow ;
CREATE TEMPORARY TABLE rr_wow  AS  


SELECT 
rdbms_id,
vendor_id,
to_char((SELECT start_month::date from time_params), 'iyyy-mm') as report_month,
base_cust as base_cust_4,
cust_4w as cust_1month_4
FROM all_customers_reorder_rates_vendor
;

-----------------------   ACTIVATIONS

SELECT now() as zendesk;
DROP TABLE IF EXISTS zendesk ;
CREATE TEMPORARY TABLE zendesk  AS 

        SELECT
            TO_CHAR(o.order_date::date, 'iyyy-mm') as report_month,
            o.rdbms_id,
            v.vendor_code, ---- we have to chage to 3E
            COUNT(DISTINCT o.order_id ) FILTER (WHERE customer_contact_reason_updated ~* '3D.' or customer_contact_reason_updated ~* '3C.' or customer_contact_reason_updated ~* '3B.') as Customer_Complaints
        FROM dwh_il.dim_date d
        LEFT JOIN(
                SELECT o.order_date, o.rdbms_id, o.vendor_id, o.order_id, o.order_code_google, o.status_id, a.products_plus_vat as gfv_local 
                FROM dwh_il_fo.fct_orders o
                LEFT JOIN dwh_il_fo.fct_accounting a 
                ON a.rdbms_id=o.rdbms_id AND o.order_id=a.order_id 
                ) o --orders
                ON d.iso_date=o.order_date::date
        
        LEFT JOIN dwh_il_fo.fct_zendesk z --zendesk
        ON z.rdbms_id=o.rdbms_id AND o.order_code_google=z.order_code
        LEFT JOIN dwh_il_fo.meta_order_status s --order status
        ON o.rdbms_id=s.rdbms_id AND s.status_id=o.status_id 
        LEFT JOIN  dwh_il_fo.dim_vendors v --vendors
        ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id

        WHERE o.order_date BETWEEN (SELECT start_month from time_params) AND (SELECT end_month from time_params) and  s.valid_order = 1 
        GROUP BY 1,2,3 
;
--check:
SELECT * FROM dwh_il_fo.fct_orders
SELECT * FROM dwh_il.dim_date
SELECT * FROM dwh_il_fo.fct_accounting
SELECT * FROM  dwh_il_fo.fct_zendesk --tickets

--3A. Food Quality, Delivery time
-- 3B. Missing Item
-- 3C. Wrong item

-- customer_contact_reason_updated IN ('3. Post-Delivery :: 3D. Food Quality :: Food quality main :: Edible main')
-- customer_contact_reason_updated IN ('3. Post-Delivery :: 3E. Rider :: Damaged packaging')


SELECT now() as nps; --net promoter score, for now not considered
DROP TABLE IF EXISTS nps ;
CREATE TEMPORARY TABLE nps  AS  

                                SELECT
                                v.rdbms_id, 
                                v.city_id,
                                v.vendor_id,
                                to_char(date_submitted, 'iyyy-mm') as report_month, 
                                COALESCE(ROUND(AVG (recommendation::int), 2),0) AS nps_packaging
                                FROM survey_gizmo_fo.survey_gizmo_nps n --
                                LEFT JOIN dwh_il.dim_countries co --country
                                ON upper(split_part(n.venture_name,'_',2))=co.country_iso
                                LEFT JOIN dwh_il_fo.fct_orders o --order
                                ON o.rdbms_id = co.rdbms_id AND o.order_id = n.last_order_id
                                LEFT JOIN dwh_il_fo.dim_vendors v --vendor
                                ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id
                                LEFT JOIN dwh_il_fo.dim_city c --city
                                ON o.rdbms_id=c.rdbms_id AND o.city_id=c.city_id
                                
                                WHERE date_submitted::date BETWEEN (SELECT start_month from time_params) AND (SELECT end_month from time_params)
                                AND  main_reason_for_recommendation_score IN ('Packaging')
                                
                                AND NOT c.is_test AND c.active=1 
                                GROUP BY 1,2,3,4

;



--CHECK:
SELECT * FROM survey_gizmo_fo.survey_gizmo_nps



SELECT now() as closing_hours; -----------------------------------------------------closing_hours
DROP TABLE IF EXISTS closing_hours ;
CREATE TEMPORARY TABLE closing_hours  AS 
SELECT rdbms_id, vendor_id, to_char(report_date, 'iyyy-mm') as report_month , SUM(COALESCE(closed_hours_num,0))::numeric/ SUM(open_hours_num) as closing_hours
FROM dwh_bl.restaurant_offline_report 
GROUP BY 1,2,3
;

--check:
SELECT * FROM  dwh_bl.restaurant_offline_report 




SELECT now() as final; ----------------------------------------------------------final
DROP TABLE IF EXISTS final ;
CREATE TEMPORARY TABLE final  AS 
SELECT *,
CASE WHEN ((ntile(100) OVER(PARTITION BY d.rdbms_id, d.city_id, d.report_month ORDER BY gmv_eur DESC))) <= 10 THEN 1 else 0 end as city_rank,  --ntile
CASE WHEN ((ntile(100) OVER(PARTITION BY d.rdbms_id, d.report_month ORDER BY gmv_eur DESC)))<=35 THEN 1 else 0 end as country_rank
FROM(

    SELECT  DISTINCT

    d.rdbms_id,
    d.city_id,
    d.vendor_id, 
    d.vendor_code,
    d.report_month,
    d.country_name,
    d.city_name,
    d.vendor_name,
    accept_pickup,
    cr3,
    COALESCE(gmv_eur,0) as gmv_eur,
    valid_orders,
    pickup_orders,
    delivery_orders,
    gross_orders,
    failed_orders_vendor,
    (failed_orders_vendor)::double precision /NULLIF(gross_orders,0)::double precision as cancellations, --cancellations
    gmv_eur as gmv, --gmv_eur=gfv_eur+delivery_fee_eur
    gfv_eur as gfv,
    COALESCE(((gfv_eur::double precision / NULLIF(valid_orders,0)::double precision) * commission_percentage::double precision),0) + 
    (COALESCE((valid_orders * flat_fee),0)::double precision / NULLIF(valid_orders,0)::double precision) as revenue, --revenue: gfv_eur/valid_orders * commission_percentage + valid_orders * flat_fee/valid_orders

    --what is flat_fee from salesforce_fo.opportunity_marketing_score_bkp (varies 0.0 and 35.0 on the first page)?


    --select distinct flat_fee from salesforce_fo.opportunity_marketing_score_bkp 
    --select * from salesforce_fo.opportunity_marketing_score_bkp 
    --select * FROM dwh_il_fo.fct_orders

    (gmv_eur::double precision) /NULLIF(valid_orders::double precision,0) as aov_eur,
    gfv_eur::double precision /NULLIF(valid_orders::double precision,0) as afv, --afv
    1- ((actionable_orders)::double precision /NULLIF(valid_orders,0)::double precision)::double precision as automation,
    processing_time::double precision/NULLIF(processing_time_count, 0)::double precision as processing_time,



    actionable_orders,
    vendor_delay_sum::double precision/NULLIF(valid_orders,0)::double precision as vendor_delay,
    automated_orders::double precision as automated_orders,
    1-automated_orders::double precision as automated_orders_perc,
    ---------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------

    COALESCE(cust_1month_4::double precision/NULLIF(base_cust_4,0)::double precision ,0) as reorder_rate,
    COALESCE(base_cust_4, 0)  as base_cust_4,

    COALESCE(cust_1month_4,0) as cust_1month_4,
    ---------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------
    COALESCE(pp.menucategories,0) as menucategories,
    COALESCE(pp.products,0) as products,
    COALESCE(pp.p_descriptions,0) as p_descriptions,


    COALESCE((pp.products-pp.p_descriptions)::double precision/NULLIF(pp.products,0)::double precision, 0) as products_without_description ,
    ---------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------
    nps_packaging,
    COALESCE(packaging_quality, 0) as packaging_quality,
    ---------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------
    m.am_name::text,

    COALESCE(commission_percentage, 0) as commission_percentage,
    COALESCE(price_mark_up, 0) as price_mark_up,


    COALESCE(fb_advert_rights, 0 )as fb_advert_rights,
    COALESCE(m.facebook_likes, 0 )as facebook_likes,
    COALESCE(m.door_sticker, 0 )as door_sticker,
    COALESCE(m.voucher_cards, 0 )as voucher_cards,
    COALESCE(m.display, 0 )as display,
    COALESCE(m.backlink, 0 ) as backlink,
    COALESCE(CASE WHEN m.exclusivity = 0 THEN 0 WHEN m.exclusivity = 1 then 1 end, 0) as exclusivity, 

     CASE 

     WHEN facebook_likes < 1000 THEN 10
     WHEN facebook_likes >= 1000 AND facebook_likes < 2000  THEN 9
     WHEN facebook_likes >= 2000 AND facebook_likes < 3000  THEN 8
     WHEN facebook_likes >= 3000 AND facebook_likes < 4000  THEN 7
     WHEN facebook_likes >= 4000 AND facebook_likes < 5000  THEN 6
     WHEN facebook_likes >= 5000 AND facebook_likes < 6000  THEN 5
     WHEN facebook_likes >= 6000 AND facebook_likes < 7000  THEN 4
     WHEN facebook_likes >= 7000 AND facebook_likes < 8000  THEN 3
     WHEN facebook_likes >= 8000 AND facebook_likes < 9000  THEN 2
     WHEN facebook_likes >= 9000 AND facebook_likes < 10000 THEN 1
     WHEN facebook_likes >= 10000 THEN 0 
     END AS "facebook_likes [score]",
     no_fb, 
     customer_complaints,
     stage,
     gmv_class,
     closing_hours
     

    FROM data_set d --d

    LEFT JOIN ops_order_info f USING (rdbms_id, vendor_id, report_month) --f

    LEFT JOIN conversion_rate c USING (rdbms_id, vendor_id, report_month) --c
    LEFT JOIN rr_wow rr USING (rdbms_id, vendor_id, report_month) --rr
    LEFT JOIN closing_hours  ch USING (rdbms_id, vendor_id, report_month) --ch
    LEFT JOIN product pp USING (rdbms_id, vendor_id) --pp
    LEFT JOIN nps nps USING(rdbms_id, vendor_id, report_month) --nps
    LEFT JOIN salesforce_fo.opportunity_marketing_score_bkp m  --m
    ON m.rdbms_id = d.rdbms_id AND m.vendor_code = d.vendor_code AND m.report_month=d.report_month
    LEFT JOIN  zendesk z --z
    ON z.rdbms_id = d.rdbms_id AND z.vendor_code = d.vendor_code AND z.report_month=d.report_month

    LEFT JOIN  dwh_bl.vendor_gmv_class cl  --cl
    ON cl.rdbms_id = d.rdbms_id AND cl.vendor_code = d.vendor_code  and  cl.company IN ('Foodora')  

    WHERE d.report_month BETWEEN (SELECT to_char(start_month, 'iyyy-mm') from time_params) AND (SELECT to_char(end_month, 'iyyy-mm') from time_params) 
    )d 
    ;



-----------------------------------------------------------------------SCORING:
SELECT now() as scoring;
DROP TABLE IF EXISTS scoring ;
CREATE TEMPORARY TABLE scoring  AS --scoring table



SELECT

f.rdbms_id,
city_id,
vendor_id, 
f.vendor_code,
f.report_month,
country_name,
city_name,
vendor_name,
am_name::text,
--------------------------------------------------------------------- commission
valid_orders, 
pickup_orders,
delivery_orders,
commission_percentage,
gmv, 
gfv,
afv,
accept_pickup,
COALESCE( CASE 
 WHEN commission_percentage < 0.22 THEN 0
 WHEN commission_percentage >= 0.22 AND commission_percentage < 0.23 THEN 1
 WHEN commission_percentage >= 0.22 AND commission_percentage < 0.23 THEN 2
 WHEN commission_percentage >= 0.23 AND commission_percentage < 0.24 THEN 3
 WHEN commission_percentage >= 0.24 AND commission_percentage < 0.25 THEN 4
 WHEN commission_percentage >= 0.25 AND commission_percentage < 0.26 THEN 5
 WHEN commission_percentage >= 0.26 AND commission_percentage < 0.27 THEN 6
 WHEN commission_percentage >= 0.27 AND commission_percentage < 0.28 THEN 7
 WHEN commission_percentage >= 0.28 AND commission_percentage < 0.29 THEN 8
 WHEN commission_percentage >= 0.29 AND commission_percentage < 0.30 THEN 9
 WHEN commission_percentage >= 0.30 THEN 10 
 END, 0) AS "commission [score]",
 
--------------------------------------------------------------------- CR3
 cr3, 
 
 COALESCE(CASE 
 WHEN cr3 < 0.075 THEN 0
 WHEN cr3 >= 0.075 AND cr3 < 0.100  THEN 1
 WHEN cr3 >= 0.100 AND cr3 < 0.125  THEN 2
 WHEN cr3 >= 0.125 AND cr3 < 0.150  THEN 3
 WHEN cr3 >= 0.150 AND cr3 < 0.175  THEN 4
 WHEN cr3 >= 0.175 AND cr3 < 0.200  THEN 5
 WHEN cr3 >= 0.200 AND cr3 < 0.225  THEN 6
 WHEN cr3 >= 0.225 AND cr3 < 0.250  THEN 7
 WHEN cr3 >= 0.250 AND cr3 < 0.275  THEN 8
 WHEN cr3 >= 0.275 AND cr3 < 0.30  THEN 9
 WHEN cr3 >= 0.30 THEN 10 
 END,0) AS "cr3 [score]",
--------------------------------------------------------------------- price_mark_up
 price_mark_up,
 COALESCE(CASE 
 WHEN price_mark_up = 0 THEN 10 
 WHEN price_mark_up >=1 THEN 0 END,0) as "price_mark_up [score]",
 
 --------------------------------------------------------------------- binary
 
door_sticker,
 CASE 
 WHEN door_sticker = 0 THEN 0 
 WHEN door_sticker = 1 THEN 10 END as "door_sticker [score]",
 
voucher_cards,
 CASE 
 WHEN voucher_cards = 0 THEN 0 
 WHEN voucher_cards = 1 THEN 10 END as "voucher_cards [score]",
 
display,
 CASE 
 WHEN display = 0 THEN 0 
 WHEN display = 1 THEN 10 END as "display [score]",
 
backlink,
 CASE 
 WHEN backlink = 0 THEN 0 
 WHEN backlink = 1 THEN 10 END as "backlink [score]",
 
CASE WHEN exclusivity = 0 THEN 0 WHEN exclusivity = 1 then 1 end as exclusivity, 
 CASE 
 WHEN exclusivity = 0 THEN 0 
 WHEN exclusivity = 1 THEN 10 END as "exclusivity [score]",
 
  --------------------------------------------------------------------- binary
nps_packaging,
nps_packaging as "nps_packaging [score]",
  
packaging_quality,
facebook_likes,

COALESCE( CASE 
 WHEN packaging_quality = 4 THEN 0 
 WHEN packaging_quality = 3 THEN 3 
 WHEN packaging_quality = 0 THEN 5 
 WHEN packaging_quality = 2 THEN 8 
 WHEN packaging_quality = 1 THEN 10 
 END ,0)as "packaging_quality [score]",
 
 
 --------COALESCE(------------------------------------------------------------- FACEBOOK
 "facebook_likes [score]",
 fb_advert_rights,
 
 COALESCE(CASE 
 WHEN no_fb = 1 THEN 10
 WHEN city_rank =1 and fb_advert_rights = 0 THEN 0
 WHEN fb_advert_rights = 1  THEN 10
 ELSE  "facebook_likes [score]"  END ,0)AS "facebook [score]",
  
 --------------------------------------------------------------------- REORDER RATE
 
 reorder_rate,

 CASE 
 WHEN reorder_rate < 0.01 THEN 0
 WHEN reorder_rate >= 0.01 AND reorder_rate < 0.02  THEN 1
 WHEN reorder_rate >= 0.02 AND reorder_rate < 0.03  THEN 2
 WHEN reorder_rate >= 0.03 AND reorder_rate < 0.04  THEN 3
 WHEN reorder_rate >= 0.04 AND reorder_rate < 0.05  THEN 4
 WHEN reorder_rate >= 0.05 AND reorder_rate < 0.06  THEN 5
 WHEN reorder_rate >= 0.06 AND reorder_rate < 0.07  THEN 6
 WHEN reorder_rate >= 0.07 AND reorder_rate < 0.08  THEN 7
 WHEN reorder_rate >= 0.08 AND reorder_rate < 0.09  THEN 8
 WHEN reorder_rate >= 0.09 AND reorder_rate < 0.1   THEN 9
 WHEN reorder_rate >= 0.1 THEN 10 
 END AS "rr [score]",
  --------------------------------------------------------------------- REORDER RATE
 
menucategories,
COALESCE( CASE 
        WHEN menucategories  <= 8 THEN 10
        WHEN menucategories  = 9 THEN 9
        WHEN menucategories  = 10 THEN 8
        WHEN menucategories  = 11 THEN 4
        WHEN menucategories  = 12 THEN 3
        WHEN menucategories  = 13 THEN 2
        WHEN menucategories  = 14 THEN 1
        WHEN menucategories  >= 15 THEN 0
 END ,0) AS "menucategories [score]", 

  --------------------------------------------------------------------- MENUS

products,

COALESCE( CASE 
 WHEN products > 85 THEN 0
 WHEN products > 80 AND products <= 85  THEN 1
 WHEN products > 70 AND products <= 75  THEN 2
 WHEN products > 65 AND products <= 70  THEN 3
 WHEN products > 60 AND products <= 65  THEN 4
 WHEN products > 55 AND products <= 60  THEN 5
 WHEN products > 50 AND products <= 55  THEN 6
 WHEN products > 49 AND products <= 50  THEN 7
 WHEN products > 45 AND products <= 49  THEN 8
 WHEN products > 40 AND products <= 45   THEN 9
 WHEN products <= 40 THEN 10 
 END ,0) AS "products [score]",


products_without_description ,
 
COALESCE( CASE 
 WHEN products_without_description > 0.50 THEN 0
 WHEN products_without_description > 0.45 AND products_without_description <= 0.50  THEN 1
 WHEN products_without_description > 0.40 AND products_without_description <= 0.45  THEN 2
 WHEN products_without_description > 0.35 AND products_without_description <= 0.40  THEN 3
 WHEN products_without_description > 0.30 AND products_without_description <= 0.35  THEN 4
 WHEN products_without_description > 0.25 AND products_without_description <= 0.30  THEN 5
 WHEN products_without_description > 0.20 AND products_without_description <= 0.25  THEN 6
 WHEN products_without_description > 0.15 AND products_without_description <= 0.20  THEN 7
 WHEN products_without_description > 0.10 AND products_without_description <= 0.15  THEN 8
 WHEN products_without_description > 0.05 AND products_without_description <= 0.10   THEN 9
 WHEN products_without_description <= 0.05 THEN 10 
 END , 0) AS "products_without_description [score]",
 
--------------------------------------------------------------------- OPS
automated_orders_perc,
 CASE 
 WHEN automated_orders_perc <  0.82 THEN 0
 WHEN automated_orders_perc >= 0.82 AND automated_orders_perc < 0.84  THEN 1
 WHEN automated_orders_perc >= 0.84 AND automated_orders_perc < 0.86  THEN 2
 WHEN automated_orders_perc >= 0.86 AND automated_orders_perc < 0.88  THEN 3
 WHEN automated_orders_perc >= 0.88 AND automated_orders_perc < 0.90  THEN 4
 WHEN automated_orders_perc >= 0.90 AND automated_orders_perc < 0.92  THEN 5
 WHEN automated_orders_perc >= 0.92 AND automated_orders_perc < 0.94  THEN 6
 WHEN automated_orders_perc >= 0.94 AND automated_orders_perc < 0.96  THEN 7
 WHEN automated_orders_perc >= 0.96 AND automated_orders_perc < 0.98  THEN 8
 WHEN automated_orders_perc >= 0.98 AND automated_orders_perc < 0.99  THEN 9
 WHEN automated_orders_perc >= 0.99 THEN 10 
 END AS "automation [score]",
 
cancellations,

                CASE 
                WHEN cancellations = 0 THEN 10
                WHEN cancellations > 0 AND cancellations <= 0.01 THEN 9
                WHEN cancellations > 0.01 AND cancellations <= 0.02 THEN 8
                WHEN cancellations > 0.02 AND cancellations <= 0.03 THEN 7
                WHEN cancellations > 0.03 AND cancellations <= 0.04 THEN 6
                WHEN cancellations > 0.04 AND cancellations <= 0.05 THEN 5
                WHEN cancellations > 0.05 AND cancellations <= 0.06 THEN 4
                WHEN cancellations > 0.06 AND cancellations <= 0.07 THEN 3
                WHEN cancellations > 0.07 AND cancellations <= 0.08 THEN 2
                WHEN cancellations > 0.08 AND cancellations <= 0.09 THEN 1
                WHEN cancellations > 0.09  THEN 0 END AS "cancellations [score]",
                
                processing_time,
                CASE 
                WHEN processing_time > 4 THEN 0
                WHEN processing_time > 3.7 AND processing_time <= 4 THEN 1
                WHEN processing_time > 3.3 AND processing_time <= 3.7 THEN 2
                WHEN processing_time > 3 AND processing_time <= 3.3 THEN 3
                WHEN processing_time > 2.7 AND processing_time <= 3 THEN 4
                WHEN processing_time > 2.3 AND processing_time <= 2.7 THEN 5
                WHEN processing_time > 2 AND processing_time <= 2.3 THEN 6
                WHEN processing_time > 1.7 AND processing_time <= 3 THEN 7
                WHEN processing_time > 1.3 AND processing_time <= 1.7 THEN 8
                WHEN processing_time > 1 AND processing_time <= 1.3 THEN 9
                WHEN processing_time <= 1  THEN 10 END AS "processing_time [score]",
                
                aov_eur,
                revenue,

                COALESCE( CASE 
                 WHEN revenue <= 5.5 THEN 0
                 WHEN revenue >= 5.5 AND revenue < 6  THEN 1
                 WHEN revenue >= 6 AND revenue < 6.5  THEN 2
                 WHEN revenue >= 6.5 AND revenue < 7  THEN 3
                 WHEN revenue >= 7 AND revenue < 7.5  THEN 4
                 WHEN revenue >= 7.5 AND revenue < 8  THEN 5
                 WHEN revenue >= 8 AND revenue < 8.5  THEN 6
                 WHEN revenue >= 8.5 AND revenue < 9  THEN 7
                 WHEN revenue >= 9 AND revenue < 9.5  THEN 8
                 WHEN revenue >= 9.5 AND revenue < 10   THEN 9
                 WHEN revenue >= 10 THEN 10 
                 END,0) AS "revenue [score]",
                

f.vendor_delay,
COALESCE(ops.vendor_delay,10) AS "vendor_delay [score]" ,
reliability_score,
prep_time_score,
waiting_time_score,
waiting_time_intercept,
prep_time_avg,
reliability_rate,

customer_complaints::numeric/NULLIF(valid_orders,0) as customer_complaints,

CASE 
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) = 0 THEN 10
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.01 THEN 8
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.01 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.02 THEN 7
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.02 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.03 THEN 6
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.03 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.04 THEN 4
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.04 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.05 THEN 2
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.05 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.06 THEN 0
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.06 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.07 THEN 0
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.07 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.08 THEN 0
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.08 AND customer_complaints::numeric/NULLIF(valid_orders,0) <= 0.09 THEN 0
WHEN customer_complaints::numeric/NULLIF(valid_orders,0) > 0.09  THEN 0 END AS "customer_complaints [score]",


 stage,
CASE 
WHEN (city_rank=1 or country_rank=1) and stage  IN (5,7,4)  THEN 10
WHEN (city_rank=1 or country_rank=1) and (stage IN (1,6) or stage is null) THEN 0
WHEN (city_rank=1 or country_rank=1) and stage  IN (2)    THEN 5
WHEN (city_rank=1 or country_rank=1) and stage  IN (3)  THEN 7 ELSE 10 END AS "dish level [score]", 
 
 gmv_class,
closing_hours,
                 CASE 
                WHEN closing_hours = 0 THEN 10
                WHEN closing_hours > 0     AND closing_hours <= 0.02 THEN 9
                WHEN closing_hours > 0.02  AND closing_hours <= 0.04 THEN 8
                WHEN closing_hours > 0.04  AND closing_hours <= 0.06 THEN 7
                WHEN closing_hours > 0.06  AND closing_hours <= 0.08 THEN 6
                WHEN closing_hours > 0.08  AND closing_hours <= 0.10 THEN 5
                WHEN closing_hours > 0.10  AND closing_hours <= 0.12 THEN 4
                WHEN closing_hours > 0.12  AND closing_hours <= 0.14 THEN 3
                WHEN closing_hours > 0.14  AND closing_hours <= 0.16 THEN 2
                WHEN closing_hours > 0.16  AND closing_hours <= 0.18 THEN 1
                WHEN closing_hours > 0.18  THEN 0 END AS "closing_hours [score]",
 
 
 
 
 
 city_rank,
 country_rank

 
 


FROM final f
LEFT JOIN  dwh_bl_fo.opportunity_ops_scores ops --another report built by logistics

ON f.rdbms_id = ops.rdbms_id and f.vendor_code::text = ops.vendor_code::text and f.report_month::text = ops.report_month::text
WHERE f.valid_orders >0
;


--check:
--SELECT * FROM dwh_bl_fo.opportunity_ops_scores



select
rdbms_id,
city_id,
vendor_id,
vendor_code,
report_month,
country_name,
city_name,
vendor_name,
am_name,
valid_orders,
commission_percentage,
gmv,
gfv,
aov_eur,
revenue,
reorder_rate,
cr3,
cancellations,
processing_time,
vendor_delay,
exclusivity,
backlink,
packaging_quality,
nps_packaging,
display,
voucher_cards,
door_sticker,
price_mark_up,
fb_advert_rights,
menucategories,
products,
products_without_description,
automated_orders_perc,
"revenue [score]",
"price_mark_up [score]",
"exclusivity [score]",
"facebook [score]",
"door_sticker [score]",
"voucher_cards [score]",
"display [score]",
"backlink [score]",
"rr [score]",
"cr3 [score]",
"menucategories [score]",
"products [score]",
"products_without_description [score]",
"packaging_quality [score]",
"automation [score]",
"cancellations [score]",
"processing_time [score]",
"vendor_delay [score]",
"commission [score]",
"commercial [score]",
"marketing [score]",
"conversion [score]",
"content [score]",
"ops [score]",
"final business [score]",
activated_date,
"3 month flag",
impact,
reliability_score,
prep_time_score,
waiting_time_score,
waiting_time_intercept,
prep_time_avg,
reliability_rate,
afv,
customer_complaints,
stage::int AS stage,
gmv_class,
closing_hours,
city_rank,
country_rank,
customer_complaints_score,
closing_hours_score,
dish_level_score,
pickup_orders,
delivery_orders,
accept_pickup


                         
FROM ( --d. scoring  --s. SF contracts
        SELECT
        d.rdbms_id,
        d.city_id,
        d.vendor_id, 
        d.vendor_code,
        d.report_month,
        d.country_name,
        d.city_name,
        d.vendor_name,
        d.am_name::text,
        ---------------------------------- GENERAL INFO
        valid_orders, 
        pickup_orders,
        delivery_orders,
        d.commission_percentage,
        d.gmv,
        d.gfv, 
        accept_pickup,
        d.aov_eur,
        d.revenue,
        ---------------------------------- CONVERSION
        d.reorder_rate,
        d.cr3, 
        ---------------------------------- OPS
        d.cancellations,
        d.processing_time,
        d.vendor_delay,
        ---------------------------------- SALESFORCE
        d.exclusivity,
        d.backlink,
        d.packaging_quality,
        d.nps_packaging,
        d.display,
        d.voucher_cards,
        d.door_sticker,
        d.price_mark_up,
        d.fb_advert_rights,
        ---------------------------------- PRODUCT
        d.menucategories,
        d.products,
        d.products_without_description ,
        automated_orders_perc,
        /*
        ************************   SCORES  **************************
        */
        "revenue [score]",
        "price_mark_up [score]",
        "exclusivity [score]" ,
        "facebook [score]",
        "door_sticker [score]",
        "voucher_cards [score]",
        "display [score]",
        "backlink [score]",
        "rr [score]",
        "cr3 [score]" ,
        "menucategories [score]",
        "products [score]" ,
        "products_without_description [score]",
        "packaging_quality [score]" ,
        "automation [score]",  
        "cancellations [score]",
        "processing_time [score]",
        "vendor_delay [score]",
        "commission [score]",
        
        
        /*
        ************************   FINAL SCORES  **************************
        */
        
        
        0.80 * "revenue [score]" + 0.1 * "price_mark_up [score]" + 0.1 * "exclusivity [score]" AS "commercial [score]", -- COMMERCIAL SCCORE
        0.35 * "facebook [score]" + 0.1 * "door_sticker [score]" + 0.1 * "voucher_cards [score]" + 0.1 * "display [score]" + 0.35 *  "backlink [score]"  AS "marketing [score]", --MARKETING SCORE
        0.2 * "rr [score]" + 0.8 * "cr3 [score]" AS "conversion [score]", --CONVERSION SCORE
        0.33 * "menucategories [score]" + 0.33 * "products [score]" + 0.34 * "products_without_description [score]"  AS "content [score]"  , -- CONTENT
        0.10 * "packaging_quality [score]"  + 0.15 * "automation [score]" + 0.15 * "cancellations [score]" + 0.6 * "vendor_delay [score]"    AS "ops [score]", -- OPS SCORE
        
        
        0.2* (0.80 * "revenue [score]" + 0.1 * "price_mark_up [score]" + 0.1 * "exclusivity [score]" ) +
        0.2* (0.35 * "facebook [score]" + 0.1 * "door_sticker [score]" + 0.1 * "voucher_cards [score]" + 0.1 * "display [score]" + 0.35 *  "backlink [score]") +
        0.1* (0.2 * "rr [score]" + 0.8 * "cr3 [score]" ) +
        0.1* (0.33 * "menucategories [score]" + 0.33 * "products [score]" + 0.34 * "products_without_description [score]" ) +
        0.4* (0.20 * "packaging_quality [score]"  + 0.2 * "automation [score]" + 0.1 * "cancellations [score]" + 0.1 * "processing_time [score]" + 0.4 * "vendor_delay [score]")    AS "final business [score]", --FINAL SCORE 
        activated_date,
        CASE WHEN current_date - activated_date ::date >= 90 THEN 1 ELSE 0 END AS "3 month flag",
        
        (10 - (0.2* (0.80 * "revenue [score]" + 0.1 * "price_mark_up [score]" + 0.1 * "exclusivity [score]" ) +
        0.2* (0.35 * "facebook [score]" + 0.1 * "door_sticker [score]" + 0.1 * "voucher_cards [score]" + 0.1 * "display [score]" + 0.35 *  "backlink [score]") +
        0.1* (0.2 * "rr [score]" + 0.8 * "cr3 [score]" ) +
        0.1* (0.33 * "menucategories [score]" + 0.33 * "products [score]" + 0.34 * "products_without_description [score]" ) +
        0.4* (0.10 * "packaging_quality [score]"  + 0.15 * "automation [score]" + 0.15 * "cancellations [score]" + 0.6 * "vendor_delay [score]") ) ) * gmv AS impact, --IMPACT
        reliability_score,
        prep_time_score,
        waiting_time_score,
        waiting_time_intercept,
        prep_time_avg,
        reliability_rate,
        afv,
        
        
        customer_complaints::NUMERIC AS customer_complaints,
        stage::INT AS stage ,
        gmv_class,
        closing_hours,
        city_rank,
        country_rank,
        "customer_complaints [score]" as customer_complaints_score,
        "closing_hours [score]" as closing_hours_score,
        "dish level [score]" as dish_level_score
        
        FROM scoring d --scoring
        LEFT JOIN salesforce_fo.il_dim_contracts s --contractds
        USING (rdbms_id, vendor_code) 
        )a





