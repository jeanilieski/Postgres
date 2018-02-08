--DROP TABLE dwh_st.zhan_weekly_opportunities
CREATE TABLE dwh_st.zhan_weekly_opportunities AS(

SELECT * FROM (

        WITH marketing_scores_week AS ( --FIRST IMPORTANT TABLE: marketing_scores_week (SF)
        SELECT 
        am_name,
        vendor_code,
        commission_percentage,
        price_mark_up,
        exclusivity,
        packaging_quality,
        fb_advert_rights,
        facebook_likes,
        door_Sticker,
        voucher_cards,
        display,
        backlink,
        rdbms_id,
        flat_fee,
        no_fb
        
        from (

                SELECT 
                am_name,
                vendor_code,
                commission_percentage::double precision, --=active_commission_percentage FROM salesforce_fo.il_dim_contracts c
                price_mark_up::double precision,
                exclusivity::double precision,
                packaging_quality::double precision, --ops, not marketing
                fb_advert_rights::double precision,
                facebook_likes::double precision,
                door_sticker::double precision,
                voucher_cards::double precision,
                display::double precision,
                backlink::double precision,
                rdbms_id::int8,
                flat_fee::double precision, -- =commission_amount_eur FROM salesforce_fo.il_dim_contracts c
                no_fb::int
                
                FROM(
        
                        SELECT 
                        c.rdbms_id,
                        u.user_name AS am_name,
                        c.vendor_code,
                        (c.active_commission_percentage::double precision/100 )::double precision as commission_percentage,
                        COALESCE(c.commission_amount_eur,0) as flat_fee,
                        COALESCE(c.price_mark_up,0)::double precision as price_mark_up,
                        COALESCE(c.exclusivity,0)::double precision as exclusivity,
                        a.no_facebook as no_fb,
                        CASE 
                                  WHEN c.packaging_quality ~ 'Good Quality' THEN 1
                                  WHEN c.packaging_quality ~ 'Acceptable Quality' THEN 2
                                  WHEN c.packaging_quality ~ 'Poor Quality' THEN 3
                                  WHEN c.packaging_quality ~ 'Unacceptable Quality' THEN 4 ELSE 5
                        END ::double precision AS packaging_quality,
                        
                        COALESCE(CASE WHEN c.facebook_rights = 'Yes' THEN 1 ELSE 0 END ,0)::double precision as fb_advert_rights,
                        COALESCE(c.facebook_likes,0)::double precision as facebook_likes,
                        COALESCE(MAX(1) FILTER(WHERE o.product_name ~* 'Door Sticker'),0)::double precision AS Door_Sticker,
                        COALESCE(MAX(1) FILTER(WHERE o.product_name ~* 'vouch'),0)::double precision AS voucher_cards,
                        COALESCE(MAX(1) FILTER(WHERE o.product_name ~* 'display'),0)::double precision AS display,
                        COALESCE(MAX(1) FILTER(WHERE (o.opportunity_type ~* 'backlink' and implementation_phase='Implementation Complete') 
                        or o.opportunity_type ~* 'white' or partner_website IS NULL),0)::double precision AS backlink
                
                   
                        FROM salesforce_fo.il_dim_contracts c --SF contract
                        LEFT JOIN salesforce_fo.il_dim_opportunities o --SF opportunities
                        ON o.account_id=c.account_id 
                        LEFT JOIN salesforce_fo.il_dim_users u --SF users
                        ON c.account_owner = u.user_name AND c.rdbms_id = u.rdbms_id 
                        LEFT JOIN salesforce_fo.il_dim_accounts a --SF accounts
                        ON c.account_id = a.account_id
                        
                        
                        WHERE c.vendor_code is not null and c.status in ('Activated', 'Amended')
                        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)a)a


, ops_scores_week AS( --SECOND IMPORTANT TABLE: ops_scores_week (logistics)

SELECT 
rdbms_id::int, 
vendor_code::text, 
to_char(score_generated_at -'7d'::interval, 'iyyy-iw')::text as report_week, --report_week
CASE            
        WHEN overall_score >= 0 AND overall_score < 26 THEN 10 
        WHEN overall_score >= 26 AND overall_score < 60 THEN 9 
        WHEN overall_score >= 60 AND overall_score < 90 THEN 8 
        WHEN overall_score >= 90 AND overall_score < 120 THEN 7 
        WHEN overall_score >= 120 AND overall_score < 150 THEN 6
        WHEN overall_score >= 150 AND overall_score < 185 THEN 5 
        WHEN overall_score >= 185 AND overall_score < 215 THEN 4 
        WHEN overall_score >= 215 AND overall_score < 250 THEN 3 
        WHEN overall_score >= 250 AND overall_score < 280 THEN 2 
        WHEN overall_score >= 280 AND overall_score < 314 THEN 1 
        WHEN overall_score >= 314 THEN 0 END AS vendor_delay, --vendor_delay

CASE    WHEN reliability_score >= 0 AND reliability_score < 4 THEN 10 
        WHEN reliability_score >= 4 AND reliability_score < 29 THEN 9 
        WHEN reliability_score >= 29 AND reliability_score < 53 THEN 8 
        WHEN reliability_score >= 53 AND reliability_score < 78 THEN 7 
        WHEN reliability_score >= 78 AND reliability_score < 102 THEN 6
        WHEN reliability_score >= 102 AND reliability_score < 127 THEN 5 
        WHEN reliability_score >= 127 AND reliability_score < 151 THEN 4 
        WHEN reliability_score >= 151 AND reliability_score < 176 THEN 3 
        WHEN reliability_score >= 176 AND reliability_score < 200 THEN 2 
        WHEN reliability_score >= 200 AND reliability_score < 225 THEN 1 
        WHEN reliability_score >= 225 THEN 0 END AS reliability_score, --reliability_score

CASE    WHEN Prep_time_score >= 0 AND Prep_time_score < 9 THEN 10 
        WHEN Prep_time_score >= 9 AND Prep_time_score <  15 THEN 9 
        WHEN Prep_time_score >= 15 AND Prep_time_score < 21 THEN 8 
        WHEN Prep_time_score >= 21 AND Prep_time_score < 27 THEN 7 
        WHEN Prep_time_score >= 27 AND Prep_time_score < 33 THEN 6
        WHEN Prep_time_score >= 33 AND Prep_time_score < 40 THEN 5 
        WHEN Prep_time_score >= 40 AND Prep_time_score < 46 THEN 4 
        WHEN Prep_time_score >= 46 AND Prep_time_score < 52 THEN 3 
        WHEN Prep_time_score >= 52 AND Prep_time_score < 58 THEN 2 
        WHEN Prep_time_score >= 58 AND Prep_time_score < 64 THEN 1 
        WHEN Prep_time_score >= 64 THEN 0 END AS Prep_time_score, --Prep_time_score

CASE    WHEN waiting_time_score >= 0 AND waiting_time_score < 2 THEN 10 
        WHEN waiting_time_score >= 2 AND waiting_time_score < 5 THEN 9 
        WHEN waiting_time_score >= 5 AND waiting_time_score < 7 THEN 8 
        WHEN waiting_time_score >= 7 AND waiting_time_score < 10 THEN 7 
        WHEN waiting_time_score >= 10 AND waiting_time_score < 12 THEN 6
        WHEN waiting_time_score >= 12 AND waiting_time_score < 15 THEN 5 
        WHEN waiting_time_score >= 15 AND waiting_time_score < 17 THEN 4 
        WHEN waiting_time_score >= 17 AND waiting_time_score < 20 THEN 3 
        WHEN waiting_time_score >= 20 AND waiting_time_score < 22 THEN 2 
        WHEN waiting_time_score >= 22 AND waiting_time_score < 25 THEN 1 
        WHEN waiting_time_score >= 25 THEN 0 END AS waiting_time_score, --waiting time scores
          
waiting_time_intercept,
prep_time_avg,
reliability_rate


        
FROM (
        
WITH params AS(                                                         --params

SELECT
-- input parameters
  10 AS days,
  2.4 AS waiting_time_adjustment,
  10 AS minimum_orders,
  -- output parameters
  7 AS rider_early_tolerance,
  8 AS rider_late_tolerance,
  5 AS waiting_time_threshold,
  -- score parameters
  0.6 AS waiting_time_score_tolerance,
  1 AS waiting_time_score_weight,
  15 AS prep_time_score_tolerance,
  1 AS prep_time_score_weight,
  10 AS reliability_rate_tolerance,
  1 AS reliability_rate_weight  
),

order_data AS (         -- order level data: waiting time, prep time, redelivery, ...

SELECT 
  o.rdbms_id,
  o.vendor_id,
  v.city_id,
  o.order_date,
  o.order_code_google AS order_code,
  od.utilization,
  ROUND((EXTRACT(EPOCH FROM d.food_picked_up - d.rider_at_restaurant)/60)::numeric, 1) - 
  (SELECT waiting_time_adjustment FROM params) AS waiting_time,
  ROUND((EXTRACT(EPOCH FROM od.dispatcher_expected_pick_up_time - d.rider_at_restaurant)/60)::numeric, 1) 
  AS expected_pickup_minus_rider_arrival,
  od.estimated_prep_duration AS prep_time_used,
  od.estimated_prep_buffer AS prep_buffer_used,
  CASE 
    WHEN d.redelivery THEN 1
    ELSE 0
  END AS redelivery
FROM dwh_il_fo.fct_orders o --orders BE
LEFT JOIN dm_ops_fo.fct_orders od --orders --what is dif hurrier
ON o.rdbms_id = od.backend_rdbms_id AND o.order_code_google = od.order_code
--LEFT JOIN multi_deliveries md ON od.rdbms_id = md.rdbms_id AND od.disp_order_id = md.disp_order_id
LEFT JOIN dm_ops_fo.fct_deliveries d --deliveries
ON od.rdbms_id = d.rdbms_id AND od.disp_order_id = d.disp_order_id AND d.delivery_status = 'completed'

LEFT JOIN dm_ops_fo.hu_fct_delivery_timings dt --delivery_timings
ON d.rdbms_id = dt.rdbms_id AND d.delivery_id = dt.delivery_id

LEFT JOIN dwh_il_fo.dim_vendors v --vendors 
ON o.rdbms_id = v.rdbms_id AND o.vendor_id = v.vendor_id

LEFT JOIN dwh_il.dim_countries co --countries
ON o.rdbms_id = co.rdbms_id

LEFT JOIN dwh_il_fo.dim_ops_timestamps ot --ops_timestamps 
ON o.rdbms_id = ot.rdbms_id AND o.order_id = ot.order_id

LEFT JOIN dwh_il_fo.meta_order_status s --order_status
ON o.rdbms_id = s.rdbms_id AND o.status_id = s.status_id

WHERE 
  DATE(NOW()) - DATE(o.order_date) <= (SELECT days FROM params)
  AND od.estimated_prep_buffer IS NOT NULL
  AND od.estimated_prep_duration IS NOT NULL
  AND o.preorder = 0
  AND s.valid_order = 1
--AND md.deliveries IS NULL
ORDER BY o.order_date DESC
--AND o.order_code_google = 's7qz-v0lh'
)




, meta_data AS (                                        -- meta data, aggregated and filtered

SELECT 
  v.rdbms_id,
  --v.city_id,
  v.vendor_id,
  -- expected normalized waiting time for 'rider on time' cases
  ROUND(regr_intercept(GREATEST(od.waiting_time, 0), od.expected_pickup_minus_rider_arrival)                           --regression intercept
  FILTER (WHERE od.expected_pickup_minus_rider_arrival BETWEEN (SELECT - 1 * rider_late_tolerance FROM params) AND 
  (SELECT rider_early_tolerance FROM params))::numeric, 1) AS waiting_time_intercept,
  
  ROUND(regr_slope(GREATEST(od.waiting_time, 0), od.expected_pickup_minus_rider_arrival)                                --regression slope
  FILTER (WHERE od.expected_pickup_minus_rider_arrival BETWEEN (SELECT - 1 * rider_late_tolerance FROM params) AND 
  (SELECT rider_early_tolerance FROM params))::numeric, 2) AS waiting_time_slope,
  
  --ROUND(AVG(ot.utilization), 1) AS avg_utilization,
  ROUND(AVG(od.prep_time_used) FILTER (WHERE od.utilization < 50), 1) AS prep_time_avg,
  
  ROUND((COUNT(od.waiting_time) FILTER (WHERE od.waiting_time > (SELECT waiting_time_threshold FROM params) AND 
  od.expected_pickup_minus_rider_arrival BETWEEN -3 AND 7))::numeric / GREATEST(COUNT(od.order_code), 1) * 100.0, 1) AS reliability_rate,
  
  ROUND(SUM(od.redelivery)::numeric / COUNT(od.order_code) * 100.0, 3) AS redelivery_rate,
  
  COUNT(od.order_code) AS orders_evaluated
  
FROM dwh_il_fo.dim_vendors v -- v vendors

LEFT JOIN order_data od -- od  order_data
ON v.rdbms_id = od.rdbms_id AND v.vendor_id = od.vendor_id

WHERE   v.vendor_active = 1
GROUP BY 1,2
)


, vendor_scores AS( --vendor_scores :rdbms, id, waiting_time, preptime, reliability

SELECT
  rdbms_id,
  vendor_id,
  quadratic_score(waiting_time_intercept, 
  (SELECT waiting_time_score_tolerance FROM params), --quadratic_score
  (SELECT waiting_time_score_weight FROM params)) AS waiting_time_score,
  quadratic_score(prep_time_avg, (SELECT prep_time_score_tolerance FROM params), 
  (SELECT prep_time_score_weight FROM params)) AS prep_time_score,
  quadratic_score(reliability_rate, (SELECT reliability_rate_tolerance FROM params), 
  (SELECT reliability_rate_weight FROM params)) AS reliability_score,
  -- FIXME: parametrize redelivery_score properly: log?
  ROUND((redelivery_rate * 10)^2, 1)*0 AS redelivery_score,                                    --^
  orders_evaluated,
  waiting_time_intercept,
prep_time_avg,
reliability_rate

FROM dwh_il_fo.dim_vendors v 

LEFT JOIN meta_data md USING (rdbms_id, vendor_id)

LEFT JOIN quant_fo.city_properties cp 
ON v.rdbms_id=backend_rdbms_id and v.city_id=cp.backend_city_id

WHERE 
  orders_evaluated > (SELECT minimum_orders FROM params)
ORDER BY orders_evaluated DESC
)


SELECT 
  v.rdbms_id,
  c.city_name,
  v.vendor_id,
  v.vendor_name, 
  v.vendor_code,
  vs.waiting_time_score,
  vs.prep_time_score,
  vs.reliability_score,
  vs.redelivery_score,
  vs.orders_evaluated,
  vs.waiting_time_score + vs.prep_time_score + vs.reliability_score + vs.redelivery_score AS overall_score,
  
  
waiting_time_intercept,
prep_time_avg,
reliability_rate,
  
rank() OVER (PARTITION BY 1 ORDER BY vs.waiting_time_score + vs.prep_time_score + vs.reliability_score + vs.redelivery_score DESC) AS global_rank,
rank() OVER (PARTITION BY v.rdbms_id ORDER BY vs.waiting_time_score + vs.prep_time_score + vs.reliability_score + vs.redelivery_score DESC) 
AS country_rank,
NOW() AS score_generated_at
--,v.vendor_name
FROM dwh_il_fo.dim_vendors v --vendor
LEFT JOIN vendor_scores vs USING (rdbms_id, vendor_id)
LEFT JOIN dwh_il_fo.dim_city c --city
ON v.rdbms_id=c.rdbms_id AND v.city_id=c.city_id 
WHERE
  -- only take rows where we have a score (i.e. more than 10 orders)
  vs.vendor_id IS NOT NULL 
  
ORDER BY 1,2 DESC
) a

)

--, final AS(
SELECT * FROM (

WITH time_params AS ( --time_params
SELECT

(date_trunc('week', NOW())  - '1 week'::interval)::DATE as start_week,
(date_trunc('week', NOW())  - '1 day'::interval)::DATE as end_week
)

,dates AS( --dates
SELECT DISTINCT
iso_date,
iso_full_week_string,
us_full_month_string,
iso_digit_day_of_week
FROM dwh_il.dim_date 
WHERE iso_date BETWEEN (SELECT start_week::date from time_params) AND (SELECT end_week::date from time_params) ORDER BY 1
),
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------DATA SET--------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

data_set as (    --data_set rdbms, v_id, v_code, city, week, country, 
        SELECT DISTINCT
        v.rdbms_id,
        v.vendor_id,
        v.vendor_code,
        c.city_id,
        d.iso_full_week_string as report_week,
        co.common_name as country_name,
        c.city_name as city_name,
        v.vendor_name
        
        FROM  dwh_il_fo.dim_vendors v
        CROSS JOIN  dates d
        LEFT JOIN dwh_il_fo.dim_city c ON v.rdbms_id=c.rdbms_id and v.city_id=c.city_id
        LEFT JOIN dwh_il.dim_countries co ON v.rdbms_id = co.rdbms_id
        WHERE co.live=1 and c.active=1 and co.company_name='Foodora'
        GROUP BY 1,2,3,4,5,6,7,8
),

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------OORDERS ------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

valid_orders AS (   --valid_orders

        SELECT o.* 
        FROM dwh_il_fo.fct_orders o 
        LEFT JOIN dwh_il.meta_order_status os 
        ON o.rdbms_id=os.rdbms_id 
        AND o.status_id=os.status_id WHERE os.valid_order=1 and o.order_id>0)


,ops_order_info AS (   --ops_order_info: valid, gross, actionable, automated, processing time, delay 

            SELECT
            o.rdbms_id,
            o.city_id,
            o.vendor_id,
            v.vendor_code,
            iso_full_week_string as report_week,
            c.city_name,
            COUNT(o.order_id) FILTER (WHERE s.valid_order=1) as valid_orders,
            COUNT(o.order_id) FILTER (WHERE s.gross_order=1) as gross_orders,
            COUNT(o.order_id) FILTER (WHERE s.failed_order_vendor=1) as failed_orders_vendor,
            SUM(o.gmv_eur) FILTER (WHERE s.valid_order=1) AS gmv_eur,
            SUM(o.gfv_eur) FILTER (WHERE s.valid_order=1) AS gfv_eur,
            SUM(EXTRACT (EPOCH FROM ot.vendor_confirmation_end::TIMESTAMP-ot.vendor_confirmation_start::TIMESTAMP)::DECIMAL(20,2)/60) 
            AS processing_time,
            COUNT(o.order_id) FILTER (WHERE ot.vendor_confirmation_start IS NOT NULL AND ot.vendor_confirmation_end IS NOT NULL) 
            AS processing_time_count,
            COUNT(o.order_id) FILTER (WHERE ot.code_array && ARRAY[51,52,53,55,56,57,59,591,592]) AS actionable_orders,
            COUNT(o.order_id) FILTER (WHERE ot.code_array && ARRAY[51,52,53,55,56,57,59,591,592] and s.valid_order=1)::double precision
            /NULLIF(COUNT(o.order_id) FILTER (WHERE s.valid_order=1),0)::double precision AS automated_orders,                             
            COUNT(o.order_id) FILTER (WHERE bl.vendor_late >= '00:05:00' and bl.courier_late < '00:05:00')as vendor_delay_sum
    
    FROM dates d
    LEFT JOIN dwh_il_fo.fct_orders o ON d.iso_date=o.order_date::date
    LEFT JOIN dwh_il.dim_countries co ON o.rdbms_id=co.rdbms_id
    LEFT JOIN dwh_il_fo.dim_vendors v ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id
    LEFT JOIN dwh_il_fo.dim_city c ON o.rdbms_id=c.rdbms_id AND o.city_id=c.city_id
    LEFT JOIN dwh_il_fo.meta_order_status s ON o.rdbms_id=s.rdbms_id AND s.status_id=o.status_id
    LEFT JOIN dwh_il_fo.dim_ops_timestamps ot ON o.rdbms_id=ot.rdbms_id AND o.order_id=ot.order_id    
    
   
    LEFT JOIN (
                    SELECT DISTINCT ops_o.backend_rdbms_id as rdbms_id, ops_o.order_id, t.vendor_late, t.courier_late 
              
                    FROM dm_ops_fo.fct_orders ops_o
                    LEFT JOIN dm_ops_fo.fct_deliveries d 
                    USING (disp_order_id, rdbms_id)
                    LEFT JOIN dm_ops_fo.hu_fct_delivery_timings t 	-- t delivery_timings
                    ON d.rdbms_id=t.rdbms_id AND d.delivery_id = t.delivery_id
                    WHERE ops_o.created_at BETWEEN (SELECT start_week from time_params) AND (SELECT end_week from time_params)
               )  bl ON o.rdbms_id=bl.rdbms_id AND o.order_id=bl.order_id

    WHERE  co.live=1 and not c.is_test
    AND iso_date BETWEEN (SELECT start_week from time_params) AND (SELECT end_week from time_params)
    GROUP BY 1,2,3,4,5,6
    ORDER BY 1,2,3,4,5,6 DESC
) 


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------  CONVERSION RATES    ------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    
,conversion_rate as (
SELECT *

---- I commentd this out as we are not giving conversion opportunities yet, we need to clean score and approach and then we re-approach this type of opportunities ------
    FROM(   SELECT 
            0 as rdbms_id,
            to_char(current_date::date-'7day'::interval,'iyyy-iw') as report_week,
            0 as vendor_id,
            0  as cr3

--            FROM dwh_bl_fo.ga_vendor_conversion_report v
--            LEFT JOIN dwh_il_fo.dim_vendors ve ON v.rdbms_id=ve.rdbms_id AND v.vendor_code=ve.vendor_code
--            
--            WHERE v.vendor_code is not null /*and report_date BETWEEN (SELECT start_week from time_params) AND (SELECT end_week from time_params)*/
--            GROUP BY 1,2,3 
--             v.rdbms_id,
--            to_char(report_date::date,'iyyy-iw') as report_week,
--            V.vendor_id,
--            SUM(cr3_end) / SUM(cr3_start)  as cr3
--
--            FROM dwh_bl_fo.ga_vendor_conversion_report v
--            LEFT JOIN dwh_il_fo.dim_vendors ve ON v.rdbms_id=ve.rdbms_id AND v.vendor_code=ve.vendor_code
--            
--            WHERE v.vendor_code is not null /*and report_date BETWEEN (SELECT start_week from time_params) AND (SELECT end_week from time_params)*/
--            GROUP BY 1,2,3 
            
            ) g
    
    WHERE cr3 is not null
), 

product as (

SELECT 
vc.rdbms_id,ci.city_id, vc.vendor_id, vc.menucategories, vc.products, vc.p_descriptions
FROM  dwh_il_fo.dim_vendors v 
LEFT JOIN dwh_il_fo.dim_city ci 
ON ci.rdbms_id=v.rdbms_id AND ci.city_id=v.city_id
LEFT JOIN dwh_il_fo.dim_vendor_content vc 
on v.rdbms_id=vc.rdbms_id AND v.vendor_id=vc.vendor_id

),

-------------VENDOR LEVEL REORDER RATES
first_order AS (
  SELECT o.rdbms_id,
  o.vendor_id ,
  o.customer_id, 
  MIN(o.order_date::date) as first_order_date 
  FROM valid_orders o GROUP BY 1,2,3)

, first_order_report_week AS 
(
        SELECT o.rdbms_id, o.vendor_id, o.customer_id, 
        MIN(o.order_date::date) AS first_order_date
        FROM dwh_il_fo.fct_orders o 
        LEFT JOIN dwh_il_fo.meta_order_status os 
        ON o.rdbms_id = os.rdbms_id AND o.status_id = os.status_id
        WHERE  os.valid_order=1
        AND to_char(o.order_date,'yyyy-iw')=to_char((SELECT start_week::date from time_params), 'iyyy-iw')
        GROUP BY 1,2,3
)
,all_customers_reorder_rates_vendor AS ( 
        SELECT
        f.rdbms_id, f.vendor_id,
        COUNT(DISTINCT(CASE WHEN date_part('day', CURRENT_DATE - f.first_order_date::timestamp)>28 THEN f.customer_id ELSE NULL END)) AS base_cust,
        COUNT(DISTINCT(CASE WHEN date_part('day', CURRENT_DATE - f.first_order_date::timestamp)>28
                                AND date_part('day', o.order_date::timestamp - first_order_date::timestamp)>0 
                                AND date_part('day', o.order_date::timestamp - first_order_date::timestamp)<=28 
                                AND o.vendor_id=f.vendor_id THEN f.customer_id END)) AS cust_4w
        FROM first_order_report_week f
        LEFT JOIN dwh_il_fo.fct_orders o -- fact orders
        ON f.rdbms_id=o.rdbms_id AND f.customer_id=o.customer_id
        LEFT JOIN dwh_il_fo.meta_order_status os --order status
        ON o.rdbms_id = os.rdbms_id AND o.status_id = os.status_id

        WHERE  os.valid_order=1
        GROUP BY 1,2

)



, rr_wow AS (
SELECT 
rdbms_id,
vendor_id,
to_char((SELECT start_week::date from time_params), 'iyyy-iw') as report_week,
base_cust as base_cust_4,
cust_4w as cust_1month_4
FROM all_customers_reorder_rates_vendor
)
,
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------   ACTIVATIONS     ------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

nps as (
                                SELECT
                                v.rdbms_id, 
                                v.city_id,
                                v.vendor_id,
                                to_char(date_submitted, 'iyyy-iw') as report_week, 
                                COALESCE(ROUND(AVG (recommendation::int), 2),0) AS nps_packaging
                                FROM survey_gizmo_fo.survey_gizmo_nps n
                                LEFT JOIN dwh_il.dim_countries co ON upper(split_part(n.venture_name,'_',2))=co.country_iso
                                LEFT JOIN dwh_il_fo.fct_orders o ON o.rdbms_id = co.rdbms_id AND o.order_id = n.last_order_id
                                LEFT JOIN dwh_il_fo.dim_vendors v ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id
                                LEFT JOIN dwh_il_fo.dim_city c ON o.rdbms_id=c.rdbms_id AND o.city_id=c.city_id
                                
                                WHERE date_submitted::date BETWEEN (SELECT start_week from time_params) AND (SELECT end_week from time_params)
                                AND  main_reason_for_recommendation_score IN ('Packaging')
                                
                                AND NOT c.is_test AND c.active=1 
                                GROUP BY 1,2,3,4
                            
)

, zendesk  AS (

        SELECT
            TO_CHAR(o.order_date::date, 'iyyy-iw') as report_week, o.rdbms_id, v.vendor_code,
            COUNT(DISTINCT o.order_id ) FILTER (WHERE customer_contact_reason_updated ~* '3D.' or customer_contact_reason_updated ~* '3C.' or 
            customer_contact_reason_updated ~* '3B.') as Customer_Complaints
        FROM dwh_il.dim_date d --date
        LEFT JOIN( SELECT o.order_date, o.rdbms_id, o.vendor_id, o.order_id, o.order_code_google, o.status_id, a.products_plus_vat as gfv_local 
                FROM dwh_il_fo.fct_orders o --orders
                LEFT JOIN dwh_il_fo.fct_accounting a ON a.rdbms_id=o.rdbms_id AND o.order_id=a.order_id 
        ) o ON d.iso_date=o.order_date::date
        
        LEFT JOIN dwh_il_fo.fct_zendesk z --fact zendesk
        ON z.rdbms_id=o.rdbms_id AND o.order_code_google=z.order_code
        LEFT JOIN dwh_il_fo.meta_order_status s --order status
        ON o.rdbms_id=s.rdbms_id AND s.status_id=o.status_id 
        LEFT JOIN  dwh_il_fo.dim_vendors v --vendor
        ON o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id

        WHERE o.order_date BETWEEN (SELECT start_week from time_params) AND (SELECT end_week from time_params) and  s.valid_order = 1 
        GROUP BY 1,2,3 
)

---
select * from dwh_il_fo.meta_order_status --titile
select * from dwh_il_fo.fct_orders --order id, cust id, order code google, preorder, expedition_type IN ('pickup')
select * from dwh_il_fo.dim_zendesk_reasons

select * from dwh_il_fo.fct_zendesk
select distinct customer_contact_reason from  dwh_il_fo.fct_zendesk --fct_zendesk_events, fct_zendesk_sessions
select distinct customer_problem_reason from  dwh_il_fo.fct_zendesk






, closing_hours  AS (

SELECT
rdbms_id, 
vendor_id, 
--to_char(report_date, 'iyyy-iw') as report_week , 
SUM(COALESCE(closed_hours_num,0))::numeric/ SUM(open_hours_num) as closing_hours

FROM dwh_bl.restaurant_offline_report  
--WHERE  to_char(report_date, 'iyyy-iw')=to_char((SELECT start_week::date from time_params), 'iyyy-iw')
GROUP BY 1,2 --,3
)

select * FROM dwh_bl.restaurant_offline_report 







                                --final (city_rank, country_rank, cancellations, gmv, gfv, revenue, aov, automation, procesing_time,
                                -- acctionable_o, automated_o, ror, v_delay, 

, final  AS (                                                   
SELECT *,
CASE WHEN ((ntile(100) OVER(PARTITION BY d.rdbms_id, d.city_id, d.report_week ORDER BY gmv_eur DESC))) <= 10 THEN 1 else 0 end as city_rank,
CASE WHEN ((ntile(100) OVER(PARTITION BY d.rdbms_id, d.report_week ORDER BY gmv_eur DESC)))<=35 THEN 1 else 0 end as country_rank
FROM(

SELECT  DISTINCT

d.rdbms_id,
d.city_id,
d.vendor_id, 
d.vendor_code,
d.report_week,
d.country_name,
d.city_name,
d.vendor_name,
--accept_pickup,

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
cr3,
COALESCE(gmv_eur,0) as gmv_eur,

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
valid_orders,
--pickup_orders,
--delivery_orders,
gross_orders,
failed_orders_vendor,
(failed_orders_vendor)::double precision /NULLIF(gross_orders,0)::double precision as cancellations, --cancellations
gmv_eur as gmv,                                                                                         --gmv=gfv+delivery_fee
gfv_eur as gfv,                                                                                         --gfv=gmv-delivery_fee
COALESCE(((gfv_eur::double precision / NULLIF(valid_orders,0)::double precision) * commission_percentage::double precision),0) + 
(COALESCE((valid_orders * flat_fee),0)::double precision / NULLIF(valid_orders,0)::double precision) as revenue,                --revenue
(gmv_eur::double precision) /NULLIF(valid_orders::double precision,0) as aov_eur, --aov= gmv/ valid_orders ?
gfv_eur::double precision /NULLIF(valid_orders::double precision,0) as afv, --afv =gfv/ valid_orders
1- ((actionable_orders)::double precision /NULLIF(valid_orders,0)::double precision)::double precision as automation, --automation=1-((actionable_o / valid_o)
processing_time::double precision/NULLIF(processing_time_count, 0)::double precision as processing_time,

--select * from dwh_il.fct_orders

actionable_orders,                                                                              --actionable_orders
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
 --stage,
 gmv_class,
 closing_hours
 

FROM data_set d

LEFT JOIN ops_order_info f USING (rdbms_id, vendor_id, report_week)

LEFT JOIN conversion_rate c USING (rdbms_id, vendor_id, report_week)
LEFT JOIN rr_wow rr USING (rdbms_id, vendor_id, report_week)
LEFT JOIN closing_hours  ch USING (rdbms_id, vendor_id, report_week)
LEFT JOIN product pp USING (rdbms_id, vendor_id)
LEFT JOIN nps nps USING(rdbms_id, vendor_id, report_week)
LEFT JOIN marketing_scores_week   m 
ON m.rdbms_id = d.rdbms_id AND m.vendor_code = d.vendor_code --AND m.report_week=d.report_week
LEFT JOIN  zendesk z 
ON z.rdbms_id = d.rdbms_id AND z.vendor_code = d.vendor_code AND z.report_week=d.report_week

LEFT JOIN  dwh_bl.vendor_gmv_class   cl  ON cl.rdbms_id = d.rdbms_id AND cl.vendor_code = d.vendor_code  and  cl.company IN ('Foodora')  
WHERE d.report_week BETWEEN (SELECT to_char(start_week, 'iyyy-iw') from time_params) AND (SELECT to_char(end_week, 'iyyy-iw') from time_params) 
)d )




, scoring  AS (


SELECT

f.rdbms_id,
city_id,
vendor_id, 
f.vendor_code,
f.report_week,
country_name,
city_name,
vendor_name,
am_name::text,
--------------------------------------------------------------------- COMMISSION
valid_orders, 
--pickup_orders,
--delivery_orders,
commission_percentage,
gmv, 
gfv,
afv,
---accept_pickup,
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


 7 as stage, 10  AS "dish level [score]", 
 
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

LEFT JOIN  ops_scores_week ops 
ON f.rdbms_id = ops.rdbms_id and f.vendor_code::text = ops.vendor_code::text and 
f.report_week::text = ops.report_week::text
WHERE f.valid_orders >0 
)

select
rdbms_id,
city_id,
vendor_id,
vendor_code,
report_week,
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
dish_level_score
--pickup_orders,
--delivery_orders,
--accept_pickup


--
--SELECT 
--rdbms_id,
--city_id,
--vendor_id,
--vendor_code,
--report_month,
--country_name,
--city_name,
--vendor_name,
--am_name,
--valid_orders
--commission_percentage,
--gmv,
--gfv,
--aov_eur,
--revenue,
--reorder_rate,
--cr3,
--cancellations,
--processing_time,
--vendor_delay,
--exclusivity,
--backlink,
--packaging_quality,
--nps_packaging,
--display,
--voucher_cards,
--door_sticker,
--price_mark_up,
--fb_advert_rights,
--menucategories,
--products,
--products_without_description,
--automated_orders_perc,
--accept_pickup,
--"revenue [score]",
--"price_mark_up [score]",
--"exclusivity [score]",
--"facebook [score]",
--"door_sticker [score]",
--"voucher_cards [score]",
--"display [score]",
--"backlink [score]",
--"rr [score]",
--"cr3 [score]",
--"menucategories [score]",
--"products [score]",
--"products_without_description [score]",
--"packaging_quality [score]",
--"automation [score]",
--"cancellations [score]",
--"processing_time [score]",
--"vendor_delay [score]",
--"commission [score]",
--"commercial [score]",
--"marketing [score]",
--"conversion [score]",
--"content [score]",
--"ops [score]",
--"final business [score]",
--activated_date,
--"3 month flag",
--impact,
--reliability_score,
--prep_time_score,
--waiting_time_score,
--waiting_time_intercept,
--prep_time_avg,
--reliability_rate,
--afv,
--
--customer_complaints,
--stage,
--gmv_class,
--closing_hours,
--city_rank,
--country_rank,
--customer_complaints_score,
--closing_hours_score,
--dish_level_score,
--valid_orders,
--pickup_orders,
--delivery_orders

FROM (
SELECT
d.rdbms_id,
d.city_id,
d.vendor_id, 
d.vendor_code,
d.report_week,
d.country_name,
d.city_name,
d.vendor_name,
d.am_name::text,
---------------------------------- GENERAL INFO
valid_orders, 
--pickup_orders,
--delivery_orders,
d.commission_percentage,
d.gmv,
d.gfv, 
--accept_pickup,
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


0.80 * "revenue [score]" + 0.1 * "price_mark_up [score]" + 0.1 * "exclusivity [score]" AS "commercial [score]",
0.35 * "facebook [score]" + 0.1 * "door_sticker [score]" + 0.1 * "voucher_cards [score]" + 0.1 * "display [score]" + 0.35 *  "backlink [score]"  AS "marketing [score]",
0.2 * "rr [score]" + 0.8 * "cr3 [score]" AS "conversion [score]",
0.33 * "menucategories [score]" + 0.33 * "products [score]" + 0.34 * "products_without_description [score]"  AS "content [score]"  ,
0.10 * "packaging_quality [score]"  + 0.15 * "automation [score]" + 0.15 * "cancellations [score]" + 0.6 * "vendor_delay [score]"    AS "ops [score]",


0.2* (0.80 * "revenue [score]" + 0.1 * "price_mark_up [score]" + 0.1 * "exclusivity [score]" ) +
0.2* (0.35 * "facebook [score]" + 0.1 * "door_sticker [score]" + 0.1 * "voucher_cards [score]" + 0.1 * "display [score]" + 0.35 *  "backlink [score]") +
0.1* (0.2 * "rr [score]" + 0.8 * "cr3 [score]" ) +
0.1* (0.33 * "menucategories [score]" + 0.33 * "products [score]" + 0.34 * "products_without_description [score]" ) +
0.4* (0.20 * "packaging_quality [score]"  + 0.2 * "automation [score]" + 0.1 * "cancellations [score]" + 0.1 * "processing_time [score]" + 0.4 * "vendor_delay [score]")    AS "final business [score]", 
activated_date,
CASE WHEN current_date - activated_date ::date >= 90 THEN 1 ELSE 0 END AS "3 month flag",

(10 - (0.2* (0.80 * "revenue [score]" + 0.1 * "price_mark_up [score]" + 0.1 * "exclusivity [score]" ) +
0.2* (0.35 * "facebook [score]" + 0.1 * "door_sticker [score]" + 0.1 * "voucher_cards [score]" + 0.1 * "display [score]" + 0.35 *  "backlink [score]") +
0.1* (0.2 * "rr [score]" + 0.8 * "cr3 [score]" ) +
0.1* (0.33 * "menucategories [score]" + 0.33 * "products [score]" + 0.34 * "products_without_description [score]" ) +
0.4* (0.10 * "packaging_quality [score]"  + 0.15 * "automation [score]" + 0.15 * "cancellations [score]" + 0.6 * "vendor_delay [score]") ) ) * gmv as impact,
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

FROM scoring d
LEFT JOIN salesforce_fo.il_dim_contracts s USING (rdbms_id, vendor_code) 
)a )a )a)

