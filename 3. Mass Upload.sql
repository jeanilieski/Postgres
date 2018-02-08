--select DISTINCT OPPORTUNITY  from dwh_st.natalia_achievable_opportunities
----- CREATE RECORD TYPES TABLE ----
--DROP TABLE record_types;
--CREATE TABLE record_types(
--   opportunity VARCHAR(50) NOT NULL PRIMARY KEY
--);
--INSERT INTO record_types(opportunity) VALUES ('Partner - Commission Renegotiation');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Gain Exclusivity');
--INSERT INTO record_types(opportunity) VALUES ('Partner - In-Restaurant Material');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Improve Menu/CR');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Automation Improvement');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Improve Cancellations');
--INSERT INTO record_types(opportunity) VALUES ('Partner - FB Advertiser Rights');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Packaging');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Improvement Prep-Time/Vendor Delay');
--INSERT INTO record_types(opportunity) VALUES ('Partner - OM Backlink');
--INSERT INTO record_types(opportunity) VALUES ('Partner - OM Whitelabel');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Pickup Activation');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Improve Offline Hours');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Improve Customer Complaints');
--INSERT INTO record_types(opportunity) VALUES ('Partner - Dish Level Photos');

--select * from record_types

------
WITH parameters AS(

SELECT * FROM (  --opp, opp_points, opp_lifespan, opp_area, low_limit_absolute, low_limit_scoring, record_type_id  
SELECT 
opportunity,
CASE    WHEN opportunity ='Partner - Commission Renegotiation'          THEN 1.6
        WHEN opportunity ='Partner - Gain Exclusivity'                  THEN 1.6
        WHEN opportunity ='Partner - In-Restaurant Material'            THEN 0.5
        WHEN opportunity ='Partner - Improve Menu/CR'                   THEN 0.7
        WHEN opportunity ='Partner - Automation Improvement'            THEN 0.35
        WHEN opportunity ='Partner - Improve Cancellations'             THEN 0.35
        WHEN opportunity ='Partner - FB Advertiser Rights'              THEN 0.8
        WHEN opportunity ='Partner - Packaging'                         THEN 0.7
        WHEN opportunity ='Partner - Improvement Prep-Time/Vendor Delay'THEN 0.35
        WHEN opportunity ='Partner - OM Backlink'                       THEN 1.0
        WHEN opportunity ='Partner - OM Whitelabel'                     THEN 1.0
        WHEN opportunity ='Partner - Dish Level Photos'                 THEN 1.6
        WHEN opportunity ='Partner - Pickup Activation'                 THEN 1
        WHEN opportunity ='Partner - Improve Offline Hours'             THEN 0.35
        WHEN opportunity ='Partner - Improve Customer Complaints'       THEN 0.35

END AS opportunity_points, 

CASE    WHEN opportunity ='Partner - Commission Renegotiation'          THEN '4 week'::interval
        WHEN opportunity ='Partner - Gain Exclusivity'                  THEN '4 week'::interval
        WHEN opportunity ='Partner - In-Restaurant Material'            THEN '1 week'::interval
        WHEN opportunity ='Partner - Improve Menu/CR'                   THEN '1 week'::interval
        WHEN opportunity ='Partner - Automation Improvement'            THEN '1 week'::interval
        WHEN opportunity ='Partner - Improve Cancellations'             THEN '1 week'::interval
        WHEN opportunity ='Partner - FB Advertiser Rights'              THEN '2 week'::interval
        WHEN opportunity ='Partner - Packaging'                         THEN '3 week'::interval
        WHEN opportunity ='Partner - Improvement Prep-Time/Vendor Delay'THEN '1 week'::interval
        WHEN opportunity ='Partner - OM Backlink'                       THEN '2 week'::interval
        WHEN opportunity ='Partner - OM Whitelabel'                     THEN '2 week'::interval
        WHEN opportunity ='Partner - Pickup Activation'                 THEN '3 week'::interval
        WHEN opportunity ='Partner - Improve Offline Hours'             THEN '1 week'::interval
        WHEN opportunity ='Partner - Improve Customer Complaints'       THEN '1 week'::interval

END AS opportunity_lifespan,

CASE    WHEN opportunity ='Partner - Commission Renegotiation'          THEN 'Commercial'
        WHEN opportunity ='Partner - Gain Exclusivity'                  THEN 'Commercial'
        WHEN opportunity ='Partner - In-Restaurant Material'            THEN 'Marketing-2'
        WHEN opportunity ='Partner - Improve Menu/CR'                   THEN 'Content'
        WHEN opportunity ='Partner - Automation Improvement'            THEN 'Operations'
        WHEN opportunity ='Partner - Improve Cancellations'             THEN 'Operations'
        WHEN opportunity ='Partner - FB Advertiser Rights'              THEN 'Marketing'
        WHEN opportunity ='Partner - Packaging'                         THEN 'Operations'
        WHEN opportunity ='Partner - Improvement Prep-Time/Vendor Delay'THEN 'Operations'
        WHEN opportunity ='Partner - OM Backlink'                       THEN 'Marketing'
        WHEN opportunity ='Partner - OM Whitelabel'                     THEN 'Marketing'
        WHEN opportunity ='Partner - Pickup Activation'                 THEN 'Commercial-2'
        WHEN opportunity ='Partner - Improve Offline Hours'             THEN 'Operations'
        WHEN opportunity ='Partner - Improve Customer Complaints'       THEN 'Operations'
END AS opportunity_area, 

CASE    WHEN opportunity ='Partner - FB Advertiser Rights'              THEN 2
        ELSE 0 
END AS low_limit_absolute,

CASE    WHEN opportunity ='Partner - FB Advertiser Rights'              THEN 4
        ELSE 0 
END AS low_limit_scoring, 

CASE    WHEN opportunity ='Partner - Commission Renegotiation'          THEN '01224000000FIia'
        WHEN opportunity ='Partner - Gain Exclusivity'                  THEN '01224000000FIif'
        WHEN opportunity ='Partner - In-Restaurant Material'            THEN '012240000002ohY'
        WHEN opportunity ='Partner - Improve Menu/CR'                   THEN '01224000000FIiu'
        WHEN opportunity ='Partner - Automation Improvement'            THEN '012240000002ohW'
        WHEN opportunity ='Partner - Improve Cancellations'             THEN '012240000002ohX'
        WHEN opportunity ='Partner - FB Advertiser Rights'              THEN '01224000000FIip'
        WHEN opportunity ='Partner - Packaging'                         THEN '01224000000fx8N'
        WHEN opportunity ='Partner - Improvement Prep-Time/Vendor Delay'THEN '01224000000FIik'
        WHEN opportunity ='Partner - OM Backlink'                       THEN '01224000000gDr1'
        WHEN opportunity ='Partner - OM Whitelabel'                     THEN ''
        WHEN opportunity ='Partner - Pickup Activation'                 THEN '012240000002rT3'
        WHEN opportunity ='Partner - Improve Offline Hours'             THEN '012240000006ucY'
        WHEN opportunity ='Partner - Improve Customer Complaints'       THEN '01224000000kPkF'
        WHEN opportunity ='Partner - Dish Level Photos'                 THEN ''
END AS record_type_id

FROM (
        SELECT DISTINCT opportunity from record_types)p 
        
) a 

where opportunity_points is not null and record_type_id is not null

)


--- HERE I AM GOING TO TAKE ALL OPEN KEY OPPORTUNITIES BY ORDER OF APPEARANCE (LATEST OF EACH KIND ONLY) AND DEFINE THEIR DAYS OPEN, I NEED TYPE, STAGE AND DAYS
--, open_opportunities AS(
SELECT
sf.account_owner as am_name, 
vendor_code,
b.*, 
p.opportunity_points,
COUNT (sf.account_owner) OVER (PARTITION BY sf.account_owner, "Opportunity Record Type" ) AS total_opportunities_per_type
FROM (
        SELECT 
        
        "Opportunity Record Type", 
        "18 Char Account ID", 
        "18 Char Opportunity ID",
        "Created Date",
        "Stage", 
        (current_date -"Created Date")::interval as age 
        
        FROM (
                SELECT 
                "Opportunity Record Type", 
                "18 Char Account ID", 
                "18 Char Opportunity ID",
                "Created Date",
                "Stage", 
                row_number() OVER (partition by "18 Char Account ID", "Opportunity Record Type" ORDER BY "Created Date" DESC) as order_of_appearance
                FROM salesforce_fo.foodora_all_opportunities  
                WHERE "Opportunity Record Type" IN (select opportunity from parameters) 
              ) a where order_of_appearance=1
        ) b 
LEFT JOIN parameters p 
ON "Opportunity Record Type"=opportunity 
LEFT JOIN salesforce_fo.il_dim_accounts sf 
ON "18 Char Account ID"=account_id

WHERE "Stage" NOT IN ('Closed Won','Closed Lost', 'Agreed','Photo Shoot Completed', 'Photos Edited' , 'Photos Uploaded') AND age <opportunity_lifespan and 
account_status='Active' and sf.account_owner='Justine Peyrat';

--AND sf.account_owner IN ('Miriam Andersen') ;

)

, AM_current_points AS(                                         --AM_current_points

SELECT
am_name, 
sum (opportunity_points) as open_opportunity_points

FROM open_opportunities

group by 1


)
, main_calculation_opportunities_queue AS(                     --main_calculation_opportunities_queue

SELECT *, 
SUM(opportunity_points) OVER (PARTITION BY am_name order by CASE WHEN rank_2 >= rank then rank_2 else rank end asc) AS opportunity_points_running_total 
                                                                                                                    --opportunity_points_running_total

FROM (
        SELECT 
        rdbms_id, 
        am_name, 
        CASE WHEN am_name IN ('Cathinka Kildal','Runar Wiig') THEN 15-COALESCE(open_opportunity_points,0) ELSE 25-COALESCE(open_opportunity_points,0) END 
                                                                                        as available_opportunity_points, --available_opportunity_points
        backend_partner_code AS vendor_code, 
        opportunity, 
        kpi_value,
        opportunity_points, 
        opportunity_lifespan,
        value,
        description,
        CASE WHEN (opportunity='Partner - FB Advertiser Rights' AND (COUNT(1) OVER (PARTITION BY am_name, opportunity order by value desc)+ open_opportunities_per_type) <=2 AND score <=6)then 1 
         WHEN (opportunity ='Partner - Commission Renegotiation' AND (COUNT(1) OVER (PARTITION BY am_name, opportunity order by value desc)+ open_opportunities_per_type) >=6 ) then 1000 
         WHEN (opportunity ='Partner - In-Restaurant Material' AND (COUNT(1) OVER (PARTITION BY am_name, opportunity, vendor_name order by value desc) >=2 ))then 1000     
         WHEN (opportunity ='Partner - Improvement Prep-Time/Vendor Delay' AND (COUNT(1) OVER (PARTITION BY am_name, opportunity order by value desc) >=10 )) then 1000  
         WHEN (opportunity ='Partner - OM Backlink' AND (COUNT(1) OVER (PARTITION BY am_name, opportunity order by value desc) >=4 )) then 1000  
         
        else rank end as rank,                                                                                                   --rank
        score,
        COUNT(1) OVER (PARTITION BY am_name, opportunity order by rank asc) AS opportunity_amount_running_total,
        open_opportunities_per_type,
        CASE WHEN (opportunity_area='Marketing' AND (COUNT(backend_partner_code) OVER (PARTITION BY rdbms_id, backend_partner_code, opportunity_area order by value desc) > 1)) THEN 1000 else rank end as rank_2 --rank2

        FROM dwh_st.natalia_achievable_opportunities
        LEFT JOIN parameters USING(opportunity)
        LEFT JOIN AM_cu  rrent_points USING (am_name) 
        LEFT JOIN (SELECT am_name, "Opportunity Record Type" as opportunity, MAX(total_opportunities_per_type) as open_opportunities_per_type FROM open_opportunities GROUP BY 1,2) oo USING (am_name, opportunity)
        where kpi_value is not null 
        --AND am_name  IN ('Kristin Chen')
        --ORDER BY value desc
               
) a 

where rank_2< 100 and rank <100

)


--SELECT * FROM main_calculation_opportunities_queue WHERE opportunity='Partner - FB Advertiser Rights' and rank<=10;-- and opportunity_points_running_total<=25;


, final AS(                                                                             --final
SELECT 
qu.*, 
sf.account_name|| ' - '||split_part(opportunity, ' - ',2) as opportunity_name,
p.record_type_id, 
sf.account_id, 
sf.account_name, 
u.user_id


FROM main_calculation_opportunities_queue qu
LEFT JOIN parameters p using (opportunity)
LEFT JOIN salesforce_fo.il_dim_accounts sf USING (rdbms_id, vendor_code)
LEFT JOIN salesforce_fo.il_dim_users u ON qu.rdbms_id=u.rdbms_id and qu.am_name=u.user_name

WHERE opportunity_points_running_total<= available_opportunity_points 
AND CASE WHEN opportunity!='Partner - FB Advertiser Rights' THEN value>=500 else score<=4 end  


)

SELECT 
am_name,
opportunity,
record_type_id, 
current_date as close_date,
'New' AS Stage, 
substring (account_id from 0 for 16 ) as account_id,
opportunity_name, 
user_id as owner_id,
description||'Please close this opportunity before '||(current_date + opportunity_lifespan)::date ||' otherwise it will automatically be set to "Closed Lost".' as description,
value

from final where user_id is not null --and am_name NOT IN ('Nikolaos Vlassis')