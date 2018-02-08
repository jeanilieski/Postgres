


----------------



WITH time_params  AS( 

SELECT
--(date_trunc('month', NOW())  - '1 month'::interval)::DATE as start_month,
--(date_trunc('month', NOW())  - '1 day'::interval)::DATE as end_month, 
to_char(current_date-'1month'::interval, 'yyyy-mm') as report_month_tp

)

, cl_types AS( --Closed Lost Types (untouched, worked on)

select s."18 Char Opportunity ID",
s."Stage",  s."Created Date", s."Last Modified By", z.opportunity_lifespan,"Close Date"::date,
(s."Created Date"::DATE + z.opportunity_lifespan::interval  +  '1 day'::interval)::DATE as opportunity_due_date
FROM salesforce_fo.foodora_all_opportunities s
left join dwh_st.zhan_opportunity_metrics z
on z.opportunity_record_type = s."Opportunity Record Type" 
where s."Stage" in ('Closed Lost') and s."Last Modified By" IN ('Josefine Spott') 

)  


SELECT *,
case    when  stage_category IN ('Closed Lost') and untouched IN (1) then 'Closed Lost untouched'
        when  stage_category IN ('Closed Lost') and untouched IN (0) then 'Closed Lost worked on'
        else  'Closed Won' end as stages
FROM
(SELECT * FROM
        (SELECT 
        to_char (s."Close Date", 'yyyy-mm') as report_month,  
                split_part (opportunity_record_type, ' - ',2) as opportunity_type, --split
  
        --"18 Char Opportunity ID" is the unique id ('00624000009LcBqAAK')
        a.rdbms_id, a.country_name, a.city_name, a.account_owner, a.account_name,
        --u.salesforce_profile, u.salesforce_role, u.account_manager,
        --z.area_weight, z.opportunity_weight,
        
        case 
             when s."Stage" in ('Closed Won', 'Photo Shoot Completed', 'Photos Edited', 'Photos Uploaded') then 'Closed Won'
             when s."Stage"='New' then 'New'
             when s."Stage"='Closed Lost' then 'Closed Lost'
             else 'Open'
             end as Stage_category,
             
        z.opportunity_points, z.opportunity_area,  o.impact, z.area_weight * z.opportunity_weight * o.impact as opportunity_type_impact,
        case when ct."Last Modified By" IN ('Josefine Spott') and s."Close Date"::date = opportunity_due_date then 1 else 0 end as untouched
             
        
        FROM salesforce_fo.foodora_all_opportunities s
        
        LEFT JOIN salesforce_fo.il_dim_accounts a --SF accounts
        ON s."18 Char Account ID" = a.account_id   
        
        LEFT JOIN salesforce_fo.il_dim_contracts c
        ON c.account_id = a.account_id
        
        LEFT JOIN dwh_bl_fo.opportunity_all_scores o
        on o.rdbms_id=a.rdbms_id and o.vendor_code=a.vendor_code and to_char (s."Close Date", 'yyyy-mm')=o.report_month
        
        LEFT JOIN salesforce_fo.il_dim_users u --SF users
        ON c.account_owner = u.user_name AND c.rdbms_id = u.rdbms_id --and u.user_name=o.am_name
        
        LEFT JOIN dwh_st.zhan_opportunity_metrics z
        on z.opportunity_record_type = s."Opportunity Record Type"
        
        LEFT JOIN cl_types ct
        on ct."18 Char Opportunity ID" = s."18 Char Opportunity ID"     
        
        where  z.opportunity_record_type is not null and
        to_char (s."Close Date", 'yyyy-mm') = (Select report_month_tp from time_params) and 
        case when   "Opportunity Record Type" in ('Partner - OM Backlink') then s."Created By" IN ('Josefine Spott') else s."Created By" is not null end 
        --condition with case (When backlink opportunity take only created by Josefine Spott) --is it same with filter (where)?
        ) sq

where impact is not null and stage_category in ('Closed Won', 'Closed Lost') -- and account_manager::numeric>0 

) y


;
-------------------------------------------


--
--select 
--case when s."Last Modified By" IN ('Josefine Spott') and "Close Date"::date = opportunity_due_date then 1 else 0 end as untouched
--from
--        (select 
--        s."Stage",  s."Created Date", s."Last Modified By", z.opportunity_lifespan,"Close Date"::date,
--        (s."Created Date"::DATE + z.opportunity_lifespan::interval  +  '1 day'::interval)::DATE as opportunity_due_date
--        FROM salesforce_fo.foodora_all_opportunities s
--        left join dwh_st.zhan_opportunity_metrics z
--        on z.opportunity_record_type = s."Opportunity Record Type" 
--        where s."Stage" in ('Closed Lost') and s."Last Modified By" IN ('Josefine Spott') 
--        ) cl_types 
--
--
--
--
--
--
--
------------
--
--SELECT * FROM  dwh_st.zhan_opportunity_metrics z
--select * FROM salesforce_fo.foodora_all_opportunities
--
----SELECT * FROM dwh_il.dim_countries WHERE managed_by_foodora is true
--
----SALESFORCE
--
--
--Select * from salesforce_fo.il_dim_contracts --account_name, account_owner, aaa, commission_percentage, exclusivity, facebook_rights
--SELECT * FROM salesforce_fo.il_dim_users -- country_name, city_name, user_id, user_name, salesforce_profile, salesforce_role, 
--SELECT * FROM salesforce_fo.il_dim_accounts a --SF accounts
--
--
----SELECT * FROM salesforce_fo.foodora_all_open_opportunities -- outdated
--SELECT * FROM salesforce_fo.foodora_all_opportunities --raw
--SELECT * FROM salesforce_fo.il_dim_opportunities o -- refactored 
--
--SELECT * FROM salesforce_fo.opportunity_marketing_score_bkp --what is it showing?
--
--
--select distinct "Stage" from salesforce_fo.foodora_all_opportunities 
--where "Opportunity Record Type" in ('Partner - Commission Renegotiation', 'Partner - Gain Exclusivity',
--'Partner - In-Restaurant Material', 'Partner - Improve Menu/CR', 'Partner - Automation Improvement',
--'Partner - Improve Cancellations', 'Partner - FB Advertiser Rights','Partner - Packaging',
--'Partner - Improvement Prep-Time/Vendor Delay','Partner - OM Backlink', 'Partner - OM Whitelabel', 
--'Partner - Pickup Activation', 'Partner - Improve Offline Hours','Partner - Customer Complaints',
--'Partner - Dish Level Photos','Partner - Price Mark-Up Removal')
----Agreed, Call Scheduled, Closed Lost, Closed Won, New, Meeting Completed, Contacted, Initial Meeting/Call Positive,
----Contact Established,  Initial Meeting/Call Negative,  Initial Meeting/Call Set Up, 
----Meeting Scheduled, Not Interested, Not Reachable, On Hold, Order Form Received, Order Form Sent, 
----Photo Shoot Completed, Photo Shoot Scheduled, Photos Edited, Photos Uploaded, Secondary Meeting/Call Complete, 
----Secondary Meeting/Call Set Up, Videoshoot Completed, Videoshoot Scheduled
--
--
--SELECT *,"Opportunity Record Type", "Stage", "Account ID",  "18 Char Opportunity ID", "18 Char Account ID", "Close Date" --"18 Char Opportunity ID" is the unique id ('00624000009LcBqAAK')
--FROM salesforce_fo.foodora_all_opportunities 
--where "Opportunity Record Type" in ('Partner - Commission Renegotiation', 'Partner - Gain Exclusivity',
--'Partner - In-Restaurant Material', 'Partner - Improve Menu/CR', 'Partner - Automation Improvement',
--'Partner - Improve Cancellations', 'Partner - FB Advertiser Rights','Partner - Packaging',
--'Partner - Improvement Prep-Time/Vendor Delay','Partner - OM Backlink', 'Partner - OM Whitelabel', 
--'Partner - Pickup Activation', 'Partner - Improve Offline Hours','Partner - Customer Complaints',
--'Partner - Dish Level Photos','Partner - Price Mark-Up Removal')
--
--
--
--select * from dwh_st.zhan_opportunity_metrics z
--select * from salesforce_fo.il_dim_users where salesforce_profile IN ('Account Manager',  'Head of AM') --dora users maybe raw tables for u and a?
--select * from salesforce_fo.il_dim_accounts --dora accounts (rdbms_id, account_id, vendor_code)
--SELECT * FROM salesforce_fo.il_dim_users -- rdbms_id, country_name, city_name, user_id, user_name, salesforce_profile, salesforce_role, 
--SELECT * FROM dwh_bl_fo.opportunity_all_scores o --impact, gmv_class, am_name, rdbms_id, vendor_code, vendor_name, gmv_class, report_month
--SELECT * FROM salesforce_fo.il_dim_opportunities dimo
--
--select distinct "Opportunity Record Type" FROM salesforce_fo.foodora_all_opportunities s
--select distinct opportunity_type from salesforce_fo.il_dim_opportunities
--
--FROM salesforce_fo.il_dim_contracts c --SF contract  
--LEFT JOIN salesforce_fo.il_dim_opportunities dimo --SF opportunities
--ON dimo.account_id=c.account_id 
--LEFT JOIN salesforce_fo.il_dim_users u --SF users
--ON c.account_owner = u.user_name AND c.rdbms_id = u.rdbms_id 
--LEFT JOIN salesforce_fo.il_dim_accounts a --SF accounts
--ON c.account_id = a.account_id            
--
--select * from salesforce_fo.il_dim_users where salesforce_profile IN ('Account Manager',  'Head of AM') --dora
--select * from salesforce.il_dim_users where salesforce_profile IN ('Account Manager',  'Head of AM') --panda
--select * from salesforce.il_dim_users u --I take the AMs from here
--select * from dwh_bl.opportunity_all_scores o-- no gmv_class so we take it from dwh_bl.vendor_gmv_class g for panda countries
--select * from dwh_bl.vendor_gmv_class g
--select * from salesforce_fo.il_dim_accounts a
--select * from dwh_il.dim_countries c
--select * from salesforce.il_dim_users u --I take the AMs from here
--
--
--SELECT rdbms_id, vendor_code, vendor_name, impact, gmv, impact/gmv as gap, 10-(impact/gmv) as final_score, 
--(10-(impact/gmv))::decimal*0.25 as commercial_impact, --commercial_impact
--(10-(impact/gmv))::decimal*0.20 as marketing_impact,
--(10-(impact/gmv))::decimal*0.05 as coversion_impact,
--(10-(impact/gmv))::decimal*0.05 as project_impact,
--(10-(impact/gmv))::decimal*0.05 as content_impact,
--(10-(impact/gmv))::decimal*0.40 as ops_impact
--FROM dwh_bl_fo.opportunity_all_scores 
--where report_month IN ('2017-09')
--
--12=2*(10-4)
--10-4=12/2
--4=10-(12/2)
--
--impact=12
--gmv=2
--10-4=gap
--4=final_score
--1=25% *4/100
--




