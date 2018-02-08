


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

