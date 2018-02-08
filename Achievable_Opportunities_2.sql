--CONTENT:
--flags
--old_opportunities (SF, v)
--future_negotiations
--renegotiations_l6m
--partner_competitors
--operational_opportunties
--data
--data_final

-- include as not achievable all commission renegotiation opportunities where contract start date is less than 3 months ago



SELECT * FROM (

WITH flags as (                                                                 --flags

 
WITH old_opportunities as (          --old_opportunities (tables: SF, vendors, contracts)

SELECT --why we need the MAX? 
vendor_code, 
chain_id, 
MAX(backlink_opp) OVER (PARTITION BY rdbms_id, chain_id) as backlink_opp, --backlink
MAX(fb_opp)  OVER (PARTITION BY rdbms_id, chain_id)as fb_opp,             --FB
MAX(commission_opp)  OVER (PARTITION BY rdbms_id, chain_id) as commission_opp, --commis 
MAX(excl_opp)  OVER (PARTITION BY rdbms_id, chain_id) as excl_opp,        --exclusivi
MAX(material_opp)  OVER (PARTITION BY rdbms_id, chain_id) as material_opp  --material
FROM (
        SELECT 
        rdbms_id, 
        vendor_code, 
        chain_id, 
        MAX(backlink_opp) as backlink_opp,  
        MAX(fb_opp) as fb_opp, 
        MAX(commission_opp) as commission_opp, 
        MAX(excl_opp) as excl_opp, 
        MAX(material_opp) as material_opp
                        
                FROM (

                        SELECT 
                        v.rdbms_id,
                        v.vendor_code, 
                        COALESCE(v.chain_id::text, v.vendor_code::text) as chain_id,
                        CASE WHEN "Opportunity Record Type" = 'Partner - OM Backlink' THEN 1 ELSE 0 END AS backlink_opp, 
                        CASE WHEN "Opportunity Record Type" = 'Partner - FB Advertiser Rights' THEN 1 ELSE 0 END AS fb_opp, 
                        CASE WHEN "Opportunity Record Type" = 'Partner - Commission Renegotiation' THEN 1 ELSE 0 END AS commission_opp, 
                        CASE WHEN "Opportunity Record Type" = 'Partner - Gain Exclusivity' THEN 1 ELSE 0 END AS excl_opp, 
                        CASE WHEN "Opportunity Record Type" = 'Partner - In-Restaurant Material' THEN 1 ELSE 0 END AS material_opp 
                        
                        FROM salesforce_fo.foodora_all_opportunities op --SF_fo_all_opp
                        LEFT JOIN salesforce_fo.il_dim_contracts c  --contracrs
                        ON c.account_id_old = op."Account ID"
                        LEFT JOIN dwh_il_fo.dim_vendors v --vendors
                        USING (vendor_code, rdbms_id)
                        
                        WHERE   --WHEN v.chain_code IS NOT NULL is not repeating in each case      
                        CASE    WHEN v.chain_code IS NOT NULL AND "Opportunity Record Type" IN ('Partner - Commission Renegotiation','Partner - Gain Exclusivity') 
                        THEN "Created Date">= current_date -'6month'::interval --6 months
                                WHEN  "Opportunity Record Type" IN ('Partner - Commission Renegotiation','Partner - Gain Exclusivity') 
                        THEN "Created Date">= current_date -'3month'::interval --3 months
                                WHEN  "Opportunity Record Type" IN ('Partner - FB Advertiser Rights','Partner - OM Backlink') 
                        THEN "Created Date">= current_date -'2month'::interval  --2
                                WHEN  "Opportunity Record Type" IN ('Partner - In-Restaurant Material') 
                        THEN "Created Date">= current_date -'1month'::interval END --1
                        --AND v.rdbms_id=86 and v.vendor_code='s9er'
                      )a
        GROUP BY 1,2,3
) b 
WHERE chain_id is not null

)

, future_negotiations as (                                                         --future_negotiations 

SELECT 
"Partner Backend Code" as vendor_code, 
"Country" as country_name, 
co.rdbms_id

FROM salesforce_fo.foodora_all_former_future_contracts c --SF_fo.contracts
LEFT JOIN dwh_il.dim_countries co --countries
ON c."Country" = co.common_name AND company_name IN ('Foodora') and backend_url is not null
WHERE "Contract Source" IN ('Upgrade','Downgrade') AND "Contract Start Date">= current_date AND "Status" IN ('Not Active')
) 



, 
renegotiations_l6m as (                                                                 --renegotiations last 6 months
SELECT * 
FROM salesforce_fo.il_dim_accounts acc --SF_fo.accounts
WHERE update_commission_l6m = 1.0
) 

, partner_competitors as (                                   --why we need?                           --partner_competitors
SELECT 
rdbms_id,
vendor_code,
competitors,
CASE WHEN 'Mjam' = ANY (competitors) 
        OR 'Lieferheld' = ANY (competitors) 
        OR 'Pizza.de' = ANY (competitors)
THEN 1 ELSE 0 END AS partner_competitor

FROM (
                
        SELECT 
        rdbms_id,
        "Account ID",
        "Account Name",
        "18 Char Account ID",
        "Country",
        "Partner Backend Code" AS vendor_code,
        string_to_array("Competitor Name",';') AS competitors

        FROM salesforce_fo.foodora_all_accounts --accounts
        LEFT JOIN salesforce_fo.il_dim_accounts --dim accounts
        ON "18 Char Account ID"=account_id
        WHERE "Competitor Name" IS NOT NULL AND "Account Record Type" IN ('Partner Account') AND "Account Status"='Active') a
)






, operational_opportunties AS(                                                             --operational_opportunties


SELECT 
v.rdbms_id,
v.vendor_code, 
CASE WHEN COUNT (vendor_code) FILTER (WHERE "Opportunity Record Type"='Partner - Improvement Prep-Time/Vendor Delay')>=1 THEN 1 ELSE 0 END as prep_time_opp,
CASE WHEN COUNT (vendor_code) FILTER (WHERE "Opportunity Record Type"='Partner - Automation Improvement')>=1 THEN 1 ELSE 0 END  as automation_opp,
CASE WHEN COUNT (vendor_code) FILTER (WHERE "Opportunity Record Type"='Partner - Improve Cancellations')>=1 THEN 1 ELSE 0 END  as cancel_opp

FROM salesforce_fo.foodora_all_opportunities op --SF_fo opp
LEFT JOIN salesforce_fo.il_dim_contracts c  --SF_fo contracts
ON c.account_id_old = op."Account ID"
LEFT JOIN dwh_il_fo.dim_vendors v --vendors
USING (vendor_code, rdbms_id)

WHERE "Opportunity Record Type" IN ('Partner - Improvement Prep-Time/Vendor Delay','Partner - Automation Improvement','Partner - Improve Cancellations') AND  
"Created Date">=  date_trunc('week',current_date -'1week'::interval)
group by 1,2


)

--here starts the creation of the table?
SELECT --we negotiate only with this vendors?
v.rdbms_id, 
v.vendor_code,
v.vendor_name,
 
CASE WHEN fn.vendor_code is not null or pn.vendor_code is not null THEN 1 ELSE 0 END as np_commission, --np?
CASE WHEN v.vendor_name like 'Vapiano %' or v.vendor_name like 'Dean & David &' or v.vendor_name like 'Nordsee%' or v.vendor_name like 'McDonald%' or 
v.vendor_name like 'KFC%' or v.vendor_name like '%Osteria%' or v.vendor_name like 'Ben & Jerr%' then 1 else backlink_opp 
END AS backlink_opp ,
CASE WHEN v.vendor_name like 'Vapiano %' or v.vendor_name like 'Dean & David &' or v.vendor_name like 'Nordsee%' or v.vendor_name like 'McDonald%' or 
v.vendor_name like 'KFC%' or v.vendor_name like '%Osteria%' or v.vendor_name like 'Ben & Jerr%' then 1 else fb_opp
END AS fb_opp, 
CASE WHEN v.vendor_name like 'Vapiano %' or v.vendor_name like 'Dean & David &' or v.vendor_name like 'Nordsee%' or v.vendor_name like 'McDonald%' or 
v.vendor_name like 'KFC%' or v.vendor_name like '%Osteria%' or v.vendor_name like 'Ben & Jerr%' then 1 else commission_opp
end as commission_opp, 
CASE WHEN v.vendor_name like 'Vapiano %' or v.vendor_name like 'Dean & David &' or v.vendor_name like 'Nordsee%' or v.vendor_name like 'McDonald%' or 
v.vendor_name like 'KFC%' or v.vendor_name like '%Osteria%' or v.vendor_name like 'Ben & Jerr%' then 1 else excl_opp
end as excl_opp,
CASE WHEN v.vendor_name like 'Vapiano %' or v.vendor_name like 'Dean & David &' or v.vendor_name like 'Nordsee%' or v.vendor_name like 'McDonald%' or 
v.vendor_name like 'KFC%' or v.vendor_name like '%Osteria%' or v.vendor_name like 'Ben & Jerr%' then 1 else material_opp
end as material_opp, 
partner_competitor, 
prep_time_opp,
automation_opp,
cancel_opp

FROM dwh_il_fo.dim_vendors v
LEFT JOIN future_negotiations fn --future negoti fn
USING (vendor_code, rdbms_id)
LEFT JOIN renegotiations_l6m pn --reneg l6m
USING (vendor_code, rdbms_id)
LEFT JOIN old_opportunities oo --old opp oo
USING (vendor_code, rdbms_id)
LEFT JOIN partner_competitors --partn compet
USING (vendor_code, rdbms_id)
LEFT JOIN operational_opportunties --opr opp
USING (vendor_code, rdbms_id)



)











, data as (                                                             --data
SELECT * FROM (       
            SELECT
            impact,
            ROW_NUMBER() OVER (PARTITION  BY am_name ORDER BY impact DESC) as impact_rank,
            
            -----////////COMMERCIAL////////
            0.3  *  0.60  * gmv * (10 -"revenue [score]")  as revenue_impact,
            0.3  *  0.1   * gmv * (10 -"price_mark_up [score]") as price_mark_up_impact,
            0.3  *  0.15   * gmv * (10 -"exclusivity [score]" ) as exclusivity_impact,
            ---- Pick up Score to be added next month, 15% as well
            -----////////MARKETING////////
            0.2  *  0.35  * gmv * (10 -"facebook [score]") as facebook_impact,
            0.2  *  0.35   * gmv * (10 -"backlink [score]") as backlink_impact, -----
            0.2  *  0.05   * gmv * (10 -"display [score]") as display_impact,
            0.2  *  0.05   * gmv * (10 -"door_sticker [score]") as door_sticker_impact,
            --0.2  *  0.1   * gmv * (10 -"voucher_cards [score]") as voucher_cards_impact, not importan for now
            -----////////CONVERSION////////
            0.05  *  0.20  * gmv * (10 -"rr [score]") as rr_impact,
            0.05  *  0.8   * gmv * (10 -"cr3 [score]")  as cr3_impact,
            -----////////CONTENT////////
            0.05  *  0.33  * gmv * (10 -"menucategories [score]")  as menucategories_impact,
            0.05  *  0.33  * gmv * (10 -"products [score]")  as products_impact,
            0.05  *  0.34  * gmv * (10 -"products_without_description [score]")  as products_without_description_impact,
            -----////////OPERATIONS////////
            0.4  *  0.00   * gmv * (10 -"packaging_quality [score]")   as packaging_quality_impact,
            0.4  *  0.10   * gmv * (10 -"automation [score]")  as automation_impact,
            0.4  *  0.15   * gmv * (10 -"cancellations [score]")  as cancellations_impact,
            0.4  *  0.00   * gmv * (10 -"processing_time [score]")  as processing_time_impact,
            0.0  *  0.50   * gmv * (10 -"vendor_delay [score]")  as vendor_delay_impact,
            0.4  *  0.10   * gmv * (10 - closing_hours_score)  as closing_hours_impact,
            0.4  *  0.15  * gmv * (10 - customer_complaints_score)  as customer_complaints_impact,

            s.* ,
            np_commission,
            backlink_opp  ,
            fb_opp, 
            commission_opp, 
            excl_opp, 
            material_opp,
            partner_competitor,
            prep_time_opp,
            automation_opp,
            cancel_opp
            
            FROM dwh_bl_fo.opportunity_all_scores s ---opp scores s
            LEFT JOIN salesforce_fo.il_dim_accounts a --SF accounts a
            ON s.rdbms_id=a.rdbms_id AND s.vendor_code=a.vendor_code
            LEFT JOIN flags f                         --flags f
            ON s.rdbms_id=f.rdbms_id AND s.vendor_code=f.vendor_code 
            WHERE report_month=  to_char(current_date -'1week'::interval,'iyyy-iw') and impact is not null AND account_status='Active' 
            --- right now I am looking at more than 40 orders per week, but this it not the best solution, we have to take GMV Class A or B from the gmv class table
            AND valid_orders>=40
            ) r 
) 






, data_final as (                                                               --data_final (repeats the same)

SELECT 
d.city_name,
d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN np_commission =1 or commission_opp=1 or "3 month flag"= 0 THEN 'Non Pursuable Opportunity' ELSE 'Pursuable Opportunity'  END AS pursuability, --pursuability
'Partner - Commission Renegotiation' as opportunity,                                                                                                    --opportunity
d.revenue_impact as value,                                                                                                                              --value
"revenue [score]" AS score,   --score
'commission' AS kpi,          --kpi
ROUND((d.commission_percentage*100)::integer, 0)::text ||'%' as kpi_value  --kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Commission last week was '||
ROUND((d.commission_percentage*100)::integer, 0)::text ||'%.'as description --description


FROM data d --data d
WHERE d.commission_percentage < 0.3 and commission_percentage >0.01



UNION ALL



SELECT 
d.city_name,

d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN excl_opp = 1 or "3 month flag"= 0 OR partner_competitor=1 THEN 'Non Pursuable Opportunity' ELSE 'Pursuable Opportunity'  END AS pursuability,
'Partner - Gain Exclusivity' as opportunity,
"exclusivity_impact" as value,
"exclusivity [score]" AS score,
'exclusivity' AS kpi,
exclusivity::text as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Exclusivity last week was '|| 
CASE WHEN exclusivity = 1 THEN '' ELSE ' not ' END ||'implemented.' as  description 
FROM data d 

UNION ALL

SELECT 
d.city_name,

d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN fb_opp = 1 or "3 month flag"= 0 THEN 'Non Pursuable Opportunity' ELSE 'Pursuable Opportunity'  END AS pursuability, --fb opport
'Partner - FB Advertiser Rights' as opportunity,
facebook_impact as value,
"facebook [score]" AS score,
'FB' AS kpi,
fb_advert_rights::text as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Fb last week was '|| CASE WHEN fb_advert_rights = 1 THEN '' ELSE ' not ' END ||'implemented properly.' as  description 
FROM data d 

UNION ALL

SELECT 
d.city_name,

d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN material_opp = 1 THEN 'Non Pursuable Opportunity' ELSE 'Pursuable Opportunity' END AS pursuability, --material opport
'Partner - In-Restaurant Material' as opportunity,
door_sticker_impact as value,
"door_sticker [score]" as score,
'Door stickers' AS kpi,
door_sticker::text as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Door stickers last week was not implemented properly.' as  description 
FROM data d 


--UNION 
--
--SELECT 
--d.city_name,
--
--d.country_name,
--d.rdbms_id, 
--am_name,
--d.vendor_code as backend_partner_code,
--d.vendor_name,
--CASE WHEN material_opp = 1 THEN 'Non Pursuable Opportunity' ELSE 'Pursuable Opportunity' END AS pursuability,
--'Partner - In-Restaurant Material' as opportunity,
--voucher_cards_impact as value,
--"voucher_cards [score]" as score,
--'Voucher cards' AS kpi,
--voucher_cards::text as kpi_value
--,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Voucher cards last week was not implemented properly.' as description 
--FROM data d 

UNION ALL

SELECT 
d.city_name,

d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN material_opp = 1 THEN 'Non Pursuable Opportunity' ELSE 'Pursuable Opportunity' END AS pursuability,
'Partner - In-Restaurant Material' as opportunity,
display_impact as value,
"display [score]" as score, 
'display' AS kpi,
display::text as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Display last week was not implemented properly.' as description 
FROM data d 

UNION ALL

SELECT 
d.city_name,

d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN backlink_opp=1  or "3 month flag"= 0 THEN 'Non Pursuable Opportunity' ELSE  'Pursuable Opportunity'  END AS pursuability,
'Partner - OM Backlink' as opportunity,
backlink_impact as value, 
"backlink [score]" as score,
'Backlink'AS kpi,
backlink::text as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Backlink is currently not implemented.' as description 
FROM data d 

UNION ALL

--SELECT 
--d.city_name,
--
--d.country_name,
--d.rdbms_id, 
--d.am_name,
--d.vendor_code as backend_partner_code,
--d.vendor_name,
--'Pursuable Opportunity'  AS pursuability,
--'Partner - Improve Menu/CR' as opportunity,
--d.cr3_impact as value,
--
--'Conversion rate' AS kpi,
--ROUND((cr3*100)::integer, 0)::text ||'%'as kpi_value
--,'' as description 
--FROM data d 


--UNION 

--SELECT 
--d.city_name,
--
--d.country_name,
--d.rdbms_id, 
--d.am_name,
--d.vendor_code as backend_partner_code,
--d.vendor_name,
--'Pursuable Opportunity'  AS pursuability,
--
--'Partner - Packaging' as opportunity,
--packaging_quality_impact as value
--,'Packaging' AS kpi,
--cr3::text as kpi_value
--,'' as description 
--FROM data d 
--
--
--UNION 


SELECT 
d.city_name,

d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN automation_opp=1 THEN 'Non Pursuable' ELSE 'Pursuable Opportunity'  END AS pursuability,  --automation opport
'Partner - Automation Improvement' as opportunity,
d.automation_impact as value,
"automation [score]" as score,
'Automation'::text AS kpi,
ROUND((automated_orders_perc*100)::integer, 0)::text ||'%'as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Automation last week was '||round(automated_orders_perc::numeric*100, 1)::text ||'%.' as description 
FROM data d 
WHERE automated_orders_perc<0.90

UNION ALL

SELECT 
d.city_name,
d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
CASE WHEN cancel_opp=1 THEN 'Non Pursuable' ELSE 'Pursuable Opportunity' end AS pursuability,  --cancelation opportu
'Partner - Improve Cancellations' as opportunity,
cancellations_impact as value,
"cancellations [score]" as score,
'Cancellations' AS kpi,
ROUND((Cancellations*100)::integer,0)::text ||'%' as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Cancellations last week was '||round(Cancellations::numeric*100, 1)::text ||'%.' as description 
FROM data d 
WHERE Cancellations>=0.05

UNION ALL

SELECT 
d.city_name,
d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,                                                                                --- preptime opport
CASE WHEN "vendor_delay [score]" >= 8  or prep_time_opp=1 THEN 'Non Pursuable Opportunity'  ELSE 'Pursuable Opportunity' END AS pursuability,

'Partner - Improvement Prep-Time/Vendor Delay' as opportunity,
vendor_delay_impact as value,
"vendor_delay [score]" as score,
'Vendor delay' AS kpi,
ROUND((vendor_delay*100)::integer,0)::text ||'%' as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Reliability rate las week was '||reliability_rate||'%, and expected waiting time of the rider at restaurant was '|| waiting_time_intercept||' minutes.' as description 
FROM data d 

UNION ALL

SELECT 
d.city_name,
d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
'Pursuable Opportunity'  AS pursuability,
'Partner - Improve Offline Hours' as opportunity,
closing_hours_impact as value,
closing_hours_score as score,
'Offline Hours' AS kpi,
ROUND((closing_hours*100)::integer,0)::text ||'%' as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Offline Hours last week was '||round(closing_hours::numeric*100, 1)::text ||'%.' as description 
FROM data d 
where closing_hours>=0.20

UNION ALL

SELECT 
d.city_name,
d.country_name,
d.rdbms_id, 
d.am_name,
d.vendor_code as backend_partner_code,
d.vendor_name,
'Pursuable Opportunity'  AS pursuability,

'Partner - Improve Customer Complaints' as opportunity,
customer_complaints_impact as value,
customer_complaints_score as score,
'Customer Complaints' AS kpi,
ROUND((customer_complaints*100)::integer,0)::text ||'%' as kpi_value
,'Required action: Please follow up with restaurant ' ||d.vendor_name||' (' ||d.vendor_code||'). Customer Complaints last week was '||round(customer_complaints::numeric*100, 1)::text ||'%.' as description 
FROM data d 
WHERE customer_complaints>=0.05
)
SELECT * FROM (
SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY am_name ORDER BY value desc) as rank,

CONCAT(backend_partner_code, rdbms_id, kpi) as id,
to_char(current_date -'1week'::interval,'iyyy-iw') AS report_week

FROM data_final 
) a 
 WHERE rank <=400 AND pursuability='Pursuable Opportunity' and value>=500
 --AND opportunity='Partner - Improve Customer Complaints' order by value desc



)a 
--where am_name IN ('Miriam Andersen') and opportunity IN ('Partner - Improve Offline Hours')
--)
