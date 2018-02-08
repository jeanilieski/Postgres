--select * from salesforce.il_dim_accounts where account_status NOT IN ('Active') --SF accounts
--        ON s."18 Char Account ID" = a.account_id 
--
--
--
--select * from salesforce_fo.il_dim_accounts --rdbms_id, country_name, vendor_code --(Bangladesh, Brunei, Bulgaria, Philippines, Romania and Taiwan are part of foodora SF now)
--select * from salesforce.il_dim_accounts --country_name, vendor_code --(MY, SG,TH,HK)  --PK is out
--
--FROM (select account_status, account_id, country_name, vendor_code from salesforce_fo.il_dim_accounts  where account_status NOT IN ('Active')
--      UNION ALL  
--      select account_status, account_id, country_name, vendor_code from salesforce.il_dim_accounts  where account_status NOT IN ('Active')) sa
--
--where rdbms_id IN (16, 15, 17, 19) 
--where account_status NOT IN ('Active')



--UNION ALL


--FOODORA
select *


from ( 
         
WITH restaurant_schedules_raw AS(--exclude between 11 pm and 9 am

--For Late Night Delivery LND) accounts, their regular account extends to 12am, and the LND accounts start from 12.01am essentially and operate up to their own dine-in hours. 
--LND orders are not yet at a level where they can justify the increased cost of operating later than they currently do

SELECT 
rdbms_id, 
vendor_id, 
weekday, 
start_hour,
case when stop_hour='00:00:00'::time then stop_hour-'1minute'::interval else stop_hour end as stop_hour
FROM dwh_il_fo.fct_vendor_schedules --fct_vendor_schedules (all the data on weird hours comes from this table)         --SELECT * FROM dwh_il_fo.fct_vendor_schedules   --select * from dwh_il_fo.dim_vendors
where type='delivering' and all_day IN (0) -- until here only deducting "00;00;00" for 1 minute (not to have negative cases)
)

, restaurant_schedules_final AS(
SELECT 
rdbms_id, 
vendor_id, 
weekday, 
CASE WHEN SUM(opening_hour)<'03:00:00'::INTERVAL THEN 1 else 0 end as less_than_3_open_hours, --finding those with less then 3 hours
SUM(opening_hour) as total_open_hours 
FROM (
        select
        rdbms_id, 
        vendor_id, 
        weekday, 
        start_hour,
        stop_hour, 
        stop_hour-start_hour as opening_hour --calculate opening hours
        -- ,case when (start_hour > '09:00:00'::time and  stop_hour < '23:00:00'::time) THEN stop_hour::interval-start_hour::interval end as opening_hours
        from restaurant_schedules_raw
        --where start_hour > '09:00:00'::time and  stop_hour < '23:00:00'::time
        ) a  

group by 1,2,3 
order by 1,2,3
)

--SELECT '00:02:00'::time, '00:02:00'::interval
--WHERE localminute BETWEEN '2013-11-01'::date AND '2014-12-01'::date 
--AND (extract('hour' from localminute) >= 22 OR extract('hour' from localminute) < 6)


, restaurants_schedules_flag AS(--flagging the vendors having at least one day per week less than 3h opened
select 
rdbms_id, 
vendor_id, 
MAX(less_than_3_open_hours) as less_than_3_open_hours_one_day --selects the "1"s from "0" or "1"
FROM restaurant_schedules_final

group by 1,2

)


, salesforce_accounts AS(

select rdbms_id, account_status, account_id, country_name, vendor_code 
from salesforce_fo.il_dim_accounts -- all fodora coun and migrated panda

    

)


, menus AS(
select rdbms_id, vendor_id, sum(active) as active_menu
from  merge_layer_rdbms_fo.menus --all the data on "not having active menu"
group by 1,2 


) 

, inactive_city AS(

select 
co.common_name,
c.city_name,
c.active AS city_active,
c.city_id,
v.vendor_id,
v.vendor_code,
v.vendor_active,
v.rdbms_id,
case when (c.active=0  or c.active is null)  and v.vendor_deleted=0 and vendor_active =1 and v.vendor_testing=0 then 1 else 0 end as inactive

from dwh_il_fo.dim_vendors v --city_id
join dwh_il.dim_countries co using (rdbms_id) --select * from dwh_il.dim_countries
left join dwh_il_fo.dim_city c using (rdbms_id, city_id) --select * from dwh_il.dim_city
where co.managed_by_foodora IS TRUE
--and c.active=0 and v.vendor_deleted=0 and v.vendor_testing=0


)



,valid_orders AS(
SELECT 
o.rdbms_id, 
o.vendor_id, 
Count(distinct o.order_code_google) filter (where s.valid_order=1) as valid_orders --unique order code for a valid order (not testing order)

FROM dwh_il_fo.fct_orders o -- code of the status
Left Join dwh_il_fo.meta_order_status s USING (rdbms_id, status_id) -- desc of the status
left join dwh_il_fo.dim_vendors v USING (rdbms_id, vendor_id) --vendor data

WHERE v.vendor_testing=1  -- in the vendor table marked as testing although in orders table is a valid order                     
and o.order_date::date >= current_date-30 --had order in the last 30 days
GROUP BY 1,2
Having (Count(distinct o.order_code_google) filter (where s.valid_order=1)) >0


) 




select 
v.rdbms_id AS "ID", 
c.common_name as "Country", 
ic.city_name as "City",
v.vendor_name as "Vendor Name", 
v.vendor_id as "Vendor ID", 
v.vendor_code as "Vendor Code", 

case when (v.vendor_active=1 and v.vendor_deleted=0) then less_than_3_open_hours_one_day else 0 end as "Weird Opening Hours",

case when (vo.valid_orders>0 and  v.vendor_deleted=0) then 1 else 0 end as "False Testing", --valid order treated as test 
case when (v.vendor_name ~*'Test' and  v.vendor_testing != 1) and  v.vendor_deleted=0 then 1 else 0 end as "Testing Not Flagged", -- test order treated as valid 

--case when (v.vendor_active=1 and  v.vendor_deleted=1) then 1 else 0 end as "Active and Deleted",
--'https://' || c.backend_url || '/vendors/index/edit/id/' || v.vendor_id as "URL of Active and Deleted",

case when (v.automatic_calls_enabled !=1 or v.automatic_calls_enabled  is null) and  v.vendor_deleted=0 then 1 else 0 end as "Autocall Not Enabled",

case when(v.accept_delivery is null or v.accept_delivery !=1) and  v.vendor_deleted=0 then 1 else 0 end as "Not Allowing Delivery",

case when v.rdbms_id=137 AND mv.vat_included=1 and  v.vendor_deleted=0 THEN 1 --except for Canada, VAT should be included
     when(v.rdbms_id !=137 and (mv.vat_included is null or mv.vat_included !=1) and  v.vendor_deleted=0) then 1
     else 0 
     end  as "Having VAT Issue",
--select * from dwh_il_fo.dim_Vendors where vendor_code='s9bw' and rdbms_id=88
--select * from dwh_il_fo.dim_Vendors where vendor_code='s3rl' --and rdbms_id=12
--select * from merge_layer_rdbms.vendors where vendor_code='s3rl' and rdbms_id=12
 
--case when (l.active != 1 and v.vendor_deleted=0) then 1 else 0 end as not_activated_location,
case when(v.accept_discount is null or v.accept_discount !=1) and v.vendor_deleted=0 then 1 else 0 end as "Not Accepting Discount",
case when(v.accept_voucher is null or v.accept_voucher !=1) and v.vendor_deleted=0 then 1 else 0 end as "Not Accepting Voucher",
case when (mv.automatic_calls_delay is null or mv.automatic_calls_delay not in (1,2)) and v.vendor_deleted=0 then 1 else 0 end as "Automatic Calls Delay Error", --there are 0,1,2,3,4 
--case when (mv.offline_calls_disabled !=0 and v.vendor_deleted=0) then 1 else 0 end as "Offline Calls Disabled",
case WHEN (mv.automatic_calls_phone = '' OR mv.automatic_calls_phone IS NULL OR (length(TRIM(mv.automatic_calls_phone)) <8)) and v.vendor_deleted=0 then 1 else 0 end as "Improper Telephone Number",
case when mu.active_menu<1 and v.vendor_deleted=0 then 1 else 0 end as "Not Having at least One Active Menu",
--case when dev.VBE=1 and dev.dispatcher=1 and v.vendor_deleted=0 then 0 else 1 end as incorrect_dispacher_configuration
case when (v.online_payment is null or v.online_payment !=1) and v.vendor_deleted=0 then 1 else 0 end as "Not Accepting Online Payment",
ic.inactive as "Vendor Assigned to Inactive City",
case when  account_status NOT IN ('Active', 'Qualified') and v.vendor_deleted=0 then 1 else 0 end as "Salesforce Inactive"

 

from dwh_il_fo.dim_Vendors v 
left join valid_orders vo       
on vo.rdbms_id=v.rdbms_id and vo.vendor_id=v.vendor_id

left join menus mu 
on v.rdbms_id=mu.rdbms_id and v.vendor_id=mu.vendor_id       

            
left join merge_layer_rdbms_fo.vendors mv 
on v.rdbms_id=mv.rdbms_id and v.vendor_id=mv.id                                                           

left join restaurants_schedules_flag rf
on v.rdbms_id=rf.rdbms_id AND v.vendor_id=rf.vendor_id

left join dwh_il.dim_countries c  
on v.rdbms_id=c.rdbms_id

left join salesforce_accounts sa
on sa.rdbms_id=v.rdbms_id and sa.vendor_code=v.vendor_code


left join inactive_city ic   --using (rdbms_id, city_id)
on ic.rdbms_id=v.rdbms_id and v.vendor_code=ic.vendor_code

where v.vendor_active=1 

) foodora

where ("Weird Opening Hours"+"False Testing"+"Testing Not Flagged"+"Autocall Not Enabled"+"Not Allowing Delivery"+"Having VAT Issue"--+"Active and Deleted"
+"Not Accepting Discount"+"Not Accepting Voucher"+"Automatic Calls Delay Error"
+"Improper Telephone Number"+"Not Having at least One Active Menu" --+"Offline Calls Disabled"
+"Not Accepting Online Payment"+"Vendor Assigned to Inactive City"+"Salesforce Inactive">0) --+incorrect_dispacher_configuration>0)







UNION ALL





--PANDA

--for vendor_active=1 and rdbms_id IN (7,12,15,16,17,18,19,20,22,27,42) 



select *


from ( --exclude between 11 pm and 9 am

WITH restaurant_schedules_raw AS( 

--For Late Night Delivery LND) accounts, their regular account extends to 12am, and the LND accounts start from 12.01am essentially 
--and operate up to their own dine-in hours. 
--LND orders are not yet at a level where they can justify the increased cost of operating later than they currently do.

SELECT 
rdbms_id, 
vendor_id, 
weekday, 
start_hour,
case when stop_hour='00:00:00'::time then stop_hour-'1minute'::interval else stop_hour end as stop_hour
FROM dwh_il.fct_vendor_schedules 
where type='delivering' and rdbms_id IN (7,12,15,16,17,18,19,20,22,27,42) and all_day IN (0) -- until here only deducting "00;00;00" for 1 minute (not to have negative cases)
)

, restaurant_schedules_final AS(
SELECT 
rdbms_id, 
vendor_id, 
weekday, 
CASE WHEN SUM(opening_hour)<'03:00:00'::INTERVAL THEN 1 else 0 end as less_than_3_open_hours, --finding those with less then 3 hours
SUM(opening_hour) as total_open_hours 
FROM (
        select
        rdbms_id, 
        vendor_id, 
        weekday, 
        start_hour,
        stop_hour, 
        stop_hour-start_hour as opening_hour --calculate opening hours
        --case when start_hour > '09:00:00' and  stop_hour < '23:00:00' THEN stop_hour-start_hour as   opening_hour1             
        
        from restaurant_schedules_raw
        --where start_hour > '09:00:00'::time and  stop_hour < '23:00:00'::time
        ) a  

group by 1,2,3 order by 1,2,3
)

, restaurants_schedules_flag AS(--flagging the vendors having at least one day per week less than 3h opened
Select 
rdbms_id, 
vendor_id, 
MAX(less_than_3_open_hours) as less_than_3_open_hours_one_day

FROM restaurant_schedules_final

group by 1,2


)


, salesforce_accounts AS(

select rdbms_id, account_status, account_id, country_name, vendor_code 
from salesforce_fo.il_dim_accounts -- all fodora coun and migrated panda

      
)

--select distinct country_name from salesforce_fo.il_dim_accounts --rdbms_id, country_name, vendor_code --(Bangladesh, Brunei, Bulgaria, Philippines, Romania and Taiwan are part of foodora SF now)
--select distinct country_name from salesforce.il_dim_accounts --country_name, vendor_code --(MY, SG,TH,HK)  --PK is out
--
--case when  filter (where country_name  IN Bangladesh, Brunei, Bulgaria, Philippines, Romania and Taiwan



, menus AS(

select rdbms_id, vendor_id, sum(active) as active_menu
from  merge_layer_rdbms.menus --all the data on "not having active menu"
group by 1,2 




)  


, inactive_city AS(

select 
co.common_name,
c.city_name,
c.active AS city_active,
c.city_id,
v.vendor_id,
v.vendor_code,
v.vendor_active,
v.rdbms_id,
case when (c.active=0  or c.active is null) and v.vendor_deleted=0 and vendor_active =1 and v.vendor_testing=0 then 1 else 0 end as inactive

from dwh_il.dim_vendors v --city_id
join dwh_il.dim_countries co using (rdbms_id) --select * from dwh_il.dim_countries
left join dwh_il.dim_city c using (rdbms_id, city_id) --select * from dwh_il.dim_city --rdb, city id
where co.managed_by_foodora IS TRUE
--and c.active=0 and v.vendor_deleted=0 and v.vendor_testing=0


)



, valid_orders AS(
SELECT 
o.rdbms_id, 
o.vendor_id, 
Count(distinct o.order_code_google) filter (where s.valid_order=1) as valid_orders --unique order code for a valid order (not testing order)

FROM dwh_il.fct_orders o -- code of the status
Left Join dwh_il.meta_order_status s USING (rdbms_id, status_id) -- desc of the status
left join dwh_il.dim_vendors v USING (rdbms_id, vendor_id)

WHERE v.vendor_testing=1 AND o.rdbms_id in (7,12,15,16,17,18,19,20,22,24,27,42) -- in the vendor table marked as testing although in orders table is a valid order                     
and o.order_date::date >= current_date-30 --had order in the last 30 days
GROUP BY 1,2
Having (Count(distinct o.order_code_google) filter (where s.valid_order=1)) >0

) 





select 
v.rdbms_id AS "ID", 
c.common_name as "Country", 
ic.city_name as "City",
v.vendor_name as "Vendor Name", 
v.vendor_id as "Vendor ID", 
v.vendor_code as "Vendor Code", 


case when (v.vendor_active=1 and v.vendor_deleted=0) then less_than_3_open_hours_one_day else 0 end as "Weird Opening Hours",

case when (vo.valid_orders>0 and  v.vendor_deleted=0) then 1 else 0 end as "False Testing", --valid order treated as test
case when (v.vendor_name ~*'Test' and  v.vendor_testing != 1) and  v.vendor_deleted=0 then 1 else 0 end as "Testing Not Flagged", -- test order treated as valid 

--case when (v.vendor_active=1 and  v.vendor_deleted=1) then 1 else 0 end as "Active and Deleted",
--'https://' || c.backend_url || '/vendors/index/edit/id/' || v.vendor_id as "URL of Active and Deleted",

case when (v.automatic_calls_enabled !=1 or v.automatic_calls_enabled  is null) and v.vendor_deleted=0 then 1 else 0 end as "Autocall Not Enabled",

case when (mv.accept_delivery is null or mv.accept_delivery !=1) and  v.vendor_deleted=0 then 1 else 0 end as "Not Allowing Delivery",

case when mv.vat_included is null or mv.vat_included !=1 and  v.vendor_deleted=0 then 1 --except for Canada, VAT should be included
     else 0 
     end  as "Having VAT Issue", 
     
case when(v.accept_discount is null or v.accept_discount !=1) and v.vendor_deleted=0 then 1 else 0 end as "Not Accepting Discount",
case when(v.accept_voucher is null or v.accept_voucher !=1) and v.vendor_deleted=0 then 1 else 0 end as "Not Accepting Voucher",
case when (mv.automatic_calls_delay is null or mv.automatic_calls_delay not in (1,2)) and v.vendor_deleted=0 then 1 else 0 end as "Automatic Calls Delay Error", --there are 0,1,2,3,4 
--case when (mv.offline_calls_disabled !=0 and v.vendor_deleted=0) then 1 else 0 end as "Offline Calls Disabled",
case WHEN (mv.automatic_calls_phone = '' OR mv.automatic_calls_phone IS NULL OR (length(TRIM(mv.automatic_calls_phone)) <8)) and v.vendor_deleted=0 then 1 else 0 end as "Improper Telephone Number",
case when mu.active_menu<1 and v.vendor_deleted=0 then 1 else 0 end as "Not Having at least One Active Menu",

case when (v.online_payment is null or v.online_payment !=1) and v.vendor_deleted=0 then 1 else 0 end as "Not Accepting Online Payment",
ic.inactive as "Vendor Assigned to Inactive City",
case when  account_status NOT IN ('Active', 'Qualified') and v.vendor_deleted=0 then 1 else 0 end as "Salesforce Inactive"


from dwh_il.dim_Vendors v
 
left join valid_orders vo
on vo.rdbms_id=v.rdbms_id and vo.vendor_id=v.vendor_id

left join menus mu
on v.rdbms_id=mu.rdbms_id and v.vendor_id=mu.vendor_id
        
        
left join merge_layer_rdbms.vendors mv 
on v.rdbms_id=mv.rdbms_id and v.vendor_id=mv.id  

left join restaurants_schedules_flag rf
on v.rdbms_id=rf.rdbms_id AND v.vendor_id=rf.vendor_id

left join dwh_il.dim_countries c  -- add counntry column  --select * from dwh_il.dim_countries  rdb, 
on v.rdbms_id=c.rdbms_id

left join salesforce_accounts sa
on sa.rdbms_id=v.rdbms_id and sa.vendor_code=v.vendor_code

left join inactive_city ic   --using (rdbms_id, city_id)
on ic.rdbms_id=v.rdbms_id and v.vendor_code=ic.vendor_code
--on ic.rdbms_id=v.rdbms_id and ic.city_id=v.city_id 

where v.vendor_active=1 and v.rdbms_id in (7,12,15,16,17,18,19,20,22,27,42) 

) panda


where ("Weird Opening Hours"+"False Testing"+"Testing Not Flagged"+"Autocall Not Enabled"+"Not Allowing Delivery"+"Having VAT Issue"--+"Active and Deleted"
+"Not Accepting Discount"+"Not Accepting Voucher"+"Automatic Calls Delay Error"
+"Improper Telephone Number"+"Not Having at least One Active Menu" --+"Offline Calls Disabled"
+"Not Accepting Online Payment"+"Vendor Assigned to Inactive City"+"Salesforce Inactive">0)  --+incorrect_dispacher_configuration>0)
