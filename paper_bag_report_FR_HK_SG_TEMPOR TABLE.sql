

DROP TABLE IF EXISTS paper_bag_opportunity;                             --OPPORTUNITY DATA
CREATE TEMPORARY TABLE paper_bag_opportunity as( 
SELECT 
rdbms_id,
country_name,
city_name,
activated_date,
vendor_name,
vendor_code,
account_id,
product_date,
product_name,
auto_paperbag_supply,
paperbag_consumption,
paperbag_size,
quantity,
paper_bag_stock,
paper_bag_check_date,
order_of_arrival,
												--m,h,d											
ABS((EXTRACT (EPOCH FROM product_date::TIMESTAMP-(CURRENT_DATE-1)::TIMESTAMP)::DECIMAL(20,2)/60/60/24)) as days_numeric, -- e.g. 4
paper_bag_check_date::TIMESTAMP-(CURRENT_DATE-1)::TIMESTAMP as days_since_check, -- e.g. -3 days

product_date::date-(CURRENT_DATE-1)::date as days  --e.g. -4
FROM (
	SELECT 
	rdbms_id,
	country_name,
	city_name,
	activated_date,
	vendor_name,
	vendor_code,
	account_id,
	product_date,
	product_name,
	auto_paperbag_supply,
	paperbag_consumption,
	paperbag_size,
	quantity,
	paper_bag_stock,
	paper_bag_check_date,
	order_of_arrival 
	FROM (
		SELECT
		rdbms_id,
		country_name,
		city_name,
		activated_date,
		vendor_name,
		vendor_code,
		account_id,
		product_date,
		product_name,
		auto_paperbag_supply,
		paperbag_consumption,
		paperbag_size,
		quantity,
		paper_bag_stock,
		paper_bag_check_date,
		row_number() over (partition by rdbms_id, vendor_name, vendor_code  order by product_date DESC) as order_of_arrival
                ---- + "order_of_arrival" WHERE order_of_arrival=1 AND auto_paperbag_supply = 1
		                FROM (
				SELECT 
				opp.rdbms_id,
				lower( a.account_name) as vendor_name, 
				lower(vendor_code) as vendor_code, 
				a.country_name,
				a.account_id, 
				opp.product_date,
				opp.product_name, 
				opp.paper_bag_stock, --stock
				opp.paper_bag_check_date,  --check date
				a.city_name, 
				activated_date,
				a.auto_paperbag_supply,
				CASE WHEN paperbag_consumption IN ('Above Average (1.5 Bags per Order)') THEN 1.6 
				WHEN paperbag_consumption IN ('Normal (1 - 1.5 per Order)','Average (1.1 Bags per Order)') THEN 1.1
				WHEN paperbag_consumption ~* 'low' THEN 0.6 ELSE 1.1 
				END AS paperbag_consumption --consumpt
				
				,paperbag_size,
				SUM(opp.quantity) as quantity  
				 

                                
 				FROM salesforce_fo.il_dim_opportunities opp       -- SELECT * FROM salesforce_fo.il_dim_opportunities  where product_name IN ('Paper Bag') and account_id IN ('0012400000eES2XAAW')  --paper_bag_stock (0.0), paper_bag_check_date ('2018-01-31'), quantity (250) 
				LEFT JOIN  salesforce_fo.il_dim_contracts a       -- SELECT * FROM salesforce_fo.il_dim_contracts where account_id IN ('0012400000eES2XAAW')
				ON opp.account_id=a.account_id  
				WHERE product_name ='Paper Bag' AND  a.status IN ('Activated', 'Amended') 
				GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
				ORDER BY 1,2,3,4,5 DESC)d1  
		)d2
		WHERE  order_of_arrival=1 AND auto_paperbag_supply = 1 
)d3 


);                                                                   --  SELECT * FROM paper_bag_opportunity where account_id IN ('0012400000eES2XAAW')

--EXPLAIN ANALYSE SELECT * FROM paper_bag_opportunity;




DROP TABLE IF EXISTS all_vendors;                                       --ORDERS DATA
CREATE TEMPORARY TABLE all_vendors as( 


SELECT
               
v.address_line1,---Backend Adress 
v.address_line2,
x.billing_street,
x.billing_postcode, 
v.vendor_name as vendor_name_Be,
v. postcode,
x.vendor_code, 
x.rdbms_id,
x.country_name,
x.city_name,
x.activated_date,
x.account_name as vendor_name,
x.account_id,

CASE WHEN p.product_date::date> current_date THEN current_date-2 
else p.product_date::date 
end as product_date,

p.product_name,
x.auto_paperbag_supply,
p.paperbag_consumption,
p.paperbag_size,
p.quantity,
paper_bag_stock,
paper_bag_check_date,
p.order_of_arrival,
p.days_numeric, 
p.days,
status,

COALESCE(COUNT (o.order_id) FILTER (WHERE o.order_date::date >= CURRENT_DATE + p.days),0) as orders, -- o. in the last 4d. 
COALESCE(COUNT (o.order_id) FILTER (WHERE o.order_date::date >= CURRENT_DATE + p.days_since_check),0) as orders_check, -- o. in the last 3d.
COALESCE((COUNT ( o.order_id) FILTER (WHERE o.order_date::date >= CURRENT_DATE -'4 week'::interval)::decimal/30)::decimal, 0) as order_per_day, -- o. per d. in the last 4w.
COALESCE(COUNT ( o.order_id) FILTER (WHERE o.order_date::date >= CURRENT_DATE -'4 week'::interval),0) as orders_l4w -- o. in last 4w. 




FROM (select rdbms_id, vendor_id, order_id, status_id, order_date from dwh_il_fo.fct_orders -- select account_id from salesforce_fo.il_dim_contracts
        UNION ALL  
     select  rdbms_id, vendor_id, order_id, status_id, order_date from dwh_il.fct_orders where rdbms_id in (15,19) ) o    --dwh_il_fo.fct_orders o  --orders                                           

LEFT JOIN (select rdbms_id, vendor_code, address_line1, address_line2,vendor_name, postcode, vendor_id from dwh_il_fo.dim_vendors 
        UNION ALL  
        select rdbms_id, vendor_code, address_line1, address_line2,vendor_name, postcode, vendor_id from dwh_il.dim_vendors where rdbms_id in (15,19) ) v   --dwh_il_fo.dim_vendors v --vendors                                       
ON o.vendor_id = v.vendor_id AND o.rdbms_id = v.rdbms_id

LEFT JOIN salesforce_fo.il_dim_contracts x  --conracts
ON x.vendor_code = v.vendor_code AND x.rdbms_id = v.rdbms_id

LEFT JOIN paper_bag_opportunity  p --pb o
using (account_id)

LEFT JOIN (select rdbms_id,  status_id, valid_order from dwh_il_fo.meta_order_status 
        UNION ALL  
        select rdbms_id,  status_id, valid_order from dwh_il.meta_order_status where rdbms_id in (15,19) ) s  --dwh_il_fo.meta_order_status s --order status                             
ON o.rdbms_id=s.rdbms_id AND o.status_id=s.status_id


WHERE 
s.valid_order=1 

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25




);  -- SELECT * FROM all_vendors where account_id IN ('0012400000eES2XAAW')  
    --product_date ('2018-01-30'), paper_bag_stock (0.0), paper_bag_check_date IN ('2018-01-31'), orders (28), orders_check (18), orders_per_day (7.3)    





DROP TABLE IF EXISTS data;
CREATE TEMPORARY TABLE data as(                                                         --ASSIGNING AND ESTIMATION
 

SELECT
*,

CASE WHEN activated_date::date >=current_date-'7 day'::interval and activated_date::date<= current_date THEN 1 
else 0 
end as new_restaurant --new resto=activated date in the last 7 days

FROM (


	SELECT 
	address_line1,
	address_line2,
	billing_street,
	billing_postcode, 
	postcode ,
	vendor_code, 
	rdbms_id,
	country_name,
	city_name,
	activated_date,
	vendor_name,
	account_id,
	product_date,
	product_name,
	auto_paperbag_supply,
	paperbag_consumption,
	paperbag_size,
	quantity,
	order_of_arrival,
	days_numeric, 
	days,
	orders,
	orders_check, 
	order_per_day, 
	quantity - orders* paperbag_consumption AS remaining_paperbag_quantitiy,
	orders_l4w ,
	paper_bag_stock,
	paper_bag_check_date,
	paper_bag_stock- orders_check* paperbag_consumption AS remaining_paperbag_quantitiy_2,

	CASE 
		WHEN (vendor_code= 'yk2x' AND city_name= 'München') THEN 250 -----Holy Burger Haidhausen
		WHEN (Vendor_code= 'hk3g' AND city_name= 'München') THEN 250 -----Holy Burger Innenstadt
		
		WHEN  orders_l4w <250 THEN 1*250 --if in the l4w v. had less then 250 o, gets 250 pb 
		WHEN  orders_l4w >=250 AND  orders_l4w <500 THEN 2*250 --if in the l4w v had 250-500 o, gets 500 pb
		WHEN  orders_l4w >=500  THEN 3*250  -->500 o gets 750 pb
		END as pep_orders_tier,
	CASE 
		WHEN  orders_l4w <250 THEN 1
		WHEN  orders_l4w >=250 AND  orders_l4w <500 THEN 2
		WHEN  orders_l4w >=500  THEN 3
		END as pep_orders_tier2,
	CASE 
		WHEN country_name ~*'Netherlands' THEN (order_per_day*paperbag_consumption)*11 
		WHEN country_name ~*'Germany' THEN  (order_per_day*paperbag_consumption)*10  --7.3 *1.1 *11
		WHEN country_name ~*'France' THEN (order_per_day*paperbag_consumption) *10
		WHEN country_name ~*'Finland' THEN (order_per_day*paperbag_consumption) *12
		WHEN country_name ~*'Sweden' THEN (order_per_day*paperbag_consumption)*12
		WHEN country_name ~*'Italy' THEN (order_per_day*paperbag_consumption)*11
		WHEN country_name ~*'Austria' THEN (order_per_day*paperbag_consumption)*11
		WHEN country_name ~*'Norway' THEN (order_per_day*paperbag_consumption)*9      -- TO ADD THEM IN THE EESTIMATION 
		WHEN country_name ~*'Hong Kong' THEN (order_per_day*paperbag_consumption)*7
		WHEN country_name ~*'Singapore' THEN (order_per_day*paperbag_consumption)*7
		END  as estimation

	FROM all_vendors WHERE status IN ('Activated', 'Amended') 
	) a

);   -- SELECT * FROM data where account_id IN ('0012400000eES2XAAW')






DROP TABLE IF EXISTS final;
CREATE TEMPORARY TABLE final as(
 
SELECT 
paper_bag_stock,
CASE 
	WHEN paper_bag_check_date >= product_date  and product_date::DATE <  current_date - '1 week'::interval 
	THEN remaining_paperbag_quantitiy_2 -- when checked in the last week remining quantity 2
	ELSE remaining_paperbag_quantitiy 
	END  as remaining_paperbag_quantitiy, 

CASE 
    WHEN paper_bag_stock is null then remaining_paperbag_quantitiy
    WHEN paper_bag_stock is not null  THEN paper_bag_stock
    END as new_stock_calc,  --new stock calculation

--status,
CASE 
	WHEN d.vendor_code = 's3fy' AND d.city_name= 'Berlin' THEN 'Stadtbahnbogen 152' 
	WHEN address_line1 IS NULL THEN billing_street ELSE address_line1 
	END AS address_line1,
CASE 
	WHEN postcode IS NULL THEN billing_postcode ELSE postcode 
	END AS postcode,

d.vendor_name,
billing_postcode,
d.vendor_code, 
rdbms_id,
country_name,
d.city_name,
activated_date,
account_id,

CASE 
	WHEN paper_bag_check_date >= product_date THEN paper_bag_check_date ELSE product_date 
	END AS last_date, --last_date

product_name,
auto_paperbag_supply,
paperbag_consumption,
paperbag_size,
quantity as quantity_new,

CASE 
	WHEN paper_bag_check_date >= product_date THEN paper_bag_stock ELSE quantity 
	END AS quantity, --quantity

order_of_arrival,
days_numeric, 
days,
d.orders, 
order_per_day, 
orders_l4w ,
pep_orders_tier,
pep_orders_tier2,
estimation,
CASE WHEN order_per_day >=1 
        AND (product_date::DATE <  current_date - '1 week'::interval or product_date is null)
        AND (paper_bag_check_date::DATE <  current_date - '1 week'::interval or paper_bag_check_date is null)
        AND paperbag_consumption > 0.9 
        THEN 1 
        ELSE 0 end as binary_flag,

CASE 
	WHEN paper_bag_check_date >  product_date THEN orders_check ELSE d.orders 
	END AS orders_interval,

paper_bag_check_date



FROM data d 


);  -- SELECT * FROM final where account_id IN ('0012400000eES2XAAW')





DROP TABLE IF EXISTS open_op;
CREATE TEMPORARY TABLE open_op as(

SELECT  "Account ID" AS account_id_stage_new --, *  
FROM salesforce_fo.foodora_all_open_opportunities 
WHERE  "Opportunity Record Type"='Partner - Restaurant Marketing' 
--here we chack that before activating the account two SF milestone opportunities are "Closed Won", the marketing opp (stickers...) and hard-ware opportunities (tablets,...) 
);





DROP TABLE IF EXISTS address;
CREATE TEMPORARY TABLE address as(


SELECT 
rdbms_id, 
vendor_code,
"Shipping City",
"Shipping Phone",
"Shipping Street",
"Shipping Times Friday",
"Shipping Times Monday",
"Shipping Times Saturday",
"Shipping Times Thursday",
"Shipping Times Tuesday",
"Shipping Times Wednesday",
"Shipping Zip/Postal Code"

FROM salesforce_fo.il_dim_accounts s                 --  SELECT * FROM salesforce_fo.foodora_all_accounts where "City" in ('Singapore') 
LEFT JOIN salesforce_fo.foodora_all_accounts f 
ON f."18 Char Account ID"=s.account_id
--WHERE account_status='Active'  and rdbms_id=87 and vendor_code='n8qd'

);   --select * from address where  rdbms_id=87 and vendor_code='n8qd'




DROP TABLE IF EXISTS dwh_st.zhan_PB_report;
CREATE TABLE dwh_st.zhan_PB_report as(

SELECT 
f.*,
"Shipping City",
"Shipping Phone",
CASE WHEN f.vendor_code = 's3fy' AND f.city_name= 'Berlin' THEN 'Stadtbahnbogen 152' ELSE
"Shipping Street" end as 
"Shipping Street",
CASE WHEN "Shipping Times Friday" IS NULL THEN '-' ELSE "Shipping Times Friday" END AS "Shipping Times Friday",
CASE WHEN "Shipping Times Monday" IS NULL THEN '-' ELSE "Shipping Times Monday"END AS"Shipping Times Monday",
CASE WHEN "Shipping Times Saturday" IS NULL THEN '-' ELSE "Shipping Times Saturday"END AS"Shipping Times Saturday",
CASE WHEN "Shipping Times Thursday" IS NULL THEN '-' ELSE "Shipping Times Thursday"END AS"Shipping Times Thursday",
CASE WHEN "Shipping Times Tuesday" IS NULL THEN '-' ELSE "Shipping Times Tuesday"END AS"Shipping Times Tuesday",
CASE WHEN "Shipping Times Wednesday" IS NULL THEN '-' ELSE "Shipping Times Wednesday"END AS"Shipping Times Wednesday",
"Shipping Zip/Postal Code",

CASE
---
--WHEN rdbms_id=88 and vendor_code in ('az4y') THEN 'included'
WHEN f.rdbms_id IN (95) AND order_per_day>2 THEN 'included'
WHEN (estimation > remaining_paperbag_quantitiy )  OR 
( (activated_date::date >=current_date-'7 day'::interval and activated_date::date<= current_date AND order_per_day>=1 AND quantity <60) --new v and o<60
AND auto_paperbag_supply=1) AND paperbag_consumption is not null AND rdbm_id not in (95) THEN 'included'
WHEN order_per_day>1 AND quantity IS NULL AND auto_paperbag_supply=1 THEN 'included' --o>1 and quan is null
WHEN  auto_paperbag_supply =1 and (remaining_paperbag_quantitiy <=0) and orders_l4w>0 AND account_id_stage_new IS NULL THEN 'included' --'Restaurant Marketing'= is null
WHEN CITY_NAME='Mannheim' AND auto_paperbag_supply =1 and (remaining_paperbag_quantitiy <=0) AND account_id_stage_new IS NULL THEN 'included'  

else 'n.a.'
end as status, 


CASE WHEN f.rdbms_id IN (84) AND (f.rdbms_id IN (84) and f.vendor_code !='s5yr') THEN 0.05 ELSE 0 END  as "Sales Price", 
CASE WHEN f.rdbms_id IN (84) AND (f.rdbms_id IN (84) and f.vendor_code !='s5yr') THEN pep_orders_tier ELSE 0 END  as "Billing: Billing Fee Quantity"

--CASE WHEN f.rdbms_id IN (84) FILTER (WHERE f.vendor_code !='s5yr') THEN 0.05 ELSE 0 END  as "Sales Price", 
--CASE WHEN f.rdbms_id IN (84) FILTER (WHERE f.vendor_code !='s5yr') THEN pep_orders_tier ELSE 0 END  as "Billing: Billing Fee Quantity"


FROM final f
LEFT JOIN open_op o 
ON o.account_id_stage_new=f.account_id
LEFT JOIN address a 
USING(rdbms_id, vendor_code)

);  --SELECT * FROM dwh_st.zhan_PB_report where account_id IN ('0012400000eES2XAAW') 



----------------------------------------------------------------------------------------------

SELECT remaining_paperbag_quantitiy /*current_stock*/, pb.paper_bag_stock as stock_pb, zf.paper_bag_stock as stock_zf,
pb.paper_bag_check_date as check_date_pb, zf.paper_bag_check_date as check_date_zf, 
new_stock_calc, last_date, paperbag_consumption, 
quantity_new, quantity, days_numeric, orders, order_per_day, estimation,  status

zf.country_name, zf.account_id 

FROM dwh_st.zhan_PB_report pb

left join dwh_st.zhan_paperbag_feedback zf
using (account_id)

where account_id IN ('0012400000eES2XAAW')  --AND zf.paper_bag_check_date IN ('2018-01-02', '2018-01-22', '2018-01-31')






quantity - orders* paperbag_consumption AS remaining_paperbag_quantitiy
estimation 
paperbag_consumption
order_per_day 
REMAINING_Paperbag_quantitiy,


-- select * from dwh_st.zhan_paperbag_feedback (country_name, account_id, paper_bag_stock, paper_bag_check_date)


left join dwh_st.zhan_paperbag_feedback zf --(country_name, account_id, paper_bag_stock, paper_bag_check_date)
using (account_id)


