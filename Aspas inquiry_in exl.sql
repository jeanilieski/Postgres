--PANDA

select *, 
case when orders_first_6_month < 25 and current_date - activation_date > 180 then 1  end  as "6m_flag",
case when orders_first_year < 275 and current_date - activation_date > 365 then 1 end as "1y_flag",
case when ord_within_6m_aft_25_ord <250 and current_date - activation_date > 180 then 1 end as "6m_flag_aft_25"
from(
select co.common_name as country, v.vendor_code, v.vendor_name, v.activation_date, (current_date-v.activation_date)::int /12  as num_of_months_live, 
op.orders_first_month, op.orders_first_3_month, op.orders_first_6_month, op.orders_first_year,
op.ord_within_6m_aft_25_ord

from dwh_il.dim_vendors v                       --select max(activation_date), min(activation_date) from dwh_il.dim_vendors  where current_date - activation_date > 180
LEFT JOIN dwh_il.dim_countries co 
ON v.rdbms_id=co.rdbms_id
         
left join        
        (select o.rdbms_id, o.vendor_id,
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (yu.ord_25::date) AND (yu.ord_25::date + 180)) as ord_within_6m_aft_25_ord,
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 30)) as orders_first_month, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 90)) as orders_first_3_month, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 180)) as orders_first_6_month, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 365)) as orders_first_year 
        
        from dwh_il.fct_orders o
        left join dwh_il.dim_vendors v
        on o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id
        LEFT JOIN dwh_il.meta_order_status os --order status
        ON o.rdbms_id=os.rdbms_id AND o.status_id=os.status_id
        
        left join( select * from(
                        select rdbms_id, vendor_id, case when row_num=25 then order_date end as ord_25
                        from
                                (select rdbms_id, vendor_id, order_date, 
                                ROW_NUMBER () OVER (partition by rdbms_id, vendor_id ORDER BY order_date) as row_num  
                                from dwh_il.fct_orders
                                ) zv  --where vendor_id in ( 32, 37) and rdbms_id in (80)) zv
                        ) pa
            where ord_25 is not null) yu
        on yu.rdbms_id=v.rdbms_id AND yu.vendor_id=v.vendor_id        
        
        where os.valid_order=1 and v.rdbms_id in (7,12,15,16,17,18,19,20,22,27,42)
        group by 1,2) op
        
on op.rdbms_id=v.rdbms_id AND op.vendor_id=v.vendor_id
where v.vendor_active=1 
and v.rdbms_id in (7,12,15,16,17,18,19,20,22,27,42) )    sq
 

union all


--FODORA
--panda countries have wrong activation_date on SF so use the vendor table

select *, 
case when orders_first_6_month < 25 and (current_date - activation_date::date)::int > 180 then 1  end  as "6m_flag",
case when orders_first_year < 275 and (current_date - activation_date::date)::int > 365 then 1 end as "1y_flag",
case when ord_within_6m_aft_25_ord <250 and (current_date - activation_date::date)::int > 180 then 1 end as "6m_flag_aft_25"
from(

select co.common_name as country, v.vendor_code, v.vendor_name, z.activation_date, (current_date-z.activation_date::date)::int /12  as num_of_months_live, 
op.orders_first_month, op.orders_first_3_month, op.orders_first_6_month, op.orders_first_year,
op.ord_within_6m_aft_25_ord

from dwh_il_fo.dim_vendors v
LEFT JOIN
        (select z."Activated Date" as activation_date, z."18 Char Account ID",  z."Partner Backend Code", vi.rdbms_id, vi.vendor_code 
        from salesforce_fo.foodora_all_accounts  z 
        
        left join salesforce_fo.il_dim_accounts a  -- select * from salesforce.il_dim_accounts
        on a.account_id=z."18 Char Account ID"  
        
        left join dwh_il_fo.dim_vendors vi
        ON vi.vendor_code=a.vendor_code  and vi.rdbms_id=a.rdbms_id                                        
        
        where z."Account Status" in ('Active') and vi.rdbms_id in (80,81,88,86,87,84,137,85,83,95)) z
on v.rdbms_id=z.rdbms_id and v.vendor_code=z.vendor_code

LEFT JOIN dwh_il.dim_countries co 
ON v.rdbms_id=co.rdbms_id
         
left join        
        (SELECT  o.rdbms_id, o.vendor_id,
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (yu.ord_25::date) AND (yu.ord_25::date + 180)) as ord_within_6m_aft_25_ord, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 30)) as orders_first_month, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 90)) as orders_first_3_month, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 180)) as orders_first_6_month, 
        COUNT(DISTINCT o.order_id) FILTER (WHERE  o.order_date::date BETWEEN (v.activation_date) AND (v.activation_date + 365)) as orders_first_year 
        
        from dwh_il_fo.fct_orders o --orders
        left join
               (select z."Activated Date"::date as activation_date, z."18 Char Account ID",  z."Partner Backend Code", ve.rdbms_id, ve.vendor_id
                from salesforce_fo.foodora_all_accounts  z 
                
                left join salesforce_fo.il_dim_accounts a                                       -- select * from salesforce.il_dim_accounts
                on a.account_id=z."18 Char Account ID"  
                
                left join dwh_il_fo.dim_vendors ve
                ON ve.vendor_code=a.vendor_code  and ve.rdbms_id=a.rdbms_id                                        
                
                where z."Account Status" in ('Active') and ve.rdbms_id in (80,81,88,86,87,84,137,85,83,95)) v -- accounts
                
        on o.rdbms_id=v.rdbms_id AND o.vendor_id=v.vendor_id
        
        LEFT JOIN dwh_il_fo.meta_order_status os --order status   
        ON o.rdbms_id=os.rdbms_id AND o.status_id=os.status_id
        
        left join( select * from(
                        select rdbms_id, vendor_id, case when row_num=25 then order_date end as ord_25
                        from
                                (select rdbms_id, vendor_id, order_date, 
                                ROW_NUMBER () OVER (partition by rdbms_id, vendor_id ORDER BY order_date) as row_num  
                                from dwh_il_fo.fct_orders
                                ) zv  --where vendor_id in ( 32, 37) and rdbms_id in (80)) zv
                        ) pa 
            where ord_25 is not null) yu --the 25th order
        on yu.rdbms_id=v.rdbms_id AND yu.vendor_id=v.vendor_id   
        
        where os.valid_order=1
        group by 1,2) op
        
on op.rdbms_id=v.rdbms_id AND op.vendor_id=v.vendor_id    
            
where v.vendor_active=1 
and v.rdbms_id in (80,81,88,86,87,84,137,85,83,95) )    sq
 
----------------------------------------------------------------------