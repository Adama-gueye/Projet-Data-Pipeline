select * from analytics.v_orders_summary order by order_day desc, gross_revenue desc;

select * from analytics.v_anomaly_monitoring limit 20;

select * from analytics.daily_inventory_health where stock_status <> 'healthy';

select * from analytics.daily_support_kpis order by opened_date desc, urgent_tickets desc;
