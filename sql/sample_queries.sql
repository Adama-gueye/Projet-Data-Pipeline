-- ============================================
-- E-commerce Analytics - Sample SQL Queries
-- ============================================

-- 1. Low Stock Products
SELECT 
    product_id,
    product_name,
    quantity_in_stock,
    reorder_level,
    warehouse_location
FROM daily_inventory
WHERE quantity_in_stock < reorder_level
ORDER BY quantity_in_stock ASC;

-- 2. Supplier Delivery Performance
SELECT 
    supplier_name,
    COUNT(*) as total_deliveries,
    AVG(delivery_days) as avg_delivery_days,
    MIN(delivery_days) as fastest,
    MAX(delivery_days) as slowest
FROM supplier_deliveries
GROUP BY supplier_name
ORDER BY avg_delivery_days ASC;

-- 3. Support Tickets Summary
SELECT 
    priority,
    status,
    COUNT(*) as ticket_count
FROM support_tickets
GROUP BY priority, status
ORDER BY 
    CASE priority 
        WHEN 'high' THEN 1 
        WHEN 'medium' THEN 2 
        ELSE 3 
    END;
