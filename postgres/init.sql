create schema if not exists streaming;
create schema if not exists staging;
create schema if not exists analytics;

create table if not exists streaming.fact_orders (
    event_id text primary key,
    event_time timestamptz not null,
    order_id text not null,
    customer_id text not null,
    product_id text not null,
    product_category text not null,
    quantity integer not null,
    unit_price numeric(10, 2) not null,
    total_amount numeric(12, 2) not null,
    payment_method text not null,
    country_code text not null,
    device_type text not null,
    stock_after_order integer not null,
    is_anomalous boolean not null default false,
    anomaly_reasons text[] not null default '{}',
    raw_payload jsonb not null,
    inserted_at timestamptz not null default now()
);

create table if not exists streaming.fact_order_anomalies (
    event_id text primary key,
    detected_at timestamptz not null,
    order_id text not null,
    severity text not null,
    reasons text[] not null,
    payload jsonb not null,
    inserted_at timestamptz not null default now()
);

create table if not exists staging.inventory_snapshot (
    snapshot_date date not null,
    product_id text not null,
    warehouse_code text not null,
    stock_on_hand integer not null,
    reserved_stock integer not null,
    reorder_threshold integer not null
);

create table if not exists staging.supplier_deliveries (
    delivery_date date not null,
    supplier_id text not null,
    product_id text not null,
    delivered_quantity integer not null,
    delivery_status text not null
);

create table if not exists staging.support_tickets (
    ticket_id text primary key,
    opened_at timestamptz not null,
    customer_id text not null,
    channel text not null,
    category text not null,
    priority text not null,
    status text not null
);

create table if not exists analytics.daily_inventory_health (
    snapshot_date date not null,
    product_id text not null,
    warehouse_code text not null,
    stock_on_hand integer not null,
    reserved_stock integer not null,
    available_stock integer not null,
    reorder_threshold integer not null,
    stock_status text not null
);

create table if not exists analytics.daily_support_kpis (
    opened_date date not null,
    channel text not null,
    total_tickets integer not null,
    urgent_tickets integer not null,
    open_tickets integer not null
);

create or replace view analytics.v_orders_summary as
select
    date_trunc('day', event_time) as order_day,
    product_category,
    payment_method,
    count(*) as total_orders,
    sum(total_amount) as gross_revenue,
    count(*) filter (where is_anomalous) as anomalous_orders
from streaming.fact_orders
group by 1, 2, 3;

create or replace view analytics.v_anomaly_monitoring as
select
    order_id,
    severity,
    reasons,
    detected_at
from streaming.fact_order_anomalies
order by detected_at desc;
