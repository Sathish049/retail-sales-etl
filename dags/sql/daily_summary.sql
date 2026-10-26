-- Rebuild the daily aggregate for ONE logical date.
-- {{ ds }} is a Jinja template variable: Airflow replaces it with the run's date (YYYY-MM-DD).
-- DELETE + INSERT makes this idempotent, so backfills and reruns never double count.

DELETE FROM agg_daily_sales WHERE sale_date = '{{ ds }}';

INSERT INTO agg_daily_sales
    (sale_date, store_id, category, txn_count, units_sold, net_revenue, avg_basket)
SELECT
    f.sale_date,
    f.store_id,
    p.category,
    COUNT(*)                        AS txn_count,
    SUM(f.quantity)                 AS units_sold,
    ROUND(SUM(f.net_amount), 2)     AS net_revenue,
    ROUND(AVG(f.net_amount), 2)     AS avg_basket
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
WHERE f.sale_date = '{{ ds }}'
GROUP BY f.sale_date, f.store_id, p.category;
