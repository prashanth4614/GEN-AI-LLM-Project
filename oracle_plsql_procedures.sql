-- Procedure to get monthly sales
CREATE OR REPLACE PROCEDURE GetMonthlySales(p_month IN NUMBER, p_year IN NUMBER, result OUT SYS_REFCURSOR)
IS
BEGIN
    OPEN result FOR
    SELECT TO_CHAR(sale_date, 'YYYY-MM') AS sale_month,
           SUM(total_amount) AS total_sales
    FROM SALES
    WHERE EXTRACT(MONTH FROM sale_date) = p_month
      AND EXTRACT(YEAR FROM sale_date) = p_year
    GROUP BY TO_CHAR(sale_date, 'YYYY-MM');
END GetMonthlySales;
/

-- Function to check reorder point for inventory
CREATE OR REPLACE FUNCTION NeedReorder(p_product_id IN NUMBER) RETURN BOOLEAN
IS
    qty NUMBER;
BEGIN
    SELECT quantity_in_stock INTO qty
    FROM INVENTORY
    WHERE product_id = p_product_id;
    IF qty < 100 THEN
       RETURN TRUE;
    ELSE
       RETURN FALSE;
    END IF;
END NeedReorder;
/

-- Sample business query: Get top 5 customers by total purchase
SELECT c.customer_name, SUM(s.total_amount) AS total_purchase
FROM SALES s
JOIN CUSTOMERS c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_purchase DESC
FETCH FIRST 5 ROWS ONLY;
/
