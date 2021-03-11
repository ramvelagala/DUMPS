SELECT * FROM product_table WHERE price >= (SELECT price FROM product_table 
ORDER BY price DESC LIMIT 1 OFFSET (SELECT 0.1 * COUNT(*) FROM product_table));
