 SELECT client_id,
         SUM (
             CASE
                 WHEN product_type = 'MEUBLE'
                 THEN
                     quantiprod_price * prod_qtyty
                 ELSE
                     0
             END)    AS ventes_meuble,
         SUM (
             CASE
                 WHEN product_type = 'DECO' THEN prod_price * prod_qty
                 ELSE 0
             END)    AS ventes_deco
    FROM TRANSACTIONS T JOIN product_nomenclature p ON t.prop_id = p.product_id
   WHERE date BETWEEN TO_DATE ('2019/01/01', 'yyyy/mm/dd')
                  AND TO_DATE ('2019/12/31', 'yyyy/mm/dd')
GROUP BY client_id;