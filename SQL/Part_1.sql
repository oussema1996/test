SELECT date, SUM (prod_price * prod_qty) AS ventes
    FROM TRANSACTIONS
   WHERE date BETWEEN TO_DATE ('2019/01/01', 'yyyy/mm/dd')
                  AND TO_DATE ('2019/12/31', 'yyyy/mm/dd')
GROUP BY date;