WITH top5Countries AS (SELECT country,
                              SUM(totalTransactionRevenue) AS countryTotalTransactionRevenue
                       FROM ecommerce_session_bigquery
                       GROUP BY country
                       ORDER BY countryTotalTransactionRevenue DESC
    LIMIT 5)
SELECT country,
       channelGrouping,
       SUM(totalTransactionRevenue) channelGroupTotalTransactionRevenue
FROM ecommerce_session_bigquery
WHERE country IN (SELECT country
                  FROM top5Countries)
GROUP BY country, channelGrouping
HAVING channelGroupTotalTransactionRevenue IS NOT NULL
ORDER BY country, channelGroupTotalTransactionRevenue DESC;