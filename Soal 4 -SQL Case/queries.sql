# Case 1
WITH top5Countries AS (
    SELECT country,
    SUM(totalTransactionRevenue) AS countryTotalTransactionRevenue
    FROM ecommerce_session_bigquery
    GROUP BY country
    ORDER BY countryTotalTransactionRevenue DESC
    LIMIT 5
)
SELECT
    country,
    channelGrouping,
    SUM(totalTransactionRevenue) channelGroupTotalTransactionRevenue
FROM ecommerce_session_bigquery
WHERE country IN (
    SELECT country FROM top5Countries
)
GROUP BY country, channelGrouping
HAVING channelGroupTotalTransactionRevenue IS NOT NULL
ORDER BY country, channelGroupTotalTransactionRevenue DESC;

# Case 2
WITH userMetrics AS (
    SELECT fullVisitorId,
           AVG(timeOnSite) avgTimeOnSite,
           AVG(pageviews) avgPageViews,
           AVG(sessionQualityDim) avgSessionQualityDim
    FROM ecommerce_session_bigquery
    GROUP BY fullVisitorId
)
SELECT ecommerce_session_bigquery.fullVisitorId, ecommerce_session_bigquery.country
FROM ecommerce_session_bigquery
INNER JOIN userMetrics ON ecommerce_session_bigquery.fullVisitorId = userMetrics.fullVisitorId
WHERE
    ecommerce_session_bigquery.timeOnSite > userMetrics.avgTimeOnSite AND
    ecommerce_session_bigquery.pageviews < userMetrics.avgPageViews;

# Case 3
# A
SELECT
    v2ProductName,
    SUM(totalTransactionRevenue) productTotalTransactionRevenue
FROM ecommerce_session_bigquery
GROUP BY v2ProductName
HAVING productTotalTransactionRevenue IS NOT NULL
ORDER BY productTotalTransactionRevenue DESC;