WITH userMetrics AS (SELECT fullVisitorId,
                            AVG(timeOnSite)        avgTimeOnSite,
                            AVG(pageviews)         avgPageViews,
                            AVG(sessionQualityDim) avgSessionQualityDim
                     FROM ecommerce_session_bigquery
                     GROUP BY fullVisitorId)
SELECT ecommerce_session_bigquery.fullVisitorId, ecommerce_session_bigquery.country
FROM ecommerce_session_bigquery
         INNER JOIN userMetrics ON ecommerce_session_bigquery.fullVisitorId = userMetrics.fullVisitorId
WHERE ecommerce_session_bigquery.timeOnSite > userMetrics.avgTimeOnSite
  AND ecommerce_session_bigquery.pageviews < userMetrics.avgPageViews;