SELECT 
    location, 
    COUNT(*) AS count
FROM {{ source("raw_data", "orders")}}
GROUP BY 1