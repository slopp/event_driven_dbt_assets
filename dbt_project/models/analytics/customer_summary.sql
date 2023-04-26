SELECT 
    COUNT(*) AS count
FROM {{ source("raw_data", "customers")}}