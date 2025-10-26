-- Active: 1758787034527@@localhost@5432@postgres
-- Active: 1758787034527@@localhost@5432@postgres
SELECT DISTINCT
    address
FROM (
        SELECT "from" AS address
        FROM demo.transfers
        UNION ALL
        SELECT "to" AS address
        FROM demo.transfers
    ) combined
WHERE
    address IS NOT NULL
ORDER BY address;