-- Active: 1756173922021@@127.0.0.1@5432@postgres
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

DROP TABLE IF EXISTS raw.raw_stablecoins;

DROP SCHEMA IF EXISTS public_staging CASCADE;
