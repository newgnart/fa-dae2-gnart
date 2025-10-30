## Conceptual Model

### Entity Relationship Diagram
<iframe src="../../assets/erd01.html" frameborder="0" width="70%" height="250px"></iframe>

### Entity Descriptions and Relationships
**STABLECOIN**
- Represents each stablecoin type (crvUSD, GHO, frxUSD, etc.)

**SUPPLY**
- The circulating/total supply of a stablecoin at a point in time
- Relationship: Stablecoin EXISTS WITH Supply

**TRANSACTION**
- Transfer of stablecoins between addresses
- Relationship: Address SENDS/RECEIVES Stablecoin

**ADDRESS**
- Wallet or contract address that holds/transacts stablecoins
- Relationship: Address HOLDS Stablecoin




## Logical Model
*Platform-independent detailed design with normalization*

<img src="../../assets/erd01.svg" alt="Logical Model" width="70%" height="250px">

### Why dimensional model?
- Analyzing stablecoin transactions, transfers, and market behavior, suitable for OLAP queries (aggregations, time-series analysis)
- 3NF optimizes for OLTP (transactional systems) with normalized tables to prevent data anomalies during writes
- OBT can work for simple analytics, this project has multiple business processes (transfers, contract interactions, time-based trends) that would create a massively denormalized table with sparse columns and data duplication.
- Data Vault: Frequent schema changes
- Intuitive star schema:
```sql
SELECT 
    d.symbol,
    d.name,
    SUM(f.amount_normalized) as total_volume
FROM mart.fct_transfers f
JOIN mart.dim_stablecoin d ON f.stablecoin_key = d.stablecoin_key
WHERE d.is_current = true
GROUP BY d.symbol, d.name;
```



