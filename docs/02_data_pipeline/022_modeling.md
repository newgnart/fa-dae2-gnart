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