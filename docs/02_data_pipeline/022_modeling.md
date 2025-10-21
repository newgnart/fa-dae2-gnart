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

### Dimension Tables (Type 2 SCD where needed)

**DIM_STABLECOIN**
```
PK: stablecoin_key (surrogate)
NK: stablecoin_code (business key)
---
stablecoin_name
stablecoin_symbol
issuer_name
backing_type (fiat/crypto/algorithmic)
peg_currency_code
launch_date
current_flag
effective_date
expiration_date
```

**DIM_CHAIN**
```
PK: chain_key (surrogate)
NK: chain_code (business key)
---
chain_name
chain_full_name
network_type (mainnet/testnet)
layer_type (L1/L2/sidechain)
consensus_mechanism
native_currency_symbol
is_evm_compatible
current_flag
effective_date
expiration_date
```

**DIM_ADDRESS**
```
PK: address_key (surrogate)
NK: address_hash, chain_key (composite natural key)
---
address_full_hash
address_type (EOA/contract/exchange/protocol)
address_label
entity_name (if known)
risk_category
first_seen_timestamp
last_seen_timestamp
is_active
```

**DIM_PROTOCOL**
```
PK: protocol_key (surrogate)
NK: protocol_code (business key)
---
protocol_name
protocol_type (lending/DEX/CDP)
protocol_category
tvl_rank
launch_date
primary_chain_key (FK)
is_active
```

**DIM_DATE**
```
PK: date_key (surrogate)
NK: full_date (business key)
---
year
quarter
month
week_of_year
day_of_month
day_of_week
is_weekend
is_month_end
is_quarter_end
is_year_end
fiscal_period
```

**DIM_TIME** (for intraday analysis)
```
PK: time_key (surrogate)
NK: time_of_day (business key)
---
hour
minute
second
time_period (morning/afternoon/evening/night)
business_hours_flag
```

### Fact Tables

**FACT_SUPPLY_SNAPSHOT** (Periodic Snapshot)
```
PK: supply_snapshot_key (surrogate)
FK: date_key
FK: stablecoin_key
FK: chain_key
---
Measures:
- total_supply_amount
- circulating_supply_amount
- reserve_amount (if applicable)
- burned_cumulative_amount
- minted_cumulative_amount
- daily_minted_amount (derived)
- daily_burned_amount (derived)
- daily_net_change_amount (derived)
- holder_count
- snapshot_timestamp
- data_quality_score
```

**FACT_TRANSACTION** (Transaction/Atomic)
```
PK: transaction_key (surrogate)
FK: date_key
FK: time_key
FK: stablecoin_key
FK: chain_key
FK: from_address_key
FK: to_address_key
NK: transaction_hash, chain_key (business key)
---
Degenerate Dimensions:
- block_number
- transaction_index
- transaction_type_code (transfer/mint/burn/swap)

Measures:
- amount
- amount_usd
- gas_fee_native
- gas_fee_usd
- gas_price
- gas_used
- transaction_timestamp
- confirmation_count
```

**FACT_ADDRESS_BALANCE_SNAPSHOT** (Periodic Snapshot)
```
PK: balance_snapshot_key (surrogate)
FK: date_key
FK: address_key
FK: stablecoin_key
FK: chain_key
---
Measures:
- opening_balance_amount
- closing_balance_amount
- total_received_amount
- total_sent_amount
- net_flow_amount
- transaction_count_in
- transaction_count_out
- first_transaction_flag
- last_transaction_flag
```

**FACT_ADDRESS_ACTIVITY_DAILY** (Accumulating Snapshot)
```
PK: activity_key (surrogate)
FK: date_key
FK: stablecoin_key
FK: chain_key
---
Semi-Additive Measures:
- unique_active_addresses_count
- unique_new_addresses_count
- unique_sender_addresses_count
- unique_receiver_addresses_count

Additive Measures:
- total_transactions_count
- total_volume_amount
- total_volume_usd
```

**FACT_LOAN** (Transaction/Atomic)
```
PK: loan_key (surrogate)
FK: loan_origination_date_key
FK: loan_maturity_date_key (can be null)
FK: loan_closed_date_key (can be null)
FK: stablecoin_borrowed_key
FK: stablecoin_collateral_key (can be null)
FK: chain_key
FK: borrower_address_key
FK: protocol_key
NK: loan_id, protocol_key (business key)
---
Degenerate Dimensions:
- loan_status_code (active/repaid/liquidated/defaulted)
- loan_type_code (overcollateralized/undercollateralized/flash)

Measures:
- principal_amount
- principal_amount_usd
- collateral_amount
- collateral_amount_usd
- interest_rate_annual_pct
- liquidation_threshold_pct
- ltv_ratio_pct
- origination_fee_amount
- interest_accrued_amount
- repaid_amount
- liquidated_amount
- outstanding_amount
- loan_duration_days (derived)
- loan_origination_timestamp
- loan_maturity_timestamp
- loan_closed_timestamp
```

### Bridge/Outrigger Tables

**BRIDGE_STABLECOIN_CHAIN_CONTRACT**
```
PK: stablecoin_chain_key (surrogate)
FK: stablecoin_key
FK: chain_key
NK: contract_address, chain_key (business key)
---
contract_address
deployment_date
deployment_block_number
is_official_contract
contract_version
proxy_contract_address (if applicable)
is_active
effective_date
expiration_date
```

### Logical Relationships & Cardinality
```
DIM_STABLECOIN (1) ──< (M) FACT_SUPPLY_SNAPSHOT
DIM_CHAIN (1) ──< (M) FACT_SUPPLY_SNAPSHOT
DIM_DATE (1) ──< (M) FACT_SUPPLY_SNAPSHOT

DIM_STABLECOIN (1) ──< (M) FACT_TRANSACTION
DIM_CHAIN (1) ──< (M) FACT_TRANSACTION
DIM_ADDRESS (1) ──< (M) FACT_TRANSACTION [as sender]
DIM_ADDRESS (1) ──< (M) FACT_TRANSACTION [as receiver]

DIM_STABLECOIN (1) ──< (M) FACT_LOAN
DIM_PROTOCOL (1) ──< (M) FACT_LOAN
DIM_ADDRESS (1) ──< (M) FACT_LOAN [as borrower]
```
