# Data Modeling & Transformation

## Conceptual Data Model
The entities and their relationships

- Entities
  - USER: Users who interact with stablecoins
  - ACQUISITION: How users obtain stablecoins (mint, buy, receive)
  - ALLOCATION: What users do with stablecoins (provide liquidity, lend, trade, hold)
  - EXIT: How users dispose of stablecoins (burn, sell, transfer out)
  - VENUE: Where interactions occur (protocols, pools, exchanges)

- Relationships
  - USER --performs--> ACQUISITION --> gains stablecoins
  - USER --performs--> ALLOCATION --> uses stablecoins at VENUE
  - USER --performs--> EXIT --> loses stablecoins


## Logical Data Model

```
┌─────────────────┐     ┌──────────────────┐
│   RAW_LOGS      │     │ RAW_TRANSACTIONS │
│   (Raw Layer)   │     │   (Raw Layer)    │
└────────┬────────┘     └────────┬─────────┘
         │                       │
         └───────────┬───────────┘
                     │ Decode
                     ▼
         ┌───────────────────────┐
         │    DECODED_EVENTS     │
         └───────────┬───────────┘
                     │ Business Logic
                     ▼
    ┌────────────────┴─────────────────┐
    │                                  │
    ▼                                  ▼
┌──────────────┐              ┌──────────────────┐
│ FACT Tables  │              │  DIM Tables      │
│ - Acquisition│              │  - User          │
│ - Allocation │              │  - Venue         │
│ - Exit       │              │  - Date          │
│ - Balance    │              └──────────────────┘
└──────────────┘
```

## Raw data
### Logs table
| Column Name       | Data Type     | Description                   |
| ----------------- | ------------- | ----------------------------- |
| address           | string        | address of the smart contract |
| topics            | array[string] | topics of the log             |
| data              | string        | data of the log               |
| block_number      | integer       | block number of the log       |
| block_hash        | string        | block hash of the log         |
| time_stamp        | timestamp     | timestamp of the log          |
| gas_price         | decimal       | gas price of the log          |
| gas_used          | integer       | gas used of the log           |
| log_index         | integer       | log index of the log          |
| transaction_hash  | string        | transaction hash of the log   |
| transaction_index | integer       | transaction index of the log  |
| chainid           | integer       | chain id of the log           |

### Transactions table
| Column Name       | Data Type     | Description                          |
| ----------------- | ------------- | ------------------------------------ |
| address           | string        | address of the smart contract        |
| topics            | array[string] | topics of the transaction            |
| data              | string        | data of the transaction              |
| block_number      | integer       | block number of the transaction      |
| block_hash        | string        | block hash of the transaction        |
| time_stamp        | timestamp     | timestamp of the transaction         |
| gas_price         | decimal       | gas price of the transaction         |
| gas_used          | integer       | gas used of the transaction          |
| transaction_index | integer       | transaction index of the transaction |
| chainid           | integer       | chain id of the transaction          |


## Transformation
### Logs table
- `topics[0]` is the hash of event signature, so we can use it to get the event name.




## Appendix

- test dbt connection
  - ./scripts/dbt.sh debug --target test
  - ./scripts/dbt.sh debug --target dev
- 