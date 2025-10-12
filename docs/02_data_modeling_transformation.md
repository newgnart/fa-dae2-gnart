# Data Modeling & Transformation

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
| chain             | string        | chain of the log              |
| contract_address  | string        | contract address of the log   |


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
| chain             | string        | chain of the transaction             |
| contract_address  | string        | contract address of the transaction  |

## Transformation
### Logs table
- `topics[0]` is the hash of event signature, so we can use it to get the event name.
- 