# Data Ingestion & Pipeline

## Historical data loading with batch pipeline

### Data Source
- Historical data was downloaded/extracted from [Etherscan](https://docs.etherscan.io/) through their free API
  - Runable command: `uv run scripts/extract/runner.py --address <contract_address> --from_block <from_block> --to_block <to_block> --logs --transactions`
  - `from_block` and `to_block` support K/M/B suffixes, e.g. `18.5M` if not provided, will be resolved to the contract creation block and latest block respectively
  - Example:
  ```bash
  uv run scripts/extract/runner.py --address 0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E --from_block 18.4M --to_block 18.5M --logs
  ```
  - 

- File naming convention: `{blockchain}_{contract_address}_{table_name}_{from_block}_{to_block}.parquet` where:
  - `blockchain`: name of the blockchain, e.g. `ethereum`
  - `contract_address`: address of the smart contract, in lowercase, e.g. `0xf939e0a03fb07f59a73314e73794be0e57ac1b4e`
  - `table_name`: `logs` or `transactions`
  - `start_block`: start block of the data, e.g. `10000000`
  - `end_block`: end block of the data, e.g. `10000000`
  - Example: `ethereum_0xf939e0a03fb07f59a73314e73794be0e57ac1b4e_logs_18400000_18500000.parquet`

### Batch pipeline to local Postgres

uv run scripts/el/load.py -f sampledata/transactions_tiny.parquet -c postgres -s raw_tiny -t logs
### Batch pipeline to Snowflake
 uv run scripts/el/load.py -f sampledata/transactions_tiny.parquet -c snowflake -s raw_tiny -t logs

## Streaming data with real-time pipeline

### Streaming pipeline to local Postgres

### Streaming pipeline to Snowflake