# Data Ingestion & Pipeline

## Historical data loading with batch pipeline

### Data Source
- 3 smart contracts on Ethereum blockchain:
  - [crvusd](https://etherscan.io/address/0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E)
  - [gho](https://etherscan.io/address/0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f)
  - [frxusd](https://etherscan.io/address/0xCAcd6fd266aF91b8AeD52aCCc382b4e165586E29)
- Historical data was downloaded/extracted from [Etherscan](https://docs.etherscan.io/) through their free API, using Python library [onchaindata](https://github.com/newgnart/onchaindata). Runable scripts: [scripts/data_extracting/ethereum_data_loader.py](../scripts/data_extracting/ethereum_data_loader.py) in parquet format: 
- File naming convention: `{blockchain}_{contract_address}_{start_block}_{end_block}_{table_name}.parquet` where:
  - `blockchain`: name of the blockchain, e.g. `ethereum`
  - `contract_address`: address of the smart contract, e.g. `0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E`
  - `start_block`: start block of the data, e.g. `10000000`
  - `end_block`: end block of the data, e.g. `10000000`
  - `table_name`:`logs` or `transactions`
  - Example: `ethereum_0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E_10000000_10000000_logs.parquet`

### Batch pipeline to local Postgres

### Batch pipeline to Snowflake

## Streaming data with real-time pipeline

### Streaming pipeline to local Postgres

### Streaming pipeline to Snowflake