## Primary Source: HyperIndex (Envio)

### `Transfer` data
Raw transaction data is indexed with [HyperIndex](https://docs.envio.dev/docs/HyperIndex/overview), a blockchain indexing framework that transforms on-chain events into structured, queryable databases with GraphQL APIs.

**To run the indexer:**
```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```

**Benefits:**
- ✅ Real-time continuous indexing
- ✅ Structured GraphQL queries
- ✅ Multiple contracts and events simultaneously
- ✅ No API rate limits

More details: [envio-stablecoins](https://github.com/newgnart/envio-stablecoins)

---

## Alternative Source: Etherscan API (Optional)

The repository includes Etherscan API extraction tools (`scripts/el/extract_etherscan.py`) as an alternative data source. While not used in the primary pipeline, it's useful for:

- Historical data extraction and validation
- Supporting additional EVM chains
- Ad-hoc analysis without running the indexer

**Trade-offs:**
- ✅ Flexible, no infrastructure needed
- ✅ 50+ EVM chains supported
- ❌ Rate limited (5 req/sec on free tier)
- ❌ Requires API key

For detailed usage, see [Additional Tools](../06_additional_tools.md).

---

## Wallet labels data
