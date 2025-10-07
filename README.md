# RabbitStream Examples

This repository demonstrates how to use **RabbitStream** to stream Solana transactions in real-time. It includes examples for detecting newly minted tokens, monitoring transaction latency, and comparing with Yellowstone Geyser streams.

---

## What is RabbitStream 🐇?

**RabbitStream** is a fast, Solana-focused transaction streamer similar to **Yellowstone Geyser gRPC**, but with some key differences:

- **Speed:** Faster transaction updates compared to Yellowstone Geyser.  
- **Transaction-only:** Provides transaction data only, without full transaction metadata.  
- **gRPC Style:** Uses a gRPC-based subscription model for real-time streaming.  

RabbitStream is ideal for applications that need low-latency transaction monitoring, such as DeFi dashboards, arbitrage bots, or token analytics tools.

---

## Included Examples

### 1. Stream Pump.fun New Minted Tokens

- Path: `PumpFun/Typescript/stream-pump-fun-new-minted-tokens`
- Features:
  - Connects to RabbitStream and Yellowstone Geyser.
  - Detects newly minted Pump.fun tokens in real-time.
  - Compares latency between endpoints.
  - Parses transaction instructions using `@shyft-to/solana-transaction-parser`.

Other utilities are included in `utils/` for transaction formatting, event parsing, and BN layout handling.

---