# Monitoring Newly Minted Pump.fun Tokens: Yellowstone gRPC vs Rabbitstream

This project provides a Node.js service to monitor and parse newly minted tokens on Pump.fun using **Solana gRPC streams**. It compares detection latency between **Yellowstone gRPC** and **Rabbitstream**.

## Usage
```bash
cargo run -- --endpoint https://grpc.fra.shyft.to --x-token <xtoken> --rabbit-endpoint https://rabbitstream.fra.shyft.to --rabbit-x-token <xtoken>
```

## Screenshot
- ![screenshot](assets/new-token.png?raw=true "Screenshot")