# Monitoring Newly Minted Pump.fun Tokens: Yellowstone gRPC vs Rabbitstream

This project provides a Node.js service to monitor and parse newly minted tokens on Pump.fun using **Solana gRPC streams**. It compares detection latency between **Yellowstone gRPC** and **Rabbitstream**.

## Features

- Real-time monitoring of new token transactions on Pump.fun.
- Parses transaction instructions using `@shyft-to/solana-transaction-parser`.
- Supports comparison of detection latency between Yellowstone gRPC and Rabbitstream.
- Configurable via `.env` file for authentication and timeouts.

- ![screenshot](assets/new-token.png?raw=true "Screenshot")


## Installation
```sh
git clone https://github.com/Shyft-to/yellowstone-grpc-vs-rabbitstream.git
cd PumpFun/Typescript/stream-pump-fun-new-minted-tokens
npm install
```

## Configuration
Create a `.env` file in the root directory based on `.env.example`:
```
GRPC_URL=GRPC_ENDPOINT
X_TOKEN=ENDPOINT_AUTH_TOKEN
RABBIT_STREAM_URL=RABBITSTREAM_ENDPOINT
RABBIT_X_TOKEN=ENDPOINT_AUTH_TOKEN
TIMEOUT=60 # in seconds
```

### Run
```sh
npm run start
```

## Dependencies
- `@solana/web3.js` for interacting with Solana blockchain.
- `@triton-one/yellowstone-grpc` for gRPC communication.
- `@shyft-to/solana-transaction-parser` for transaction parsing.

## License
This project is licensed under the [MIT License](LICENSE).
