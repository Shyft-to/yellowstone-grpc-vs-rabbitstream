import "dotenv/config";
import Client, { CommitmentLevel, SubscribeRequestAccountsDataSlice } from "@triton-one/yellowstone-grpc";
import { PublicKey, VersionedTransactionResponse } from "@solana/web3.js";
import { Idl } from "@coral-xyz/anchor";
import { SolanaParser } from "@shyft-to/solana-transaction-parser";
import { TransactionFormatter } from "./utils/transaction-formatter";
import { SolanaEventParser } from "./utils/event-parser";
import { bnLayoutFormatter } from "./utils/bn-layout-formatter";
import pumpFunAmmIdl from "./idls/pump_0.1.0.json";
import { pumpFunParsedTransaction } from "./utils/pump-fun-parsed-transaction";
import { SubscribeRequestPing } from "node_modules/@triton-one/yellowstone-grpc/dist/types/grpc/geyser";

["warn", "log", "error"].forEach(level => {
  const original = console[level as "warn" | "log" | "error"];
  console[level as "warn" | "log" | "error"] = (msg?: any, ...args: any[]) => {
    if (typeof msg === "string" && msg.includes("Parser does not matching the instruction args")) return;
    original(msg, ...args);
  };
});

const TIMEOUT_MS = parseInt(process.env.TIMEOUT || "60") * 1000;
const PUMP_FUN_PROGRAM_ID = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const PUMP_FUN_PROGRAM_ID_STRING = PUMP_FUN_PROGRAM_ID.toBase58();

const GRPC_URL = process.env.GRPC_URL!;
const RABBIT_STREAM_URL = process.env.RABBIT_STREAM_URL!;

const TXN_FORMATTER = new TransactionFormatter();

const PUMP_FUN_IX_PARSER = new SolanaParser([]);
PUMP_FUN_IX_PARSER.addParserFromIdl(PUMP_FUN_PROGRAM_ID_STRING, pumpFunAmmIdl as Idl);

const PUMP_FUN_EVENT_PARSER = new SolanaEventParser([], console);
PUMP_FUN_EVENT_PARSER.addParserFromIdl(PUMP_FUN_PROGRAM_ID_STRING, pumpFunAmmIdl as Idl);

const req = {
  accounts: {},
  slots: {},
  transactions: {
    pumpFun: {
      vote: false,
      failed: false,
      signature: undefined,
      accountInclude: [PUMP_FUN_PROGRAM_ID_STRING],
      accountExclude: [],
      accountRequired: [],
    },
  },
  transactionsStatus: {},
  entry: {},
  blocks: {},
  blocksMeta: {},
  accountsDataSlice: [] as SubscribeRequestAccountsDataSlice[],
  ping: undefined as SubscribeRequestPing | undefined,
  commitment: CommitmentLevel.PROCESSED,
};

const yellowstoneClient = new Client(GRPC_URL, process.env.X_TOKEN, undefined);
const rabbitClient = new Client(RABBIT_STREAM_URL, process.env.RABBIT_X_TOKEN, undefined);

type Detection = { endpoint: string; detectedAt: number; signature: string };
type TokenRecord = { token: string; detections: Detection[] };
const tokenMap: Map<string, TokenRecord> = new Map();

let stopAllStreams = false;

function hydrateLoadedAddresses(tx: VersionedTransactionResponse) {
  const loaded = tx?.meta?.loadedAddresses;
  if (!loaded) return tx;

  const ensurePublicKey = (arr: (Buffer | PublicKey)[]) =>
    arr.map(item => (item instanceof PublicKey ? item : new PublicKey(item)));

  tx.meta!.loadedAddresses = {
    writable: ensurePublicKey(loaded.writable),
    readonly: ensurePublicKey(loaded.readonly),
  };
  return tx;
}

function decodePumpFunTxn(tx: VersionedTransactionResponse) {
  try {
    const parsedIxs = PUMP_FUN_IX_PARSER.parseTransactionData(tx.transaction.message, tx?.meta?.loadedAddresses);
    const pumpIxs = parsedIxs.filter(ix => ix.programId.equals(PUMP_FUN_PROGRAM_ID));
    const hydratedTx = hydrateLoadedAddresses(tx);
    const innerIxs = PUMP_FUN_IX_PARSER.parseTransactionWithInnerInstructions(hydratedTx)
      .filter(ix => ix.programId.equals(PUMP_FUN_PROGRAM_ID));

    if (!pumpIxs.length && !innerIxs.length) return;

    const events = PUMP_FUN_EVENT_PARSER.parseEvent(tx);
    const result = { instructions: pumpIxs, inner_ixs: innerIxs, events };
    bnLayoutFormatter(result);
    return result;
  } catch {
    return;
  }
}

async function subscribeAndDetect(client: Client, endpoint: string) {
  const stream = await client.subscribe();

  return new Promise<void>((resolve, reject) => {
    stream.on("data", (data) => {
      if (stopAllStreams || !data?.transaction) return;

      const txn = TXN_FORMATTER.formTransactionFromJson(data.transaction, Date.now());
      const parsedTxn = decodePumpFunTxn(txn);
      if (!parsedTxn) return;

      const pumpfunParsedTxn: any = pumpFunParsedTransaction(parsedTxn, txn);
      if (!pumpfunParsedTxn) return;

      const requiredIx = pumpfunParsedTxn.meta.innerInstructions.find(
        (ix: any) => ix.programId === PUMP_FUN_PROGRAM_ID_STRING && ix.name === "create"
      );

      if (!requiredIx) return;

      const token = requiredIx.accounts[0].pubkey.toString();
      const detectedAt = Date.now();
      const signature = txn.transaction.signatures[0];

      if (!tokenMap.has(token)) tokenMap.set(token, { token, detections: [] });
      tokenMap.get(token)!.detections.push({ endpoint, detectedAt, signature });

      console.log(`[${endpoint}] Detected token ${token} at ${new Date(detectedAt).toISOString()}`);
    });

    stream.on("error", reject);
    stream.on("end", resolve);
    stream.on("close", resolve);

    stream.write(req, (err: any) => { if (err) reject(err); });
  });
}

(async () => {
  console.log("Starting latency comparison...");
  console.log(`Timeout: ${TIMEOUT_MS / 1000}s`);

  const timeout = new Promise<void>((resolve) => {
    setTimeout(() => {
      console.log("\n⏳ Timeout reached. Stopping streams...");
      stopAllStreams = true;
      resolve();
    }, TIMEOUT_MS);
  });

  await Promise.race([
    Promise.all([
      subscribeAndDetect(yellowstoneClient, "yellowstone geyser stream"),
      subscribeAndDetect(rabbitClient, "rabbit stream"),
    ]),
    timeout,
  ]);

  console.log("\n--- TOKEN LAUNCH LATENCY COMPARISON ---");

  if (!tokenMap.size) {
    console.log("No tokens detected within the timeout.");
    process.exit(0);
  }

  type TokenDiff = {
    token: string;
    signature: string;
    faster: string;
    slower: string;
    diffMs: number;
    detectedYellowstone?: string;
    detectedRabbit?: string;
  };

  const diffs: TokenDiff[] = [];
  let totalAdvantageYellowstone = 0, totalAdvantageRabbit = 0;
  let countYellowstone = 0, countRabbit = 0;

  tokenMap.forEach(record => {
    if (record.detections.length < 2) return;

    const yellowstone = record.detections.find(d => d.endpoint.includes("yellowstone"))!;
    const rabbit = record.detections.find(d => d.endpoint.includes("rabbit"))!;
    if (!yellowstone || !rabbit) return;

    const diff = Math.abs(yellowstone.detectedAt - rabbit.detectedAt);
    let faster = yellowstone.detectedAt < rabbit.detectedAt ? "yellowstone geyser stream" : "rabbit stream";
    let slower = faster === "yellowstone geyser stream" ? "rabbit stream" : "yellowstone geyser stream";

    if (faster === "yellowstone geyser stream") { totalAdvantageYellowstone += diff; countYellowstone++; }
    else { totalAdvantageRabbit += diff; countRabbit++; }

    diffs.push({
      token: record.token,
      signature: yellowstone.signature || rabbit.signature,
      faster, slower, diffMs: diff,
      detectedYellowstone: new Date(yellowstone.detectedAt).toISOString(),
      detectedRabbit: new Date(rabbit.detectedAt).toISOString(),
    });
  });

  if (!diffs.length) {
    console.log("Tokens were not detected by both endpoints, cannot compare latency.");
  } else {
    console.table(diffs.map(d => ({
      Token: d.token,
      Signature: d.signature,
      Faster: d.faster,
      Slower: d.slower,
      "Diff (ms)": d.diffMs,
      "Yellowstone geyser": d.detectedYellowstone,
      "Rabbit stream": d.detectedRabbit,
    })));

    console.log("\n--- SUMMARY ---");
    console.log(`Tokens detected by BOTH: ${diffs.length}`);
    if (countYellowstone) console.log(`Yellowstone was faster for ${countYellowstone} tokens, avg advantage: ${(totalAdvantageYellowstone / countYellowstone).toFixed(2)} ms`);
    if (countRabbit) console.log(`Rabbit was faster for ${countRabbit} tokens, avg advantage: ${(totalAdvantageRabbit / countRabbit).toFixed(2)} ms`);
  }

  console.log("\nExiting...");
  process.exit(0);
})();
