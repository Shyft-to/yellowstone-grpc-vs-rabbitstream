use {
    backoff::{future::retry, ExponentialBackoff},
    chrono::{DateTime, Utc},
    clap::Parser as ClapParser,
    futures::{stream::StreamExt, sink::SinkExt},
    log::info,
    std::{collections::HashMap, env, sync::Arc, time::Duration},
    solana_sdk::{
        hash::Hash,
        message::{v0::LoadedAddresses, MessageHeader},
        pubkey::Pubkey,
        signature::Signature,
        transaction::{Result as TransactionResult},
        message::v0::MessageAddressTableLookup,
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{TransactionTokenBalance, Rewards},
    tokio::{sync::Mutex, time::sleep},
    tonic::transport::ClientTlsConfig,
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        geyser::SubscribeRequestFilterTransactions,
        prelude::{subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing},
    },
};

mod processor;
use processor::TransactionProcessor;
use processor::types::DecodedInstruction;

type TxnFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, Clone, ClapParser)]
#[clap(author, version, about)]
struct Args {
    #[clap(long)] endpoint: String,
    #[clap(long)] x_token: String,

    #[clap(long)] rabbit_endpoint: String,
    #[clap(long)] rabbit_x_token: String,

    #[clap(long, default_value = "60")] timeout_dur: u64, // seconds
}

impl Args {
    async fn connect(&self, endpoint: &str, token: &str) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        let client = GeyserGrpcClient::build_from_shared(endpoint.to_owned())?
            .x_token(Some(token.to_owned()))?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;
        Ok(client)
    }

    fn build_txn_request(&self) -> SubscribeRequest {
        let mut transactions: TxnFilterMap = HashMap::new();
        transactions.insert(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                account_include: vec![PUMPFUN_PROGRAM_ID.to_string()],
                account_exclude: vec![],
                account_required: vec![],
                signature: None,
            },
        );

        SubscribeRequest {
            transactions,
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParsedConfirmedTransactionWithStatusMeta {
    pub slot: u64,
    pub transaction: ParsedTransaction,
    pub meta: ParsedTransactionStatusMeta,
    pub block_time: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct ParsedTransaction {
    pub signatures: Vec<Signature>,
    pub message: ParsedMessage,
}

#[derive(Clone, Debug)]
pub struct ParsedMessage {
    pub header: MessageHeader,
    pub account_keys: Vec<Pubkey>,
    pub recent_blockhash: Hash,
    pub instructions: Vec<DecodedInstruction>,
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

#[derive(Clone, Debug)]
pub struct ParsedTransactionStatusMeta {
    pub status: TransactionResult<()>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Vec<DecodedInstruction>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub rewards: Option<Rewards>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Clone)]
struct Detection {
    #[allow(dead_code)]
    token: String,
    signature: String,
    time: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe {
        env::set_var(
            env_logger::DEFAULT_FILTER_ENV,
            env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
        );
    }
    env_logger::init();
    let args = Arc::new(Args::parse());

    info!("Starting latency comparison...");
    info!("Timeout: {}s", args.timeout_dur);

    let (tx_rabbit, tx_yellow) = (args.rabbit_endpoint.clone(), args.endpoint.clone());
    let (token_rabbit, token_yellow) = (args.rabbit_x_token.clone(), args.x_token.clone());
    let timeout = args.timeout_dur;

    // Shared detection map: token -> {rabbit?, yellowstone?}
    let detections: Arc<Mutex<HashMap<String, (Option<Detection>, Option<Detection>)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let geyser_task = run_stream(
        "yellowstone geyser stream",
        args.clone(),
        tx_yellow,
        token_yellow,
        args.build_txn_request(),
        detections.clone(),
    );

    let rabbit_task = run_stream(
        "rabbit stream",
        args.clone(),
        tx_rabbit,
        token_rabbit,
        args.build_txn_request(),
        detections.clone(),
    );

    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl+C");
    };

    info!("Press Ctrl+C to stop early, or wait for timeout...");

    tokio::select! {
        _ = sleep(Duration::from_secs(timeout)) => {
            info!("\n Timeout reached. Stopping streams...");
        }
        _ = ctrl_c => {
            info!("\n Interrupted by user (Ctrl+C). Cleaning up...");
        }
        _ = futures::future::join(geyser_task, rabbit_task) => {}
    }

    let map = detections.lock().await;
    print_latency_report(&*map);

    Ok(())
}

async fn run_stream(
    name: &str,
    args: Arc<Args>,
    endpoint: String,
    token: String,
    request: SubscribeRequest,
    detections: Arc<Mutex<HashMap<String, (Option<Detection>, Option<Detection>)>>>,
) {
    let _: Result<(), anyhow::Error> = retry(ExponentialBackoff::default(), || {
        let name = name.to_string();
        let args = args.clone();
        let request = request.clone();
        let detections = detections.clone();
        let endpoint = endpoint.clone();
        let token = token.clone();

        async move {
            let mut client = args.connect(&endpoint, &token).await
                .map_err(backoff::Error::transient)?;

            let (mut tx, mut stream) = client.subscribe().await
                .map_err(|e| backoff::Error::transient(anyhow::anyhow!(e)))?;
            let processor = TransactionProcessor::new()
                .map_err(backoff::Error::transient)?;

            tx.send(request.clone()).await
                 .map_err(|e| backoff::Error::transient(anyhow::anyhow!(e)))?;

            while let Some(msg) = stream.next().await {
                let Ok(update) = msg else { break };

                if let Some(UpdateOneof::Transaction(txn)) = update.update_oneof {
                    if let Ok(Some(pump_tx)) = processor.process_transaction_update(txn) {
                        if let Some(ix) = pump_tx.transaction.message.instructions.iter().chain(pump_tx.meta.inner_instructions.iter()).find(|ix| ix.name == "create" || ix.name == "createV2") {
                            let token = ix.accounts[0].pubkey.to_string();
                            let sig = pump_tx.transaction.signatures.first().unwrap().to_string();
                            let now = Utc::now();

                            {
                                let mut map = detections.lock().await;
                                let entry = map.entry(token.clone()).or_insert((None, None));
                                match name.as_str() {
                                    "rabbit stream" => entry.0 = Some(Detection { token: token.clone(), signature: sig.clone(), time: now }),
                                    "yellowstone geyser stream" => entry.1 = Some(Detection { token: token.clone(), signature: sig.clone(), time: now }),
                                    _ => (),
                                }
                            }

                            info!("[{name}] Detected token {token} in txn {sig}");
                        }
                    }
                } else if let Some(UpdateOneof::Ping(_)) = update.update_oneof {
                    let _ = tx.send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    }).await;
                }
            }
            Err(backoff::Error::transient(anyhow::anyhow!("Stream ended; retrying")))
        }
    }).await;
}

fn print_latency_report(map: &HashMap<String, (Option<Detection>, Option<Detection>)>) {
    use comfy_table::{Table, presets::UTF8_FULL, modifiers::UTF8_ROUND_CORNERS};

    let mut table = Table::new();
    table.load_preset(UTF8_FULL).apply_modifier(UTF8_ROUND_CORNERS);
    table.set_header(vec![
        "(index)", "Token", "Signature", "Faster", "Slower",
        "Diff (ms)", "Yellowstone geyser", "Rabbit stream"
    ]);

    let mut idx = 0;
    let mut total_diff = 0f64;
    let mut count = 0;

    for (token, (rabbit, yellow)) in map.iter() {
        if let (Some(r), Some(y)) = (rabbit, yellow) {
            let dur = (y.time - r.time).num_milliseconds();
            let (faster, slower) = if dur > 0 {
                ("rabbit stream", "yellowstone geyser stream")
            } else {
                ("yellowstone geyser stream", "rabbit stream")
            };
            let diff = dur.abs();
            table.add_row(vec![
                idx.to_string(),
                token.clone(),
                r.signature.clone(),
                faster.to_string(),
                slower.to_string(),
                diff.to_string(),
                y.time.to_rfc3339(),
                r.time.to_rfc3339(),
            ]);
            total_diff += diff as f64;
            count += 1;
            idx += 1;
        }
    }

    println!("\n--- TOKEN LAUNCH LATENCY COMPARISON ---");
    println!("{table}");
    println!(
        "\n--- SUMMARY ---\nTokens detected by BOTH: {count}\nRabbit was faster for {count} tokens, avg advantage: {:.2} ms",
        total_diff / count.max(1) as f64
    );
    println!("\nExiting...");
}
