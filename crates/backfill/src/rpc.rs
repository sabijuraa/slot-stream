//! Solana JSON-RPC client for historical blocks.

use serde::Deserialize;
use slot_stream_common::{Error, Result};
use std::time::Duration;
use tracing::{debug, warn};

/// A minimal Solana JSON-RPC client.
///
/// Only the three calls backfill needs are implemented: which slots have blocks,
/// what is in a block, and where the chain currently is. Anything else belongs to
/// the caller's own RPC client.
pub struct RpcClient {
    endpoint: String,
    http: reqwest::Client,
    max_retries: u32,
}

impl RpcClient {
    /// Create a client for `endpoint`.
    pub fn new(endpoint: impl Into<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Configuration(format!("building HTTP client: {e}")))?;

        Ok(Self {
            endpoint: endpoint.into(),
            http,
            max_retries: 3,
        })
    }

    /// Set how many times a failing call is retried.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// The endpoint in use.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Confirmed slots that produced a block, in `[start, end]`.
    pub async fn get_blocks(&self, start: u64, end: u64) -> Result<Vec<u64>> {
        if start > end {
            return Err(Error::BackfillRange { start, end });
        }
        self.call("getBlocks", serde_json::json!([start, end])).await
    }

    /// The current slot.
    pub async fn get_slot(&self) -> Result<u64> {
        self.call(
            "getSlot",
            serde_json::json!([{ "commitment": "confirmed" }]),
        )
        .await
    }

    /// A block, or `None` if the slot was skipped or has been pruned.
    pub async fn get_block(&self, slot: u64) -> Result<Option<BlockData>> {
        let params = serde_json::json!([
            slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 0,
                "rewards": false,
                "commitment": "confirmed",
            }
        ]);

        match self.call::<Option<RpcBlock>>("getBlock", params).await {
            Ok(Some(block)) => Ok(Some(BlockData::from_rpc(slot, block))),
            Ok(None) => Ok(None),
            Err(e) => {
                // A skipped or pruned slot is a normal answer, not a failure.
                let text = e.to_string();
                if text.contains("-32009") || text.contains("-32007") || text.contains("was skipped")
                {
                    debug!(slot, "slot skipped or pruned");
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Issue a JSON-RPC call, retrying transient failures.
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let mut backoff = Duration::from_millis(200);
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }

            let response = match self.http.post(&self.endpoint).json(&body).send().await {
                Ok(response) => response,
                Err(e) => {
                    warn!(method, attempt, error = %e, "RPC request failed");
                    last_error = Some(Error::GrpcConnection(e.to_string()));
                    continue;
                }
            };

            let status = response.status();
            if status.as_u16() == 429 || status.is_server_error() {
                warn!(method, attempt, %status, "RPC returned a retryable status");
                last_error = Some(Error::GrpcConnection(format!("HTTP {status}")));
                continue;
            }

            let envelope: RpcEnvelope<T> = response
                .json()
                .await
                .map_err(|e| Error::EventParse(format!("decoding {method} response: {e}")))?;

            if let Some(error) = envelope.error {
                return Err(Error::EventParse(format!(
                    "RPC {method} failed: {} (code {})",
                    error.message, error.code
                )));
            }

            return envelope.result.ok_or_else(|| {
                Error::EventParse(format!("RPC {method} returned neither result nor error"))
            });
        }

        Err(last_error.unwrap_or_else(|| {
            Error::GrpcConnection(format!("{method} exhausted {} retries", self.max_retries))
        }))
    }
}

#[derive(Debug, Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    #[serde(rename = "parentSlot")]
    parent_slot: u64,
    blockhash: String,
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
    #[serde(default)]
    transactions: Vec<RpcTransaction>,
}

#[derive(Debug, Deserialize)]
struct RpcTransaction {
    transaction: RpcTransactionInner,
    meta: Option<RpcMeta>,
}

#[derive(Debug, Deserialize)]
struct RpcTransactionInner {
    #[serde(default)]
    signatures: Vec<String>,
    message: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct RpcMeta {
    err: Option<serde_json::Value>,
    #[serde(default)]
    fee: u64,
    #[serde(rename = "computeUnitsConsumed")]
    compute_units_consumed: Option<u64>,
}

/// A block, reduced to what the indexer stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockData {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: String,
    pub block_time: Option<i64>,
    pub transactions: Vec<TransactionData>,
}

impl BlockData {
    fn from_rpc(slot: u64, block: RpcBlock) -> Self {
        let transactions = block
            .transactions
            .into_iter()
            .enumerate()
            .map(|(index, tx)| TransactionData {
                index: index as u32,
                signature: tx
                    .transaction
                    .signatures
                    .first()
                    .cloned()
                    .unwrap_or_default(),
                success: tx.meta.as_ref().is_none_or(|m| m.err.is_none()),
                fee: tx.meta.as_ref().map(|m| m.fee).unwrap_or(0),
                compute_units: tx.meta.as_ref().and_then(|m| m.compute_units_consumed),
                message: tx.transaction.message,
            })
            .collect();

        Self {
            slot,
            parent_slot: block.parent_slot,
            block_hash: block.blockhash,
            block_time: block.block_time,
            transactions,
        }
    }
}

/// A transaction within a backfilled block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionData {
    pub index: u32,
    pub signature: String,
    pub success: bool,
    pub fee: u64,
    pub compute_units: Option<u64>,
    pub message: serde_json::Value,
}

impl TransactionData {
    /// The JSON body stored for this transaction.
    pub fn to_body(&self) -> serde_json::Value {
        serde_json::json!({
            "signature": self.signature,
            "index": self.index,
            "success": self.success,
            "fee": self.fee,
            "compute_units": self.compute_units,
            "message": self.message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_inverted_range_is_rejected_before_any_request() {
        let client = RpcClient::new("http://127.0.0.1:1").unwrap();
        let result = futures::executor::block_on(client.get_blocks(100, 50));
        assert!(matches!(
            result,
            Err(Error::BackfillRange { start: 100, end: 50 })
        ));
    }

    #[test]
    fn block_conversion_extracts_the_fields_we_index() {
        let block: RpcBlock = serde_json::from_value(serde_json::json!({
            "parentSlot": 99,
            "blockhash": "abc",
            "blockTime": 1_700_000_000i64,
            "transactions": [
                {
                    "transaction": { "signatures": ["sig1"], "message": {"k": 1} },
                    "meta": { "err": null, "fee": 5000, "computeUnitsConsumed": 1234 }
                },
                {
                    "transaction": { "signatures": ["sig2"], "message": {} },
                    "meta": { "err": {"InstructionError": []}, "fee": 5000 }
                }
            ]
        }))
        .unwrap();

        let data = BlockData::from_rpc(100, block);
        assert_eq!(data.slot, 100);
        assert_eq!(data.parent_slot, 99);
        assert_eq!(data.transactions.len(), 2);
        assert_eq!(data.transactions[0].signature, "sig1");
        assert!(data.transactions[0].success);
        assert_eq!(data.transactions[0].compute_units, Some(1234));
        assert!(!data.transactions[1].success, "err means failed");
    }
}
