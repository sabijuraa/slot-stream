//! Common types for Solana data structures.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A 32-byte account public key. Serialized as base58, like `Signature`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountKey(pub [u8; 32]);

impl Serialize for AccountKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_base58())
    }
}

impl<'de> Deserialize<'de> for AccountKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_base58(&s).map_err(serde::de::Error::custom)
    }
}

impl AccountKey {
    /// Create from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(bytes);
            Some(Self(arr))
        } else {
            None
        }
    }

    /// Create from a base58 string.
    pub fn from_base58(s: &str) -> crate::Result<Self> {
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|e| crate::Error::EventParse(format!("account key is not base58: {e}")))?;
        Self::from_bytes(&bytes).ok_or_else(|| {
            crate::Error::EventParse(format!(
                "account key must decode to 32 bytes, got {}",
                bytes.len()
            ))
        })
    }

    /// Encode as base58.
    pub fn to_base58(&self) -> String {
        bs58::encode(&self.0).into_string()
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for AccountKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base58())
    }
}

/// A 64-byte transaction signature.
///
/// Serialized as base58 rather than a byte array: it is the representation used
/// by Solana RPC, explorers, and our own JSON payloads, and `[u8; 64]` has no
/// serde impls anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Signature(pub [u8; 64]);

impl Serialize for Signature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_base58())
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_base58(&s).map_err(serde::de::Error::custom)
    }
}

impl Signature {
    /// Create from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() == 64 {
            let mut arr = [0u8; 64];
            arr.copy_from_slice(bytes);
            Some(Self(arr))
        } else {
            None
        }
    }

    /// Create from a base58 string.
    pub fn from_base58(s: &str) -> crate::Result<Self> {
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|e| crate::Error::EventParse(format!("signature is not base58: {e}")))?;
        Self::from_bytes(&bytes).ok_or_else(|| {
            crate::Error::EventParse(format!(
                "signature must decode to 64 bytes, got {}",
                bytes.len()
            ))
        })
    }

    /// Encode as base58.
    pub fn to_base58(&self) -> String {
        bs58::encode(&self.0).into_string()
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base58())
    }
}

/// Transaction metadata extracted during indexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMeta {
    /// Transaction signature.
    pub signature: String,

    /// Slot containing this transaction.
    pub slot: u64,

    /// Position in the slot.
    pub index: u32,

    /// Whether the transaction succeeded.
    pub success: bool,

    /// Fee paid in lamports.
    pub fee: u64,

    /// Compute units consumed.
    pub compute_units: Option<u64>,

    /// Account keys involved.
    pub account_keys: Vec<String>,

    /// Program IDs invoked.
    pub program_ids: Vec<String>,

    /// Error message if failed.
    pub error: Option<String>,
}

/// Account update metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    /// Account public key.
    pub pubkey: String,

    /// Slot of this update.
    pub slot: u64,

    /// Account owner program.
    pub owner: String,

    /// Account balance in lamports.
    pub lamports: u64,

    /// Account data (base64 encoded for large data).
    pub data: AccountData,

    /// Whether the account is executable.
    pub executable: bool,

    /// Rent epoch.
    pub rent_epoch: u64,

    /// Write version for ordering updates to same account.
    pub write_version: u64,
}

/// Account data representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "encoding")]
pub enum AccountData {
    /// Raw bytes (for small accounts).
    #[serde(rename = "raw")]
    Raw { data: Vec<u8> },

    /// Base64 encoded (for large accounts).
    #[serde(rename = "base64")]
    Base64 { data: String },

    /// Data was truncated.
    #[serde(rename = "truncated")]
    Truncated { size: usize },
}

impl AccountData {
    /// Get the size of the account data.
    pub fn size(&self) -> usize {
        match self {
            AccountData::Raw { data } => data.len(),
            AccountData::Base64 { data } => {
                // Approximate decoded size
                data.len() * 3 / 4
            }
            AccountData::Truncated { size } => *size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_key_roundtrip() {
        let original = "11111111111111111111111111111111";
        let key = AccountKey::from_base58(original).unwrap();
        let encoded = key.to_base58();
        assert_eq!(original, encoded);
    }

    #[test]
    fn keys_and_signatures_round_trip_through_json_as_base58() {
        let key = AccountKey([7u8; 32]);
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, format!("\"{}\"", key.to_base58()));
        assert_eq!(serde_json::from_str::<AccountKey>(&json).unwrap(), key);

        let sig = Signature([9u8; 64]);
        let json = serde_json::to_string(&sig).unwrap();
        assert_eq!(json, format!("\"{}\"", sig.to_base58()));
        assert_eq!(serde_json::from_str::<Signature>(&json).unwrap(), sig);
    }

    #[test]
    fn wrong_length_base58_is_rejected_with_a_useful_message() {
        let err = Signature::from_base58("11111111111111111111111111111111").unwrap_err();
        assert!(err.to_string().contains("64 bytes"), "got: {err}");
    }

    #[test]
    fn test_account_data_size() {
        let raw = AccountData::Raw { data: vec![0; 100] };
        assert_eq!(raw.size(), 100);

        let truncated = AccountData::Truncated { size: 1000 };
        assert_eq!(truncated.size(), 1000);
    }
}
