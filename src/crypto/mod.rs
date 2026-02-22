//! Server-Side Encryption implementation using AES-256-GCM
//!
//! This module provides encryption and decryption for S3 objects using
//! AES-256-GCM (Galois/Counter Mode) authenticated encryption.

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::hkdf::{Salt, HKDF_SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use std::fmt;

/// Length of AES-256 key in bytes
pub const KEY_LENGTH: usize = 32;

/// Length of GCM nonce/IV in bytes
pub const NONCE_LENGTH: usize = 12;

/// Length of GCM authentication tag in bytes
pub const TAG_LENGTH: usize = 16;

/// Errors that can occur during encryption/decryption
#[derive(Debug)]
pub enum SseError {
    /// Master key is invalid (wrong length or format)
    InvalidMasterKey,
    /// Key derivation failed
    KeyDerivationFailed,
    /// Random nonce generation failed
    NonceGenerationFailed,
    /// Encryption operation failed
    EncryptionFailed(String),
    /// Decryption operation failed (authentication failed or data corrupted)
    DecryptionFailed(String),
}

impl fmt::Display for SseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SseError::InvalidMasterKey => write!(f, "Invalid master encryption key"),
            SseError::KeyDerivationFailed => write!(f, "Key derivation failed"),
            SseError::NonceGenerationFailed => write!(f, "Failed to generate random nonce"),
            SseError::EncryptionFailed(msg) => write!(f, "Encryption failed: {}", msg),
            SseError::DecryptionFailed(msg) => write!(f, "Decryption failed: {}", msg),
        }
    }
}

impl std::error::Error for SseError {}

/// Server-Side Encryption context for SSE-S3 operations
///
/// Holds the master encryption key and provides methods for encrypting
/// and decrypting object data using per-object derived keys.
pub struct SseContext {
    /// Master encryption key (32 bytes for AES-256)
    master_key: Vec<u8>,
    /// Secure random number generator
    random: SystemRandom,
}

impl SseContext {
    /// Create a new SSE context from a master key
    ///
    /// The master key must be exactly 32 bytes (256 bits).
    pub fn new(master_key: Vec<u8>) -> Result<Self, SseError> {
        if master_key.len() != KEY_LENGTH {
            return Err(SseError::InvalidMasterKey);
        }

        Ok(Self {
            master_key,
            random: SystemRandom::new(),
        })
    }

    /// Derive a per-object encryption key using HKDF
    ///
    /// The derived key is unique per object based on:
    /// - bucket name
    /// - object key
    /// - version ID
    fn derive_key(&self, bucket: &str, key: &str, version_id: &str) -> Result<[u8; KEY_LENGTH], SseError> {
        // Create info string for HKDF - unique per object
        let info = format!("s3:{}:{}:{}", bucket, key, version_id);

        // Use HKDF to derive a key
        let salt = Salt::new(HKDF_SHA256, &self.master_key);
        let prk = salt.extract(info.as_bytes());

        let mut derived_key = [0u8; KEY_LENGTH];
        prk.expand(&[b"aes256-gcm"], &AES_256_GCM)
            .map_err(|_| SseError::KeyDerivationFailed)?
            .fill(&mut derived_key)
            .map_err(|_| SseError::KeyDerivationFailed)?;

        Ok(derived_key)
    }

    /// Generate a random 12-byte nonce for GCM
    pub fn generate_nonce(&self) -> Result<[u8; NONCE_LENGTH], SseError> {
        let mut nonce = [0u8; NONCE_LENGTH];
        self.random
            .fill(&mut nonce)
            .map_err(|_| SseError::NonceGenerationFailed)?;
        Ok(nonce)
    }

    /// Encrypt data with AES-256-GCM
    ///
    /// Returns the ciphertext (with authentication tag appended) and the nonce.
    /// The nonce must be stored alongside the ciphertext for decryption.
    pub fn encrypt(
        &self,
        plaintext: &[u8],
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(Vec<u8>, [u8; NONCE_LENGTH]), SseError> {
        // Derive per-object key
        let derived_key = self.derive_key(bucket, key, version_id)?;

        // Generate random nonce
        let nonce_bytes = self.generate_nonce()?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        // Create AEAD key
        let unbound_key = UnboundKey::new(&AES_256_GCM, &derived_key)
            .map_err(|_| SseError::EncryptionFailed("Failed to create encryption key".into()))?;
        let sealing_key = LessSafeKey::new(unbound_key);

        // Encrypt in place (we need to allocate buffer with space for tag)
        let mut ciphertext = plaintext.to_vec();
        ciphertext.reserve(TAG_LENGTH);

        sealing_key
            .seal_in_place_append_tag(nonce, Aad::empty(), &mut ciphertext)
            .map_err(|_| SseError::EncryptionFailed("Encryption operation failed".into()))?;

        Ok((ciphertext, nonce_bytes))
    }

    /// Decrypt data with AES-256-GCM
    ///
    /// The ciphertext should include the authentication tag (appended during encryption).
    pub fn decrypt(
        &self,
        ciphertext: &[u8],
        nonce: &[u8; NONCE_LENGTH],
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Vec<u8>, SseError> {
        if ciphertext.len() < TAG_LENGTH {
            return Err(SseError::DecryptionFailed("Ciphertext too short".into()));
        }

        // Derive per-object key
        let derived_key = self.derive_key(bucket, key, version_id)?;

        // Create AEAD key
        let unbound_key = UnboundKey::new(&AES_256_GCM, &derived_key)
            .map_err(|_| SseError::DecryptionFailed("Failed to create decryption key".into()))?;
        let opening_key = LessSafeKey::new(unbound_key);

        // Decrypt in place
        let mut plaintext = ciphertext.to_vec();
        let nonce = Nonce::assume_unique_for_key(*nonce);

        let decrypted = opening_key
            .open_in_place(nonce, Aad::empty(), &mut plaintext)
            .map_err(|_| SseError::DecryptionFailed("Authentication failed - data may be corrupted".into()))?;

        Ok(decrypted.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_master_key() -> Vec<u8> {
        // 32-byte test key
        vec![
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        ]
    }

    #[test]
    fn test_sse_context_creation() {
        let key = test_master_key();
        let ctx = SseContext::new(key);
        assert!(ctx.is_ok());
    }

    #[test]
    fn test_sse_context_invalid_key_length() {
        // Too short
        let short_key = vec![0u8; 16];
        assert!(matches!(SseContext::new(short_key), Err(SseError::InvalidMasterKey)));

        // Too long
        let long_key = vec![0u8; 64];
        assert!(matches!(SseContext::new(long_key), Err(SseError::InvalidMasterKey)));
    }

    #[test]
    fn test_generate_nonce() {
        let ctx = SseContext::new(test_master_key()).unwrap();

        let nonce1 = ctx.generate_nonce().unwrap();
        let nonce2 = ctx.generate_nonce().unwrap();

        // Nonces should be 12 bytes
        assert_eq!(nonce1.len(), NONCE_LENGTH);
        assert_eq!(nonce2.len(), NONCE_LENGTH);

        // Nonces should be different (with high probability)
        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let ctx = SseContext::new(test_master_key()).unwrap();
        let plaintext = b"Hello, this is secret data!";
        let bucket = "test-bucket";
        let key = "secret-file.txt";
        let version_id = "v1";

        // Encrypt
        let (ciphertext, nonce) = ctx.encrypt(plaintext, bucket, key, version_id).unwrap();

        // Ciphertext should be different from plaintext
        assert_ne!(&ciphertext[..plaintext.len()], plaintext);

        // Ciphertext should include tag (16 bytes longer)
        assert_eq!(ciphertext.len(), plaintext.len() + TAG_LENGTH);

        // Decrypt
        let decrypted = ctx.decrypt(&ciphertext, &nonce, bucket, key, version_id).unwrap();

        // Should match original
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_objects_different_ciphertext() {
        let ctx = SseContext::new(test_master_key()).unwrap();
        let plaintext = b"Same content";

        // Encrypt same content for two different objects
        let (ct1, _) = ctx.encrypt(plaintext, "bucket1", "key1", "v1").unwrap();
        let (ct2, _) = ctx.encrypt(plaintext, "bucket2", "key2", "v1").unwrap();

        // Ciphertexts should be different (different derived keys)
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn test_decrypt_with_wrong_context() {
        let ctx1 = SseContext::new(test_master_key()).unwrap();
        let mut key2 = test_master_key();
        key2[0] = 0xff; // Different key
        let ctx2 = SseContext::new(key2).unwrap();

        let plaintext = b"Secret message";
        let bucket = "bucket";
        let key = "key";
        let version_id = "v1";

        // Encrypt with ctx1
        let (ciphertext, nonce) = ctx1.encrypt(plaintext, bucket, key, version_id).unwrap();

        // Try to decrypt with ctx2 (different master key)
        let result = ctx2.decrypt(&ciphertext, &nonce, bucket, key, version_id);
        assert!(result.is_err());
        assert!(matches!(result, Err(SseError::DecryptionFailed(_))));
    }

    #[test]
    fn test_decrypt_wrong_bucket() {
        let ctx = SseContext::new(test_master_key()).unwrap();
        let plaintext = b"Secret";

        let (ciphertext, nonce) = ctx.encrypt(plaintext, "bucket1", "key", "v1").unwrap();

        // Try to decrypt with different bucket name
        let result = ctx.decrypt(&ciphertext, &nonce, "bucket2", "key", "v1");
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_tampered_ciphertext() {
        let ctx = SseContext::new(test_master_key()).unwrap();
        let plaintext = b"Important data";

        let (mut ciphertext, nonce) = ctx.encrypt(plaintext, "bucket", "key", "v1").unwrap();

        // Tamper with ciphertext
        if !ciphertext.is_empty() {
            ciphertext[0] ^= 0xff;
        }

        // Decryption should fail authentication
        let result = ctx.decrypt(&ciphertext, &nonce, "bucket", "key", "v1");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_plaintext() {
        let ctx = SseContext::new(test_master_key()).unwrap();
        let plaintext = b"";

        let (ciphertext, nonce) = ctx.encrypt(plaintext, "bucket", "key", "v1").unwrap();

        // Should only contain the tag
        assert_eq!(ciphertext.len(), TAG_LENGTH);

        let decrypted = ctx.decrypt(&ciphertext, &nonce, "bucket", "key", "v1").unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_large_plaintext() {
        let ctx = SseContext::new(test_master_key()).unwrap();
        let plaintext: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let (ciphertext, nonce) = ctx.encrypt(&plaintext, "bucket", "key", "v1").unwrap();
        let decrypted = ctx.decrypt(&ciphertext, &nonce, "bucket", "key", "v1").unwrap();

        assert_eq!(decrypted, plaintext);
    }
}
