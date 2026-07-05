//! Text normalization and tokenization for MinHash signatures.

use crate::minhash::{RMinHash, NUM_PERM};

pub const TOKENIZER_VERSION: &str = "unicode_char_3gram_v2";

pub const DEFAULT_CHAR_NGRAM_WIDTH: usize = 3;
const BYTE_UPDATE_BATCH_SIZE: usize = 256;

/// Normalize text for language-agnostic near-duplicate detection.
///
/// Letters and numbers are lowercased and preserved. All other character runs
/// become one ASCII space so word boundaries still matter for Latin text while
/// unsegmented CJK text can be compared by overlapping character n-grams.
#[must_use]
pub fn normalize_content(content: &str) -> String {
    let mut normalized = String::with_capacity(content.len());
    let mut last_was_space = true;

    for ch in content.chars().flat_map(char::to_lowercase) {
        if ch.is_alphanumeric() {
            normalized.push(ch);
            last_was_space = false;
        } else if !last_was_space {
            normalized.push(' ');
            last_was_space = true;
        }
    }

    if normalized.ends_with(' ') {
        normalized.pop();
    }

    normalized
}

/// Tokenize document content into overlapping Unicode character 3-grams.
///
/// This intentionally does not depend on whitespace segmentation. Chinese,
/// Japanese, Korean, and mixed-language documents are all handled by the same
/// path.
#[must_use]
pub fn tokenize(content: &str) -> Vec<String> {
    tokenize_with_ngram_width(content, DEFAULT_CHAR_NGRAM_WIDTH)
}

/// Tokenize document content into overlapping Unicode character n-grams.
#[must_use]
pub fn tokenize_with_ngram_width(content: &str, width: usize) -> Vec<String> {
    assert!(width > 0, "ngram width must be greater than zero");

    let normalized = normalize_content(content);
    let Some(char_offsets) = char_offsets(&normalized) else {
        return Vec::new();
    };

    let char_count = char_offsets.len() - 1;
    if char_count <= width {
        return vec![normalized];
    }

    let mut tokens = Vec::with_capacity(char_count - width + 1);
    for idx in 0..=(char_count - width) {
        let start = char_offsets[idx];
        let end = char_offsets[idx + width];
        tokens.push(normalized[start..end].to_string());
    }
    tokens
}

/// Compute MinHash signature for a document using the production tokenizer.
#[must_use]
pub fn compute_signature(content: &str, seed: u64) -> Vec<u32> {
    compute_signature_with_ngram_width(content, seed, DEFAULT_CHAR_NGRAM_WIDTH)
}

/// Compute MinHash signature for a document using a specific n-gram width.
#[must_use]
pub fn compute_signature_with_ngram_width(content: &str, seed: u64, width: usize) -> Vec<u32> {
    assert!(width > 0, "ngram width must be greater than zero");

    let normalized = normalize_content(content);
    let mut minhash = RMinHash::new(NUM_PERM, seed);

    let Some(char_offsets) = char_offsets(&normalized) else {
        minhash.update_bytes(&[content.as_bytes()]);
        return minhash.digest_owned();
    };

    let char_count = char_offsets.len() - 1;
    if char_count <= width {
        minhash.update_bytes(&[normalized.as_bytes()]);
        return minhash.digest_owned();
    }

    let bytes = normalized.as_bytes();
    let mut batch: Vec<&[u8]> = Vec::with_capacity(BYTE_UPDATE_BATCH_SIZE);
    for idx in 0..=(char_count - width) {
        let start = char_offsets[idx];
        let end = char_offsets[idx + width];
        batch.push(&bytes[start..end]);

        if batch.len() == BYTE_UPDATE_BATCH_SIZE {
            minhash.update_bytes(&batch);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        minhash.update_bytes(&batch);
    }

    minhash.digest_owned()
}

fn char_offsets(text: &str) -> Option<Vec<usize>> {
    if text.is_empty() {
        return None;
    }

    let mut offsets: Vec<usize> = text.char_indices().map(|(idx, _)| idx).collect();
    offsets.push(text.len());
    Some(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_unsegmented_chinese_into_many_ngrams() {
        let tokens = tokenize("新华社北京七月四日电国务院发布新的通知");

        assert!(
            tokens.len() > 10,
            "unsegmented Chinese should produce overlapping n-grams, got {tokens:?}"
        );
        assert!(tokens.iter().any(|token| token == "新华社"));
    }

    #[test]
    fn normalization_is_case_and_punctuation_insensitive() {
        assert_eq!(
            normalize_content("Hello, WORLD!! 新华社。"),
            "hello world 新华社"
        );
    }

    #[test]
    fn signature_is_deterministic() {
        let content = "新华社北京七月四日电国务院发布新的通知";
        assert_eq!(
            compute_signature(content, 42),
            compute_signature(content, 42)
        );
    }
}
