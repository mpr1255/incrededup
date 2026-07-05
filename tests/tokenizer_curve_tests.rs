//! Oracle tests for the tokenizer similarity-curve harness.

use incrededup::{
    compute_signature_with_ngram_width, jaccard_from_signatures, tokenize_with_ngram_width,
};
use std::collections::HashSet;

const SEED: u64 = 42;

#[test]
fn exact_ngram_jaccard_matches_spread_edit_formula() {
    let width = 3;
    let original = unique_cjk_string(400, 0x4e00);
    let positions: Vec<usize> = (20..220).step_by(20).collect();
    let edited = replace_positions(&original, &positions, 0x8000);

    let exact = exact_token_jaccard(&original, &edited, width);
    let token_count = original.chars().count() - width + 1;
    let changed_ngrams = positions.len() * width;
    let expected = (token_count - changed_ngrams) as f64 / (token_count + changed_ngrams) as f64;

    assert!(
        (exact - expected).abs() < 1e-12,
        "exact token Jaccard should match the known spread-edit formula; exact={exact:.6}, expected={expected:.6}"
    );
}

#[test]
fn minhash_estimate_tracks_exact_ngram_jaccard() {
    let width = 3;
    let original = unique_cjk_string(1_000, 0x4e00);
    let positions: Vec<usize> = (25..325).step_by(25).collect();
    let edited = replace_positions(&original, &positions, 0x8000);

    let exact = exact_token_jaccard(&original, &edited, width);
    let estimated = minhash_jaccard(&original, &edited, width);

    assert!(
        (estimated - exact).abs() <= 0.12,
        "MinHash estimate should stay close enough to exact token Jaccard for curve thresholds; exact={exact:.3}, estimated={estimated:.3}"
    );
}

#[test]
fn production_width_passes_three_percent_spread_edits() {
    let original = unique_cjk_string(1_000, 0x4e00);
    let positions: Vec<usize> = (16..496).step_by(16).collect();
    let edited = replace_positions(&original, &positions, 0x8000);

    let estimated = minhash_jaccard(&original, &edited, 3);

    assert!(
        estimated >= 0.8,
        "3-grams should keep 3 percent spread edits above the production threshold; estimated={estimated:.3}"
    );
}

#[test]
fn five_grams_fail_the_same_spread_edit_threshold() {
    let original = unique_cjk_string(1_000, 0x4e00);
    let positions: Vec<usize> = (16..496).step_by(16).collect();
    let edited = replace_positions(&original, &positions, 0x8000);

    let estimated = minhash_jaccard(&original, &edited, 5);

    assert!(
        estimated < 0.8,
        "5-grams should fail this validated spread-edit threshold; estimated={estimated:.3}"
    );
}

fn exact_token_jaccard(left: &str, right: &str, width: usize) -> f64 {
    let left_tokens: HashSet<String> = tokenize_with_ngram_width(left, width).into_iter().collect();
    let right_tokens: HashSet<String> = tokenize_with_ngram_width(right, width)
        .into_iter()
        .collect();

    let intersection = left_tokens.intersection(&right_tokens).count();
    let union = left_tokens.union(&right_tokens).count();
    intersection as f64 / union as f64
}

fn minhash_jaccard(left: &str, right: &str, width: usize) -> f64 {
    let left_signature = compute_signature_with_ngram_width(left, SEED, width);
    let right_signature = compute_signature_with_ngram_width(right, SEED, width);
    jaccard_from_signatures(&left_signature, &right_signature)
}

fn unique_cjk_string(len: usize, start_codepoint: u32) -> String {
    (0..len)
        .map(|idx| {
            char::from_u32(start_codepoint + idx as u32)
                .expect("test range should contain valid CJK scalar values")
        })
        .collect()
}

fn replace_positions(text: &str, positions: &[usize], replacement_start: u32) -> String {
    let mut chars: Vec<char> = text.chars().collect();
    for (replacement_idx, position) in positions.iter().enumerate() {
        chars[*position] = char::from_u32(replacement_start + replacement_idx as u32)
            .expect("test replacement range should contain valid CJK scalar values");
    }
    chars.into_iter().collect()
}
