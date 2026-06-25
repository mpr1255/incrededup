//! Quick diagnostic tool to compare two documents in an LSH index

use anyhow::Result;
use incrededup::lsh::DiskLSH;
use incrededup::minhash::{compute_band_hashes, jaccard_from_signatures, NUM_BANDS};
use std::env;
use uuid::Uuid;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: compare_docs <lsh.redb path> <uuid1> <uuid2>");
        std::process::exit(1);
    }

    let lsh_path = &args[1];
    let uuid1 = Uuid::parse_str(&args[2])?;
    let uuid2 = Uuid::parse_str(&args[3])?;

    println!("Opening LSH index: {}", lsh_path);
    let lsh = DiskLSH::open(lsh_path)?;

    // Get documents
    let doc1 = lsh
        .get_document(&uuid1)?
        .ok_or_else(|| anyhow::anyhow!("Document {} not found in index", uuid1))?;
    let doc2 = lsh
        .get_document(&uuid2)?
        .ok_or_else(|| anyhow::anyhow!("Document {} not found in index", uuid2))?;

    println!("\nDocument 1: {}", uuid1);
    println!("  Content length: {}", doc1.content_len);
    println!("  Signature length: {}", doc1.signature.len());

    println!("\nDocument 2: {}", uuid2);
    println!("  Content length: {}", doc2.content_len);
    println!("  Signature length: {}", doc2.signature.len());

    // Compute Jaccard
    let jaccard = jaccard_from_signatures(&doc1.signature, &doc2.signature);
    println!("\n=== JACCARD SIMILARITY: {:.4} ===", jaccard);

    // Compute band hashes and find shared bands
    let bands1 = compute_band_hashes(&doc1.signature);
    let bands2 = compute_band_hashes(&doc2.signature);

    let mut shared_bands = 0;
    println!("\nBand comparison:");
    for i in 0..NUM_BANDS {
        let match_symbol = if bands1[i] == bands2[i] {
            shared_bands += 1;
            "MATCH"
        } else {
            "differ"
        };
        println!(
            "  Band {:2}: {:20} vs {:20} - {}",
            i, bands1[i], bands2[i], match_symbol
        );
    }

    println!(
        "\nShared bands: {} / {} ({:.1}%)",
        shared_bands,
        NUM_BANDS,
        shared_bands as f64 / NUM_BANDS as f64 * 100.0
    );

    // Check if they should have been candidates
    if shared_bands >= 1 {
        println!(
            "\nThese documents SHOULD have been LSH candidates (share {} bands)",
            shared_bands
        );
    } else {
        println!("\nThese documents would NOT be LSH candidates (no shared bands)");
    }

    // Check if Jaccard meets threshold
    if jaccard >= 0.8 {
        println!(
            "Jaccard {:.4} >= 0.8 threshold - should be marked as duplicates",
            jaccard
        );
    } else {
        println!(
            "Jaccard {:.4} < 0.8 threshold - should NOT be duplicates",
            jaccard
        );
    }

    // Size difference check
    let size_diff_ratio = if doc1.content_len > doc2.content_len {
        1.0 - (doc2.content_len as f64 / doc1.content_len as f64)
    } else {
        1.0 - (doc1.content_len as f64 / doc2.content_len as f64)
    };

    println!("\nSize difference ratio: {:.4}", size_diff_ratio);
    if size_diff_ratio > 0.3 {
        println!(
            "Size difference {:.1}% exceeds 30% threshold - might have been filtered",
            size_diff_ratio * 100.0
        );
    }

    Ok(())
}
