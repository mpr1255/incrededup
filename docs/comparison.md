# Comparison and matching behavior

This page describes the comparator contract and how `incrededup` differs from
near-duplicate batch tools.

## Comparator contract

Duplicate detection is based on text similarity after a small, fixed
normalization step:

1. Content is split with Rust `split_whitespace()`.
2. Tokens with length 1 are ignored.
3. If at least three tokens remain, signatures are built from 3-word shingles.
   If fewer than three remain, the remaining tokens are used directly.
4. MinHash uses 128 permutations and the configured seed.
5. LSH uses 16 bands with 8 rows per band.
6. Candidate pairs are rejected when `abs(len_a - len_b) / max(len_a, len_b)`
   is greater than `--size-diff`. The default is `0.3`.
7. Candidate pairs are duplicates when signature Jaccard similarity is at least
   `--threshold`. The default is `0.8`.
8. The larger document is stored as the child. Ties use the document currently
   being processed as the child. Transitive sync later chooses the
   lexicographically smallest UUID in a component as the canonical parent.

The stored `jaccard_similarity` is the fraction of equal MinHash values across
the two signatures, not exact set Jaccard over source tokens.

## Related software

Most near-duplicate text tools use MinHash and LSH. The practical difference is
whether the index can live outside RAM, whether raw duplicate pairs are saved
for later transitive resolution, and whether new rows can be deduplicated
against an existing corpus without rebuilding everything.

Legend: yes, partial, no.

| Software | LSH | Disk index | Saves pairs | Incremental | DB writes |
|---|---:|---:|---:|---:|---:|
| incrededup | yes | yes | yes | yes | yes |
| [Duplodocus](https://github.com/allenai/duplodocus) | yes | yes | yes | no | no |
| [DataTrove](https://github.com/huggingface/datatrove) | yes | yes | yes | partial | no |
| [text-dedup](https://github.com/ChenghaoMou/text-dedup) | yes | no | no | no | no |
| [datasketch](https://github.com/ekzhu/datasketch) | yes | partial | no | partial | no |
| [Rensa](https://github.com/beowolx/rensa) | yes | no | no | no | no |

Duplodocus and DataTrove are the closest batch comparators. `datasketch` and
Rensa are useful building blocks, but the corpus pipeline is yours.
`text-dedup` is convenient for dataset cleanup, but its MinHash path is not
shaped as a disk-backed database service.

This does not mean every operation has tiny peak memory. A genuinely large
duplicate component still has to be resolved in Phase 3, and that connected
edge set can be several GB. The design goal is narrower: do not require the
full MinHash/LSH index or full historical match graph to fit in RAM just to
keep deduplicating a growing corpus.
