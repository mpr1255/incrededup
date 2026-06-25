#!/usr/bin/env python3
"""
Generate a fake SQLite database with 10k documents for testing deduplication.

Creates:
- 8000 documents in 2000 groups of 4 similar docs (should find duplicates)
- 2000 unique documents (should not match)

Usage:
    python3 scripts/generate_fake_sqlite.py /tmp/fake.sqlite
"""

import sqlite3
import uuid
import sys
import random

def generate_base_content(group_id: int) -> str:
    """Generate base content for a group of similar documents."""
    topics = ["technology", "science", "politics", "sports", "entertainment",
              "health", "business", "education", "environment", "culture"]
    topic = topics[group_id % len(topics)]

    return f"""This is document group {group_id} discussing {topic} with substantial content
for MinHash signature generation. The article covers various aspects of {topic} including
recent developments, historical context, and future implications. Key terms include
alpha-{group_id} beta-{group_id} gamma-{group_id} delta-{group_id} epsilon-{group_id}.

The {topic} sector has seen significant changes in recent years. Experts in the field
of {topic} have noted several emerging trends. This document provides comprehensive
analysis of {topic} related matters. Group identifier: GROUP_{group_id:05d}.

Additional context about {topic}: Lorem ipsum dolor sit amet, consectetur adipiscing
elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad
minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat. Document group {group_id} continues with more {topic} discussion."""


def generate_similar_content(group_id: int, variation: int) -> str:
    """Generate content similar to base (slight variations)."""
    base = generate_base_content(group_id)
    # Add small variation - keeps high similarity
    return f"{base}\n\nVariation {variation} of group {group_id}. Minor addendum {variation}."


def generate_unique_content(idx: int) -> str:
    """Generate completely different content for unique documents."""
    categories = ["recipe", "travel", "review", "tutorial", "memoir",
                  "poetry", "news", "analysis", "interview", "opinion"]
    category = categories[idx % len(categories)]

    return f"""Unique document {idx} - Category: {category}

This is an entirely distinct piece of content with no similarity to any grouped documents.
It discusses {category} topics with unique terminology: zeta-{idx} theta-{idx}
iota-{idx} kappa-{idx} lambda-{idx} mu-{idx}.

The {category} content here uses completely different vocabulary and structure.
Document identifier: UNIQUE_{idx:05d}. This should NOT match any other documents.

Additional unique content for document {idx}: The quick brown fox jumps over the lazy
dog. Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump.
Sphinx of black quartz, judge my vow. Unique suffix: {idx * 17} {idx * 31} {idx * 47}."""


def create_database(db_path: str, num_groups: int = 2000, docs_per_group: int = 4,
                    num_unique: int = 2000):
    """Create SQLite database with fake documents."""

    print(f"Creating database: {db_path}")
    print(f"  - {num_groups} groups x {docs_per_group} similar docs = {num_groups * docs_per_group}")
    print(f"  - {num_unique} unique docs")
    print(f"  - Total: {num_groups * docs_per_group + num_unique} documents")
    print()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create schema (matching our SqliteSource schema)
    cursor.executescript("""
        DROP TABLE IF EXISTS documents;
        DROP TABLE IF EXISTS dupes;

        CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            content_len INTEGER NOT NULL,
            filename TEXT,
            is_parent INTEGER
        );

        CREATE TABLE dupes (
            child_id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL,
            jaccard_similarity REAL NOT NULL,
            size_difference INTEGER NOT NULL,
            size_difference_pct REAL NOT NULL
        );

        CREATE INDEX idx_documents_is_parent ON documents(is_parent);
    """)

    # Insert similar document groups
    print("Inserting similar document groups...")
    similar_count = 0
    for group_id in range(num_groups):
        for variation in range(docs_per_group):
            doc_id = str(uuid.uuid4())
            content = generate_similar_content(group_id, variation)
            filename = f"group_{group_id:05d}_var_{variation}.txt"

            cursor.execute(
                "INSERT INTO documents (id, content, content_len, filename, is_parent) VALUES (?, ?, ?, ?, NULL)",
                (doc_id, content, len(content), filename)
            )
            similar_count += 1

        if (group_id + 1) % 500 == 0:
            print(f"  ... {group_id + 1}/{num_groups} groups")
            conn.commit()

    conn.commit()
    print(f"  Inserted {similar_count} similar documents")

    # Insert unique documents
    print("Inserting unique documents...")
    for idx in range(num_unique):
        doc_id = str(uuid.uuid4())
        content = generate_unique_content(idx)
        filename = f"unique_{idx:05d}.txt"

        cursor.execute(
            "INSERT INTO documents (id, content, content_len, filename, is_parent) VALUES (?, ?, ?, ?, NULL)",
            (doc_id, content, len(content), filename)
        )

        if (idx + 1) % 500 == 0:
            print(f"  ... {idx + 1}/{num_unique} unique docs")
            conn.commit()

    conn.commit()
    print(f"  Inserted {num_unique} unique documents")

    # Verify
    cursor.execute("SELECT COUNT(*) FROM documents")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM documents WHERE is_parent IS NULL")
    unprocessed = cursor.fetchone()[0]

    print()
    print(f"Database created successfully!")
    print(f"  Total documents: {total}")
    print(f"  Unprocessed (is_parent IS NULL): {unprocessed}")
    print(f"  File size: {sys.getsizeof(db_path)} bytes")

    conn.close()

    # Show file size
    import os
    size_kb = os.path.getsize(db_path) / 1024
    print(f"  Database file: {size_kb:.1f} KB")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 generate_fake_sqlite.py <output.sqlite>")
        print("Example: python3 generate_fake_sqlite.py /tmp/fake.sqlite")
        sys.exit(1)

    db_path = sys.argv[1]
    create_database(db_path)

    print()
    print("Now run deduplication with:")
    print(f"  ./target/release/incrededup --sqlite {db_path} --data-dir /tmp/ourtest --min-content-len 100")
