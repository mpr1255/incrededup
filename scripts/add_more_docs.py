#!/usr/bin/env python3
"""
Add 10k more documents to existing SQLite database.

Usage:
    python3 scripts/add_more_docs.py /tmp/fake.sqlite
"""

import sqlite3
import uuid
import sys

def generate_base_content(group_id: int) -> str:
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
elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Document
group {group_id} continues with more {topic} discussion."""

def generate_similar_content(group_id: int, variation: int) -> str:
    base = generate_base_content(group_id)
    return f"{base}\n\nVariation {variation} of group {group_id}. Minor addendum {variation}."

def generate_unique_content(idx: int) -> str:
    categories = ["recipe", "travel", "review", "tutorial", "memoir"]
    category = categories[idx % len(categories)]
    return f"""Unique document {idx} - Category: {category}
This is an entirely distinct piece of content. It discusses {category} topics with
unique terminology: zeta-{idx} theta-{idx} iota-{idx} kappa-{idx}.
Document identifier: UNIQUE_{idx:05d}. Should NOT match other documents."""

def add_documents(db_path: str, num_groups: int = 2000, docs_per_group: int = 4, num_unique: int = 2000, offset: int = 10000):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get current count
    cursor.execute("SELECT COUNT(*) FROM documents")
    before = cursor.fetchone()[0]
    print(f"Current documents: {before}")

    total_to_add = num_groups * docs_per_group + num_unique
    print(f"Adding {total_to_add} more documents...")
    print()

    # Insert similar document groups (with offset to make them distinct)
    print("Inserting similar document groups...")
    for group_id in range(offset, offset + num_groups):
        for variation in range(docs_per_group):
            doc_id = str(uuid.uuid4())
            content = generate_similar_content(group_id, variation)
            filename = f"group_{group_id:05d}_var_{variation}.txt"
            cursor.execute(
                "INSERT INTO documents (id, content, content_len, filename, is_parent) VALUES (?, ?, ?, ?, NULL)",
                (doc_id, content, len(content), filename)
            )
        if (group_id - offset + 1) % 500 == 0:
            print(f"  ... {group_id - offset + 1}/{num_groups} groups")
            conn.commit()
    conn.commit()

    # Insert unique documents
    print("Inserting unique documents...")
    for idx in range(offset, offset + num_unique):
        doc_id = str(uuid.uuid4())
        content = generate_unique_content(idx)
        filename = f"unique_{idx:05d}.txt"
        cursor.execute(
            "INSERT INTO documents (id, content, content_len, filename, is_parent) VALUES (?, ?, ?, ?, NULL)",
            (doc_id, content, len(content), filename)
        )
        if (idx - offset + 1) % 500 == 0:
            conn.commit()
    conn.commit()

    # Verify
    cursor.execute("SELECT COUNT(*) FROM documents")
    after = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM documents WHERE is_parent IS NULL")
    unprocessed = cursor.fetchone()[0]

    print()
    print(f"Documents before: {before}")
    print(f"Documents after: {after}")
    print(f"Added: {after - before}")
    print(f"Unprocessed (is_parent IS NULL): {unprocessed}")

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 add_more_docs.py <database.sqlite>")
        sys.exit(1)
    add_documents(sys.argv[1])
