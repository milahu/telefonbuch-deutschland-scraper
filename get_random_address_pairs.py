#!/usr/bin/env python3

import sqlite3
from collections import defaultdict
import random

def format_address(row, delimiter="\n"):
    lines = [
        row["firstname0"] + " " + row["name0"],
        row["street"] + " " + row["housenumber"],
        row["zipcode"] + " " + row["city"],
    ]
    if delimiter is None:
        return lines
    return delimiter.join(lines)

def fetch_random_entries(conn, limit, TABLE_NAME, WHERE_CLAUSE):
    """Fetch a batch of random entries from the database."""
    query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE {WHERE_CLAUSE}
        ORDER BY RANDOM()
        LIMIT ?
    """
    return conn.execute(query, (limit,)).fetchall()

def generate_pairs(entries, ZIP_COLUMN):
    """
    Generate pairs of addresses that share the same zipcode prefix.
    """
    prefix_map = defaultdict(list)
    for entry in entries:
        prefix = entry[ZIP_COLUMN][:2]
        prefix_map[prefix].append(entry)

    pairs = []
    for group in prefix_map.values():
        # take pairs sequentially
        while len(group) >= 2:
            pair = (group.pop(0), group.pop(0))
            pairs.append(pair)
    return pairs

DEFAULT_WHERE_CLAUSE = " AND ".join([
    # exclude child entries
    "recordtype_int IN (0, 1)",
    # exclude commercial entries
    "commercial = FALSE",
    "firstname0 != ''",
    # exclude abbreviated firstnames like "A."
    "LENGTH(firstname0) > 2",
    "name0 != ''",
    "street != ''",
    "housenumber != ''",
    "zipcode != ''",
    "city != ''",
])

def get_random_address_pairs(
        DB_PATH = "telefonbuch.db",
        TABLE_NAME = "telefonbuch",
        ZIP_COLUMN = "zipcode",
        SAMPLE_SIZE = 1000,   # number of random entries to fetch per batch
        PAIR_COUNT = 100,     # number of pairs to generate
        WHERE_CLAUSE = DEFAULT_WHERE_CLAUSE,
    ):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    all_pairs = []

    # keep fetching random samples until we have enough pairs
    while len(all_pairs) < PAIR_COUNT:
        entries = fetch_random_entries(conn, SAMPLE_SIZE, TABLE_NAME, WHERE_CLAUSE)
        if not entries:
            break  # no more data

        pairs = generate_pairs(entries, ZIP_COLUMN)
        random.shuffle(pairs)
        all_pairs.extend(pairs)

    # trim to the desired number of pairs
    all_pairs = all_pairs[:PAIR_COUNT]

    conn.close()

    return all_pairs

def main():

    all_pairs = get_random_address_pairs()

    # output
    for i, (a, b) in enumerate(all_pairs, 1):
        print(format_address(a, ", "), "->", format_address(b, ", "))

    conn.close()

if __name__ == "__main__":
    main()
