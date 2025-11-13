#!/usr/bin/env python3


import os
import sys
import csv
import sqlite3
from pathlib import Path
from itertools import islice

from tqdm import tqdm


telefonbuch_columns = {
    "name0": str,
    "firstname0": str,
    "nameextension0": str,
    "profession0": str,
    "nameconnection1": str,
    "name1": str,
    "firstname1": str,
    "nameextension1": str,
    "profession1": str,
    "nameconnection2": str,
    "name2": str,
    "firstname2": str,
    "nameextension2": str,
    "profession2": str,
    "extendedtext": str,
    "street": str,
    "housenumber": str,
    "zipcode": str,
    "city": str,
    "areacode": str,
    "phonenumber": str,
    "callrate": str,
    "commercial": bool,
    "webadress": bool,
    "advertising": bool,
    # "recordtype": ("single", "parent", "child"), # enum
    "recordtype_int": int, # 0=single 1=parent 2=child
}


sqlite_col_type_map = {
    str: "TEXT",
    int: "INTEGER",
    bool: "BOOLEAN",
}


def csv_to_sqlite(csv_path, sqlite_path, table_name="records", batch_size=10000):

    if os.path.exists(sqlite_path):
        print(f"error: output exists: {sqlite_path}")
        sys.exit(1)

    csv_path = Path(csv_path)
    sqlite_path = Path(sqlite_path)

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    # Lock the database exclusively
    cur.execute("BEGIN EXCLUSIVE")

    # Estimate total rows using file size
    bytes_per_row = 85.37834685553358
    estimated_rows = int(csv_path.stat().st_size / bytes_per_row)

    # Read CSV header
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)

        # Create SQLite table
        columns = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "parent_id INTEGER"
        ]
        for col in header:
            col_type = telefonbuch_columns[col]
            sqlite_col_type = sqlite_col_type_map[col_type]
            columns.append(f'{col} {sqlite_col_type}')

        batch_row_types = tuple(telefonbuch_columns[col] for col in header)

        # cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(f"CREATE TABLE {table_name} (\n  {',\n  '.join(columns)}\n)")

        insert_columns = ["parent_id"] + header
        placeholders = ",".join(["?"] * len(insert_columns))
        recordtype_index = header.index("recordtype_int")

        next_rowid = 1
        last_parent_rowid = None
        total_inserted = 0

        def process_batch(batch_rows, start_rowid, last_parent):
            """Prepare tuples with parent_id predictions."""
            data = []
            next_rowid = start_rowid
            current_parent = last_parent

            for row in batch_rows:
                record_type = int(row[recordtype_index])

                if record_type == 1:  # parent
                    parent_id = None
                    current_parent = next_rowid
                elif record_type == 2:  # child
                    parent_id = current_parent
                else:  # single
                    parent_id = None

                for idx, val in enumerate(row):
                    _type = batch_row_types[idx]
                    if _type == str:
                        continue
                    if _type == bool:
                        row[idx] = True if val == "1" else False
                        continue
                    if _type == int:
                        row[idx] = int(val)
                        continue

                data.append([parent_id] + row)
                next_rowid += 1

            return data, next_rowid, current_parent

        # tqdm progress bar
        pbar = tqdm(total=estimated_rows, unit="rows", ncols=80)

        while True:
            batch = list(islice(reader, batch_size))
            if not batch:
                break

            data_to_insert, next_rowid, last_parent_rowid = process_batch(
                batch, next_rowid, last_parent_rowid
            )

            cur.executemany(
                f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
                data_to_insert,
            )

            total_inserted += len(batch)
            pbar.update(len(batch))

            # break # debug: stop after first batch

        conn.commit()
        pbar.close()
        print(f"done {sqlite_path.name} with {total_inserted:_} rows")

    conn.close()


if __name__ == "__main__":

    csv_file = "telefonbuch.csv"
    sqlite_file = "telefonbuch.db"
    table = "telefonbuch"
    batch_size = 10_000

    csv_to_sqlite(csv_file, sqlite_file, table, batch_size)
