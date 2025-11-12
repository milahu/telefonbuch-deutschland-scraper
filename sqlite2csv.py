#!/usr/bin/env python3

import sqlite3
import csv
import tempfile
import os
import sys

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
    "housenumber": str, # int?
    "zipcode": str, # int?
    "city": str,
    "areacode": str, # int?
    "phonenumber": str, # int?
    "callrate": str,
    "commercial": bool,
    "webadress": bool,
    "advertising": bool,
    "recordtype": ("single", "parent", "child"),
}

bool_columns = set()
for col, _type in telefonbuch_columns.items():
    if _type == bool:
        bool_columns.add(col)


def escape_newlines(value: str) -> str:
    """Escape newlines with \\n for CSV stability."""
    if isinstance(value, str):
        return value.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    return value


def bool_to_int(key, value):
    """Convert booleans to integers."""
    if key in bool_columns:
        return 1 if value[0] == "t" else None
        return 1 if value[0] == "t" else 0
    return value
    # if isinstance(value, bool):
    #     return int(value)
    # if value in ("true", "True", "TRUE"):
    #     return 1
    # if value in ("false", "False", "FALSE"):
    #     return 0
    # return value


def serialize_payload(row, payload_cols):
    """Serialize payload columns into a single deterministic CSV string."""
    parts = []
    for c in payload_cols:
        v = bool_to_int(c, escape_newlines(row[c]))
        if v is None:
            s = ""
        else:
            s = str(v)
        if s == "0":
            s = "" # compress 0 to None
        # quote only when necessary
        if ";" in s or "\n" in s or '"' in s:
            s = '"' + s.replace('"', '""') + '"'
        parts.append(s)
    return ";".join(parts)


def deserialize_payload(payload_csv):
    """Split payload CSV line back into list of strings."""
    # use csv.reader to properly handle quotes
    reader = csv.reader([payload_csv], delimiter=";")
    return next(reader)


def recordtype_to_int(value: str) -> int:
    """Map recordtype: single/parent/child → 0/1/2"""
    if not value: return 0
    ch = value[0]
    if ch == "s": return 0
    if ch == "p": return 1
    if ch == "c": return 2
    return 0


def convert_sqlite_to_csv(source_db, source_table, output_csv):
    # tmp_fd, tmp_path = tempfile.mkstemp(suffix=".sqlite")
    # os.close(tmp_fd)
    tmp_path = "sqlite2csv.temp.db"
    print(f"[info] temporary DB: {tmp_path}")

    if os.path.exists(tmp_path):
        os.unlink(tmp_path)

    if os.path.exists(output_csv):
        os.unlink(output_csv)

    src = sqlite3.connect(source_db)
    src.row_factory = sqlite3.Row
    tmp = sqlite3.connect(tmp_path)
    tmp.row_factory = sqlite3.Row

    s_cur = src.cursor()
    t_cur = tmp.cursor()

    # get columns from source table
    s_cur.execute(f"PRAGMA table_info({source_table})")
    cols_info = s_cur.fetchall()
    if not cols_info:
        raise SystemExit(f"Table {source_table} not found or has no columns.")
    all_cols = [r["name"] for r in cols_info]

    # determine payload columns (exclude meta + ignored)
    ignored = {"query_name", "query_offset", "query_child_num", "recordtype"}
    meta = {"id", "parent_id"}
    payload_cols = [c for c in all_cols if c not in ignored and c not in meta]

    print(f"[info] payload columns ({len(payload_cols)}): {payload_cols}")

    # count rows for progress
    s_cur.execute(f"SELECT COUNT(1) FROM {source_table}")
    num_source_rows = s_cur.fetchone()[0]
    print(f"[info] source rows: {num_source_rows:,}")

    # ------------------------
    # Stage 1: copy rows to temp with serialized payload and numeric recordtype
    # ------------------------
    t_cur.execute(
        "CREATE TABLE temp (recordtype INTEGER, id INTEGER, parent_id INTEGER, payload_csv TEXT)"
    )

    # s_cur.execute(f"SELECT * FROM {source_table}")
    s_cur.execute(f"SELECT * FROM {source_table} LIMIT 100000") # debug

    batch = []
    batch_size = 5000
    inserted = 0
    with tqdm(total=num_source_rows, unit="rows", ncols=80, desc="Stage 1 - copy") as pbar:
        for row in s_cur:
            rt = recordtype_to_int(row["recordtype"]) if "recordtype" in row.keys() else 0
            payload_csv = serialize_payload(row, payload_cols)
            batch.append((rt, row["id"], row["parent_id"], payload_csv))
            if len(batch) >= batch_size:
                t_cur.executemany("INSERT INTO temp VALUES (?,?,?,?)", batch)
                tmp.commit()
                inserted += len(batch)
                pbar.update(len(batch))
                batch.clear()
        if batch:
            t_cur.executemany("INSERT INTO temp VALUES (?,?,?,?)", batch)
            tmp.commit()
            inserted += len(batch)
            pbar.update(len(batch))
            batch.clear()

    print(f"[info] inserted into temp: {inserted:,}")

    print("creating index idx_temp_parent_id")
    t_cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_parent_id ON temp(parent_id)")
    tmp.commit()

    # slow?
    print("creating index idx_temp_payload")
    t_cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_payload ON temp(payload_csv)")
    tmp.commit()

    # sanity counts
    t_cur.execute("SELECT COUNT(1) FROM temp WHERE recordtype = 1")
    total_parents_raw = t_cur.fetchone()[0]
    t_cur.execute("SELECT COUNT(1) FROM temp WHERE recordtype = 2")
    total_children_raw = t_cur.fetchone()[0]
    t_cur.execute("SELECT COUNT(1) FROM temp WHERE recordtype = 0")
    total_singles_raw = t_cur.fetchone()[0]
    print(
        f"[info] raw counts - parents: {total_parents_raw:,}, children: {total_children_raw:,}, singles: {total_singles_raw:,}"
    )

    # ------------------------
    # Stage 2: Build parent_map (id -> parent_payload) from ALL parent rows
    # ------------------------
    print("[info] building parent_map (id -> parent_payload) ...")
    t_cur.execute("CREATE TABLE parent_map (id INTEGER PRIMARY KEY, parent_payload TEXT)")
    # Insert in batches to allow progress
    t_cur.execute("SELECT COUNT(1) FROM temp WHERE recordtype = 1")
    parent_rows_count = t_cur.fetchone()[0]
    with tqdm(total=parent_rows_count, unit="rows", ncols=80, desc="Stage 2 - parent_map") as pbar:
        cur = t_cur.execute("SELECT id, payload_csv FROM temp WHERE recordtype = 1")
        batch = []
        bs = 2000
        processed = 0
        for r in cur:
            batch.append((r["id"], r["payload_csv"]))
            if len(batch) >= bs:
                t_cur.executemany("INSERT INTO parent_map (id, parent_payload) VALUES (?,?)", batch)
                tmp.commit()
                processed += len(batch)
                pbar.update(len(batch))
                batch.clear()
        if batch:
            t_cur.executemany("INSERT INTO parent_map (id, parent_payload) VALUES (?,?)", batch)
            tmp.commit()
            processed += len(batch)
            pbar.update(len(batch))
            batch.clear()
    t_cur.execute("CREATE INDEX IF NOT EXISTS idx_parent_map_payload ON parent_map(parent_payload)")
    tmp.commit()
    t_cur.execute("SELECT COUNT(1) FROM parent_map")
    parent_map_count = t_cur.fetchone()[0]
    print(f"[info] parent_map entries: {parent_map_count:,}")

    # ------------------------
    # Stage 3: Deduplicate parents by payload_csv (unique parent payloads)
    # ------------------------
    print("[info] deduplicating parents by payload...")
    # parents: one unique row per parent_payload
    t_cur.execute(
        """
        CREATE TABLE parents AS
        SELECT MIN(id) AS id, parent_payload AS payload_csv
        FROM parent_map
        GROUP BY parent_payload
        ORDER BY parent_payload
        """
    )
    tmp.commit()
    t_cur.execute("CREATE INDEX IF NOT EXISTS idx_parents_payload ON parents(payload_csv)")
    tmp.commit()
    t_cur.execute("SELECT COUNT(1) FROM parents")
    unique_parents = t_cur.fetchone()[0]
    print(f"[info] unique parent payloads: {unique_parents:,}")

    # ------------------------
    # Stage 4: Deduplicate children within their parent's payload
    #   Join every child row to parent_map to get parent's payload,
    #   Group by (parent_payload, child_payload) to deduplicate.
    # ------------------------
    print("[info] deduplicating children (group by parent_payload + child_payload) ...")
    # count raw child rows for progress
    t_cur.execute("SELECT COUNT(1) FROM temp WHERE recordtype = 2")
    children_raw_count = t_cur.fetchone()[0]

    # create children table
    t_cur.execute(
        """
        CREATE TABLE children AS
        SELECT MIN(t.id) AS id,
               pm.parent_payload AS parent_payload,
               t.payload_csv AS payload_csv
        FROM temp t
        JOIN parent_map pm ON t.parent_id = pm.id
        WHERE t.recordtype = 2
        GROUP BY pm.parent_payload, t.payload_csv
        ORDER BY pm.parent_payload, t.payload_csv
        """
    )
    tmp.commit()
    t_cur.execute("CREATE INDEX IF NOT EXISTS idx_children_parent_payload ON children(parent_payload)")
    tmp.commit()
    t_cur.execute("SELECT COUNT(1) FROM children")
    unique_children = t_cur.fetchone()[0]
    print(
        f"[info] children: raw={children_raw_count:,}, unique_by_parent_payload={unique_children:,}"
    )

    # ------------------------
    # Stage 5: Deduplicate singles (recordtype == 0)
    # ------------------------
    print("[info] deduplicating singles ...")
    t_cur.execute(
        """
        CREATE TABLE singles AS
        SELECT MIN(id) AS id, payload_csv
        FROM temp
        WHERE recordtype = 0
        GROUP BY payload_csv
        ORDER BY payload_csv
        """
    )
    tmp.commit()
    t_cur.execute("SELECT COUNT(1) FROM singles")
    unique_singles = t_cur.fetchone()[0]
    print(f"[info] unique singles: {unique_singles:,}")

    # ------------------------
    # Stage 6: Write final CSV
    # ------------------------
    print("[info] writing CSV ...")
    # We'll calculate total for progress bar as total output rows estimate
    t_cur.execute("SELECT COUNT(1) FROM parents")
    cnt_parents_out = t_cur.fetchone()[0]
    t_cur.execute("SELECT COUNT(1) FROM children")
    cnt_children_out = t_cur.fetchone()[0]
    t_cur.execute("SELECT COUNT(1) FROM singles")
    cnt_singles_out = t_cur.fetchone()[0]
    total_out_rows = cnt_parents_out + cnt_children_out + cnt_singles_out
    print(
        f"[info] output rows estimate: parents={cnt_parents_out:,}, children={cnt_children_out:,}, singles={cnt_singles_out:,} -> total={total_out_rows:,}"
    )

    with open(output_csv, "w", newline="", encoding="utf-8") as outf:
        writer = csv.writer(outf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        # header: type + payload cols
        writer.writerow(["type"] + payload_cols)

        with tqdm(total=total_out_rows, unit="rows", ncols=80, desc="Stage 6 - write CSV") as pbar:
            # parents in lexicographic order of payload
            for parent in t_cur.execute("SELECT payload_csv FROM parents ORDER BY payload_csv"):
                parent_payload = parent["payload_csv"]
                writer.writerow([1] + deserialize_payload(parent_payload))
                pbar.update(1)

                # children that belong to this parent_payload
                for child in t_cur.execute(
                    "SELECT payload_csv FROM children WHERE parent_payload = ? ORDER BY payload_csv",
                    (parent_payload,),
                ):
                    writer.writerow([2] + deserialize_payload(child["payload_csv"]))
                    pbar.update(1)

            # singles
            for single in t_cur.execute("SELECT payload_csv FROM singles ORDER BY payload_csv"):
                writer.writerow([0] + deserialize_payload(single["payload_csv"]))
                pbar.update(1)

    print(f"[info] CSV written to: {output_csv}")

    src.close()
    tmp.close()

    # cleanup
    keep_temp_db = False
    if keep_temp_db:
        print(f"[info] temporary DB kept at: {tmp_path}")
    else:
        try:
            os.remove(tmp_path)
            print("[info] temporary DB removed")
        except Exception as e:
            print(f"[warning] failed to remove temp DB: {e}")


def convert_sqlite_to_csv(source_db, source_table, output_csv, keep_temp_db=True):
    # tmp_fd, tmp_path = tempfile.mkstemp(suffix=".sqlite")
    # os.close(tmp_fd)
    tmp_path = "sqlite2csv.temp.db"
    print(f"[info] temporary DB: {tmp_path}")

    # if os.path.exists(tmp_path):
    #     os.unlink(tmp_path)

    if os.path.exists(output_csv):
        os.unlink(output_csv)

    src = sqlite3.connect(source_db)
    src.row_factory = sqlite3.Row
    tmp = sqlite3.connect(tmp_path)
    tmp.row_factory = sqlite3.Row

    s_cur = src.cursor()
    t_cur = tmp.cursor()

    # get columns from source table
    s_cur.execute(f"PRAGMA table_info({source_table})")
    cols_info = s_cur.fetchall()
    if not cols_info:
        raise SystemExit(f"Table {source_table} not found or has no columns.")
    all_cols = [r["name"] for r in cols_info]

    # determine payload columns (exclude meta + ignored)
    ignored = {"query_name", "query_offset", "query_child_num", "recordtype"}
    meta = {"id", "parent_id"}
    payload_cols = [c for c in all_cols if c not in ignored and c not in meta]

    print(f"[info] payload columns ({len(payload_cols)}): {payload_cols}")

    # if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) == 0:

    if 1:
        # count rows for progress
        s_cur.execute(f"SELECT COUNT(*) FROM {source_table}")
        num_source_rows = s_cur.fetchone()[0]
        print(f"[info] source rows: {num_source_rows:,}")

        # ------------------------
        # Stage 1: copy rows to temp with serialized payload and numeric recordtype
        # ------------------------
        t_cur.execute(
            "CREATE TABLE temp (recordtype INTEGER, id INTEGER, parent_id INTEGER, payload_csv TEXT)"
        )

        sql = f"SELECT * FROM {source_table}"
        if 0:
            # debug
            num_source_rows = 1_000_000
            sql += f" LIMIT {num_source_rows}"
        s_cur.execute(sql)

        batch = []
        batch_size = 5000
        inserted = 0
        with tqdm(total=num_source_rows, unit="rows", ncols=80, desc="Stage 1 - copy") as pbar:
            for row in s_cur:
                rt = recordtype_to_int(row["recordtype"]) if "recordtype" in row.keys() else 0
                payload_csv = serialize_payload(row, payload_cols)
                batch.append((rt, row["id"], row["parent_id"], payload_csv))
                if len(batch) >= batch_size:
                    t_cur.executemany("INSERT INTO temp VALUES (?,?,?,?)", batch)
                    tmp.commit()
                    inserted += len(batch)
                    pbar.update(len(batch))
                    batch.clear()
            if batch:
                t_cur.executemany("INSERT INTO temp VALUES (?,?,?,?)", batch)
                tmp.commit()
                inserted += len(batch)
                pbar.update(len(batch))
                batch.clear()

        print(f"[info] inserted into temp: {inserted:,}")
        t_cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_parent_id ON temp(parent_id)")
        t_cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_payload ON temp(payload_csv)")
        tmp.commit()

        # sanity counts
        t_cur.execute("SELECT COUNT(*) FROM temp WHERE recordtype = 1")
        total_parents_raw = t_cur.fetchone()[0]
        t_cur.execute("SELECT COUNT(*) FROM temp WHERE recordtype = 2")
        total_children_raw = t_cur.fetchone()[0]
        t_cur.execute("SELECT COUNT(*) FROM temp WHERE recordtype = 0")
        total_singles_raw = t_cur.fetchone()[0]
        print(
            f"[info] raw counts - parents: {total_parents_raw:,}, children: {total_children_raw:,}, singles: {total_singles_raw:,}"
        )

        # ------------------------
        # Stage 2: Build parent_map (id -> parent_payload) from ALL parent rows
        # ------------------------
        print("[info] building parent_map (id -> parent_payload) ...")
        t_cur.execute("CREATE TABLE parent_map (id INTEGER PRIMARY KEY, parent_payload TEXT)")
        t_cur.execute("SELECT COUNT(*) FROM temp WHERE recordtype = 1")
        parent_rows_count = t_cur.fetchone()[0]
        with tqdm(total=parent_rows_count, unit="rows", ncols=80, desc="Stage 2 - parent_map") as pbar:
            cur = t_cur.execute("SELECT id, payload_csv FROM temp WHERE recordtype = 1")
            batch = []
            bs = 2000
            processed = 0
            for r in cur:
                batch.append((r["id"], r["payload_csv"]))
                if len(batch) >= bs:
                    t_cur.executemany("INSERT INTO parent_map (id, parent_payload) VALUES (?,?)", batch)
                    tmp.commit()
                    processed += len(batch)
                    pbar.update(len(batch))
                    batch.clear()
            if batch:
                t_cur.executemany("INSERT INTO parent_map (id, parent_payload) VALUES (?,?)", batch)
                tmp.commit()
                processed += len(batch)
                pbar.update(len(batch))
                batch.clear()
        t_cur.execute("CREATE INDEX IF NOT EXISTS idx_parent_map_payload ON parent_map(parent_payload)")
        tmp.commit()
        t_cur.execute("SELECT COUNT(*) FROM parent_map")
        parent_map_count = t_cur.fetchone()[0]
        print(f"[info] parent_map entries: {parent_map_count:,}")

        # ------------------------
        # Stage 3: Deduplicate parents by payload_csv (unique parent payloads)
        # ------------------------
        print("[info] deduplicating parents by payload...")
        t_cur.execute(
            """
            CREATE TABLE parents AS
            SELECT MIN(id) AS id, parent_payload AS payload_csv
            FROM parent_map
            GROUP BY parent_payload
            ORDER BY parent_payload
            """
        )
        tmp.commit()
        t_cur.execute("CREATE INDEX IF NOT EXISTS idx_parents_payload ON parents(payload_csv)")
        tmp.commit()
        t_cur.execute("SELECT COUNT(*) FROM parents")
        unique_parents = t_cur.fetchone()[0]
        print(f"[info] unique parent payloads: {unique_parents:,}")

        # ------------------------
        # Stage 4: Deduplicate children within their parent's payload
        # ------------------------
        print("[info] deduplicating children (group by parent_payload + child_payload) ...")
        t_cur.execute("SELECT COUNT(*) FROM temp WHERE recordtype = 2")
        children_raw_count = t_cur.fetchone()[0]

        t_cur.execute(
            """
            CREATE TABLE children AS
            SELECT MIN(t.id) AS id,
                pm.parent_payload AS parent_payload,
                t.payload_csv AS payload_csv
            FROM temp t
            JOIN parent_map pm ON t.parent_id = pm.id
            WHERE t.recordtype = 2
            GROUP BY pm.parent_payload, t.payload_csv
            ORDER BY pm.parent_payload, t.payload_csv
            """
        )
        tmp.commit()
        t_cur.execute("CREATE INDEX IF NOT EXISTS idx_children_parent_payload ON children(parent_payload)")
        tmp.commit()
        t_cur.execute("SELECT COUNT(*) FROM children")
        unique_children = t_cur.fetchone()[0]
        print(
            f"[info] children: raw={children_raw_count:,}, unique_by_parent_payload={unique_children:,}"
        )

        # ------------------------
        # Stage 5: Deduplicate singles (recordtype == 0)
        # ------------------------
        print("[info] deduplicating singles ...")
        t_cur.execute(
            """
            CREATE TABLE singles AS
            SELECT MIN(id) AS id, payload_csv
            FROM temp
            WHERE recordtype = 0
            GROUP BY payload_csv
            ORDER BY payload_csv
            """
        )
        tmp.commit()
        t_cur.execute("SELECT COUNT(*) FROM singles")
        unique_singles = t_cur.fetchone()[0]
        print(f"[info] unique singles: {unique_singles:,}")

        # ------------------------
        # Stage 6: Combine parents and singles into one table and sort by payload_csv
        # This ensures parents and singles are emitted interleaved by payload order.
        # ------------------------
        print("[info] combining parents and singles (preserve recordtype 1=parent,0=single) ...")
        t_cur.execute("DROP TABLE IF EXISTS parent_single")
        t_cur.execute(
            """
            CREATE TABLE parent_single AS
            SELECT 1 AS recordtype, payload_csv
            FROM parents
            UNION ALL
            SELECT 0 AS recordtype, payload_csv
            FROM singles
            """
        )
        tmp.commit()
        t_cur.execute("CREATE INDEX IF NOT EXISTS idx_parent_single_payload ON parent_single(payload_csv)")
        tmp.commit()
        t_cur.execute("SELECT COUNT(*) FROM parent_single")
        cnt_parent_single = t_cur.fetchone()[0]
        print(f"[info] parent_single rows (parents+singles): {cnt_parent_single:,}")

    # ------------------------
    # Stage 7: Write final CSV
    # ------------------------
    print("[info] writing CSV ...")
    # Estimate output rows for progress bar
    t_cur.execute("SELECT COUNT(*) FROM parent_single")
    cnt_parsing_out = t_cur.fetchone()[0]
    t_cur.execute("SELECT COUNT(*) FROM children")
    cnt_children_out = t_cur.fetchone()[0]
    total_out_rows = cnt_parsing_out + cnt_children_out
    print(
        f"[info] output rows estimate: parent/singles={cnt_parsing_out:,}, children={cnt_children_out:,} -> total={total_out_rows:,}"
    )

    with open(output_csv, "w", newline="", encoding="utf-8") as outf:
        writer = csv.writer(outf, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["type"] + payload_cols)

        with tqdm(total=total_out_rows, unit="rows", ncols=80, desc="Stage 7 - write CSV") as pbar:
            # Parent & single rows mixed, sorted by payload_csv
            # NOTE we need a different cursor for the nested query
            t_cur_2 = tmp.cursor()
            cur = t_cur.execute("SELECT recordtype, payload_csv FROM parent_single ORDER BY payload_csv")

            for r in cur:
                rt = r["recordtype"]
                payload = r["payload_csv"]
                if rt == 1:
                    # parent: emit parent row then its children
                    writer.writerow([1] + deserialize_payload(payload))
                    pbar.update(1)
                    # emit children (deduped by parent_payload)
                    for child in t_cur_2.execute(
                        "SELECT payload_csv FROM children WHERE parent_payload = ? ORDER BY payload_csv",
                        (payload,),
                    ):
                        writer.writerow([2] + deserialize_payload(child["payload_csv"]))
                        pbar.update(1)
                else:
                    # single: emit single row
                    writer.writerow([0] + deserialize_payload(payload))
                    pbar.update(1)

    print(f"[info] CSV written to: {output_csv}")

    # cleanup
    src.close()
    tmp.close()
    if keep_temp_db:
        print(f"[info] temporary DB kept at: {tmp_path}")
    else:
        try:
            os.remove(tmp_path)
            print("[info] temporary DB removed")
        except Exception as e:
            print(f"[warning] failed to remove temp DB: {e}")


if __name__ == "__main__":
    # if len(sys.argv) != 4:
    #     print(f"Usage: {sys.argv[0]} SOURCE_DB TABLE_NAME OUTPUT_CSV")
    #     sys.exit(1)
    # source_db, source_table, output_csv = sys.argv[1], sys.argv[2], sys.argv[3]

    source_db = "telefonbuch.db"
    source_table = "telefonbuch"
    output_csv = "telefonbuch.csv"

    convert_sqlite_to_csv(source_db, source_table, output_csv)
