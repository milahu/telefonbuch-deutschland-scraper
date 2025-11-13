#!/usr/bin/env python3

import sqlite3
import sys

DB_PATH = "telefonbuch-scrape.db"
TABLE_NAME = "telefonbuch_scrape"

# on error, show only the first N bad rows
error_num_rows = 100

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
    "recordtype": ("single", "parent", "child"), # enum
}


def main():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # --- Identify boolean and enum columns ---
        bool_cols = [col for col, typ in telefonbuch_columns.items() if typ is bool]
        enum_cols = {col: vals for col, vals in telefonbuch_columns.items() if isinstance(typ := telefonbuch_columns[col], tuple)}

        # --- Build WHERE conditions ---
        conditions = []

        # Boolean columns: must be 'true' or 'false'
        for col in bool_cols:
            conditions.append(f"({col} != 'true' AND {col} != 'false')")

        # Enum columns: must be in given allowed values
        for col, allowed_values in enum_cols.items():
            allowed_list = ", ".join(f"'{v}'" for v in allowed_values)
            conditions.append(f"({col} NOT IN ({allowed_list}))")

        # Combine all into one WHERE clause
        where_clause = " OR ".join(conditions)
        query = f"SELECT * FROM {TABLE_NAME} WHERE {where_clause} LIMIT {error_num_rows}"

        # --- Execute ---
        cursor.execute(query)
        bad_rows = cursor.fetchall()

        if bad_rows:
            print(f"error: found {len(bad_rows)} bad rows:")
            for row in bad_rows:
                print(row)
            sys.exit(1)
        else:
            print(f"ok: all column types are valid")
            sys.exit(0)

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
