#!/usr/bin/env python3

import os
import re
import sys
import glob

def join_parts(output_filename="telefonbuch.csv", pattern="telefonbuch.part[0-9]*.csv"):

    if os.path.exists(output_filename):
        print(f"error: output exists: {output_filename}")
        sys.exit(1)

    # Find all matching part files
    part_files = sorted(
        glob.glob(pattern),
        key=lambda x: int(re.search(r"part(\d+)", x).group(1)) if re.search(r"part(\d+)", x) else float('inf')
    )

    if not part_files:
        print("No part files found matching pattern:", pattern)
        return

    with open(output_filename, "wb") as outfile:
        for part_file in part_files:
            with open(part_file, "rb") as infile:
                content = infile.read()
                # Ensure trailing newline byte
                if not content.endswith(b"\n"):
                    content += b"\n"
                outfile.write(content)

    print(f"done {output_filename}")

if __name__ == "__main__":
    join_parts()
