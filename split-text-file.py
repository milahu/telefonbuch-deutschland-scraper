#!/usr/bin/env python3


import os
import sys
import math


def split_file(input_file, chunk_size):
    base_path, file_ext = os.path.splitext(input_file)

    # Estimate total number of chunks for zero-padding
    file_size = os.path.getsize(input_file)
    total_chunks = math.ceil(file_size / chunk_size)
    num_digits = len(str(total_chunks))  # number of digits for zero-padding

    chunk_num = 1
    bytes_written = 0
    output_file = f"{base_path}.part{str(chunk_num).zfill(num_digits)}{file_ext}"
    out_f = open(output_file, 'wb')
    print(f"writing {output_file}")

    with open(input_file, 'rb') as f:
        for line in f:
            # If adding this line exceeds the chunk size, start a new chunk
            if bytes_written + len(line) > chunk_size:
                out_f.close()
                chunk_num += 1
                bytes_written = 0
                output_file = f"{base_path}.part{str(chunk_num).zfill(num_digits)}{file_ext}"
                out_f = open(output_file, 'wb')
                print(f"writing {output_file}")

            out_f.write(line)
            bytes_written += len(line)

    out_f.close()


if __name__ == "__main__":

    # github file size limits:
    # soft limit: 50 MiB
    # hard limit: 100 MiB
    chunk_size = 40 * 1024 * 1024 # 40 MiB

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input_file>")
        sys.exit(1)

    split_file(sys.argv[1], chunk_size)
