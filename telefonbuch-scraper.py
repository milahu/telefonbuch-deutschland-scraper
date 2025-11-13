#!/usr/bin/env python3

# this runs 14 hours
# and produces a 1.7 GiB telefonbuch.db file

import os
import re
import io
import sys
import csv
import time
import shlex
import string
import signal
import atexit
import logging
import asyncio
import sqlite3
import subprocess
import itertools
import urllib.parse

import aiohttp
import aiohttp_retry
from tqdm import tqdm
import lxml.etree


# Configure the logging format and level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


# config

debug = False
# debug = True

if debug:
    logger.setLevel(logging.DEBUG)
    logging.getLogger("aiohttp_retry").setLevel(logging.DEBUG)

# no. CSV export fails on recordtype="child"
# example: query_name='ma' query_offset=217320 perpage=15 page=14489
# page = 1 + (query_offset / perpage)
# request_xml = False
request_xml = True

# dastelefonbuch intranet
# NOTE on linux this path translates to
# "$HOME/.wine/drive_c/Program Files (x86)/TVG/DasTelefonbuch Intranet/dastelefonbuch.exe"
dastelefonbuch_intranet_exe = 'C:/Program Files (x86)/TVG/DasTelefonbuch Intranet/dastelefonbuch.exe'
base_url = "http://localhost:1780"
max_results_per_page = 75
# FIXME set_results_per_page has no effect
# FIXME the server ignores our results_per_page config and always returns only 15 results
max_results_per_page = 15
results_per_page = max_results_per_page
if debug:
    # NOTE the server allows only some values: 15, 30, 75
    # results_per_page = 15
    pass
query_name_alphabet = string.ascii_lowercase + string.digits + "äöüß"
query_name_length = 2

# hate these idiots... all text should be utf8
text_encoding = 'latin1'

# also: bad data: utf8 bytes interpreted as latin1 bytes
# "MÃ¼nchen".encode("latin1").decode("utf8") == "München"
# TODO postprocess data: use chardet or charset_normalizer library to fix encoding
# must work with short strings
# should let me suggest encodings (latin1, utf8)
# simple fix...? how can this go wrong?
def fix_encoding(input_str):
    try:
        return input_str.encode("latin1").decode("utf8")
    except UnicodeDecodeError:
        return input_str

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

sessionid_regex = b'<sessionid>([0-9]+)</sessionid>'

# <hitcount>0</hitcount>
num_results_regex = b'<hitcount>([0-9]+)</hitcount>'

# <perpage>15</perpage>
results_per_page_regex = b'<perpage>([0-9]+)</perpage>'

# <refresh><percentcomplete>0,0</percentcomplete><seconds>16</seconds></refresh>
search_results_are_loading_bytes = b'<refresh><percentcomplete>'

# quiet!
# TODO also disable debug logging from aiohttp_chromium.extensions
import logging
logging.getLogger("aiohttp_chromium.client").setLevel("INFO")


# global state
server_process = None
query_offset = 0
num_results = -1


def start_server():
    """
    start background process
    which is killed by "def cleanup" when the script ends
    """
    # FIXME check if server is running already
    global server_process
    kwargs = dict()
    args = [
        dastelefonbuch_intranet_exe,
        "-debug",
        # TODO set port?
    ]
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        args = ["wine"] + args
        kwargs["preexec_fn"] = os.setsid
    logger.info("starting dastelefonbuch_intranet_exe: " + shlex.join(args))
    server_process = subprocess.Popen(args, **kwargs)
    # wait for server to start
    # FIXME dynamic. wait for base_url
    time.sleep(2)


def stop_server():
    """
    kill background process when the script ends
    """
    if os.name == "nt":
        server_process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
    # wait for server to stop
    # FIXME dynamic
    time.sleep(1)


def restart_server():
    logger.info("restarting server")
    stop_server()
    start_server()


def cleanup_on_exit():
    try:
        stop_server()
    except Exception:
        pass


atexit.register(cleanup_on_exit)


# http://httpbin.org/headers
request_headers = dict()
if request_xml:
    # make the server return xml and xsl
    request_headers = {
        "Accept": "text/xml_bytes,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    }
else:
    request_headers = {
        # same accept without "application/xhtml+xml,application/xml"
        "Accept": "text/xml_bytes;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    }


class NextIdCounter:
    "mutable reference of an integer"
    def __init__(self, first_next_id=1):
        self.next_id = first_next_id
    def get(self):
        return self.next_id # return a copy
    def increment(self):
        self.next_id += 1


def parse_address(address_elem, next_id_counter, parent_id=None, rows=None):
    """Recursively parse <address> elements into flat rows."""
    global telefonbuch_columns
    # NOTE next_id is wrapped in a list to make it a "mutable reference"
    if rows is None:
        rows = []

    # predicted row ID
    current_id = next_id_counter.get()
    next_id_counter.increment()

    # Extract all known columns
    row = {"id": current_id, "parent_id": parent_id}
    for col in telefonbuch_columns.keys():
        elem = address_elem.find(col)
        if elem is not None and elem.text:
            row[col] = elem.text.strip()
        else:
            row[col] = None

    rows.append(row)

    # loop direct child address elements
    # findall means "Find all <address> elements that are immediate children of this element."
    for child in address_elem.findall("address"):
        # recurse
        parse_address(child, next_id_counter, parent_id=current_id, rows=rows)

    return rows


async def main():
    retry_options = aiohttp_retry.ExponentialRetry(
        # total attempts (including the first)
        # attempts=5,
        attempts=100,
        # initial delay (seconds)
        start_timeout=0.5,
        # max delay between retries
        max_timeout=10,
        # exponential growth factor
        factor=2,
        # HTTP status codes to retry
        statuses={500, 502, 503, 504},
        # retry on network/timeouts
        exceptions={Exception},
    )
    # total response timeout
    # the default timeout is None so requests can hang forever
    timeout = aiohttp.ClientTimeout(total=30)
    kwargs = dict(
        # raise_for_status=True,  # raise exceptions on bad status codes
        raise_for_status=False,
        retry_options=retry_options,
        headers=request_headers,
        timeout=timeout,
    )
    # async with aiohttp.ClientSession(**kwargs) as session:
    async with aiohttp_retry.RetryClient(**kwargs) as session:
        return await main_inner(session)


async def main_inner(session):

    global debug

    start_server()

    db_path = "telefonbuch-scrape.db"
    db_con = sqlite3.connect(db_path)
    db_cur = db_con.cursor()

    # transaction start
    # get exclusive write access
    db_con.execute("BEGIN EXCLUSIVE")

    sessionid = -1
    query_name = ""
    query_offset = -1
    num_results = -1

    # NOTE SQLite does not check foreign keys by default
    # so FOREIGN KEY constraints are silently ignored.
    # to enable checking foreign keys:
    # conn.execute("PRAGMA foreign_keys = ON;")
    sql = (
        "CREATE TABLE IF NOT EXISTS telefonbuch_scrape (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  parent_id INTEGER,\n"
        "  query_name TEXT,\n"
        "  query_offset INTEGER,\n"
        "  query_child_num INTEGER,\n"
        +
        ",\n".join(map(lambda k: f"  {k} TEXT", telefonbuch_columns.keys()))
        +
        ",\n"
        "  FOREIGN KEY (parent_id) REFERENCES telefonbuch_scrape (id)\n"
        ")"
    )
    db_cur.execute(sql)

    if 1:
        sql = (
            "CREATE UNIQUE INDEX IF NOT EXISTS telefonbuch_scrape_query_name_query_offset ON telefonbuch_scrape (\n"
            "  query_name,\n"
            "  query_offset,\n"
            "  query_child_num\n"
            ")"
        )
        db_cur.execute(sql)

    xml_header = '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>'
    xslt_url_regex = r'<\?xml-stylesheet type="text/xsl" href="([^"]+)"\?>'

    xslt_transform_cache = dict()

    async def render_html(xml_bytes):
        if not request_xml:
            return xml_bytes
        # global xslt_transform_cache
        if not xml_bytes.startswith(xml_header):
            return xml_bytes
        match = re.search(xslt_url_regex, xml_bytes)
        if not match:
            return xml_bytes
        xslt_url = base_url + "/" + match.group(1)
        xslt_transform = xslt_transform_cache.get(xslt_url)
        if not xslt_transform:
            logger.info(f"xslt_url: {xslt_url}")
            async with session.get(xslt_url) as response:
                assert response.status == 200, f"bad response.status {response.status}"
                xslt_text = await response.read()
            xslt_doc = lxml.etree.fromstring(xslt_text.encode('utf8')) # latin1?
            # FIXME lxml.etree.XSLTParseError: Cannot resolve URI string://__STRING__XSLT__/common.xsl
            xslt_transform = lxml.etree.XSLT(xslt_doc)
            xslt_transform_cache[xslt_url] = xslt_transform
        xml_text = xml_bytes
        xml_doc = lxml.etree.fromstring(xml_text.encode('utf-8')) # latin1?
        html_doc = xslt_transform(xml_doc)
        xml_bytes = str(html_doc)
        return xml_bytes

    async def get_sessionid():
        # get sessionid
        logger.info(f"getting sessionid")
        sessionid = 0
        url = base_url
        if debug: logger.debug(f"url: {url}")
        async with session.get(url) as response:
            assert response.status == 200, f"bad response.status {response.status}"
            xml_bytes = await response.read()
            # xml_bytes = await render_html(xml_bytes)
            if match := re.search(sessionid_regex, xml_bytes):
                sessionid = int(match.group(1))
            else:
                raise ValueError(f"failed to parse sessionid from xml_bytes:\n\n{xml_bytes}")
            logger.info(f"sessionid: {sessionid}")
        return sessionid

    async def set_results_per_page():
        # await asyncio.sleep(1)
        # get the preferences page
        logger.info(f"setting results_per_page")
        params = [
            ('sessionid', sessionid),
            ('mask', 'preferences'),
        ]
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}/telefonbuch.cgi?{query_string}"
        if debug: logger.debug(f"url: {url}")
        async with session.get(url) as response:
            assert response.status == 200, f"bad response.status {response.status}"
            # logger.debug("xml_bytes:"); logger.debug(await response.read())
            # xml_bytes = await response.read()
            # parse?
        # await asyncio.sleep(1)
        # set results_per_page
        params = [
            ('sessionid', sessionid),
            ('database', 'whitepages'),
            ('lastdatabase', 'whitepages'),
            ('btnpreferences', 'Speichern'), # save settings
            ('stylesheet', 'standard.css'),
            ('results_per_page', results_per_page),
            ('defaultcity', ""),
            ('refresh', "on"),
            ('showmask', 'on'),
            ('btnpreferences', 'Speichern'), # save settings
        ]
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}/telefonbuch.cgi?{query_string}"
        if debug: logger.debug(f"url: {url}")
        async with session.get(url) as response:
            assert response.status == 200, f"bad response.status {response.status}"
            # logger.debug("xml_bytes:"); logger.debug(await response.read())
            # xml_bytes = await response.read()
            # parse?
        # await asyncio.sleep(1)

    async def get_search_results():
        global num_results
        # start search and get num_results
        logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: getting search results")
        # logger.debug(f"query_name={query_name!r} query_offset={query_offset}: getting search results")
        num_results = -1
        # if debug: time.sleep(2)
        params = [
            ('sessionid', sessionid),
            ('database', 'whitepages'),
            ('lastdatabase', 'whitepages'),
            ('city', ''),
            ('name', query_name),
            ('firstname', ''),
            # ('results_per_page', results_per_page),
        ]
        if query_offset == 0:
            params += [
                ('btnhidden', "Suchen"),
            ]
        else:
            params += [
                # ('startrecord', query_offset),
                ('startrecord', 0),
                ('btnpage', 1 + (query_offset // max_results_per_page)), # startrecord = 0
            ]

        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}/telefonbuch.cgi?{query_string}"
        if debug: logger.debug(f"url: {url}")
        xml_bytes = b""
        for retry_idx in range(1000):
            async with session.get(url) as response:
                assert response.status == 200, f"bad response.status {response.status}"
                # xml_string = await response.read()
                xml_bytes = await response.read()
                if search_results_are_loading_bytes in xml_bytes:
                    await asyncio.sleep(0.1)
                    continue # retry
                # logger.debug("xml_bytes:"); logger.debug(xml_bytes); sys.exit() # debug
                if match := re.search(num_results_regex, xml_bytes):
                    num_results = int(match.group(1))
                else:
                    logger.error(f"query_name={query_name!r}: FIXME not found num_results in xml_bytes:")
                    print(xml_bytes)
                    sys.exit(1)
                break # stop retry loop

        if num_results == -1:
            logger.error(f"query_name={query_name!r}: FIXME not found num_results")
            sys.exit(1)

        if 0:
            logger.error(f"query_name={query_name!r}: xml_bytes:")
            print(xml_bytes)
            sys.exit(1)

        return num_results, xml_bytes

    async def select_results():
        # if debug: time.sleep(2)
        logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: selecting results")
        # select results
        params = [
            ('sessionid', sessionid),
            ('database', 'whitepages'),
            ('lastdatabase', 'whitepages'),
            ('city', ''),
            ('name', query_name),
            ('firstname', ''),
            ('btnselect', 'Alle+Einträge+markieren'), # search + select results
            # ('results_per_page', results_per_page),
            ('startrecord', query_offset),
            # ('btnpage', 1 + (query_offset // max_results_per_page)), # startrecord = 0
        ]
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}/telefonbuch.cgi?{query_string}"
        if debug: logger.debug(f"url: {url}")
        async with session.get(url) as response:
            assert response.status == 200, f"bad response.status {response.status}"
            # logger.debug("xml_bytes:"); logger.debug(await response.read())
            # xml_bytes = await response.read()
            # parse?

    # TODO request export

    # TODO fetch csv

    async def unselect_results():
        # unselect results
        logger.debug(f"query_name={query_name!r}: unselecting results")
        params = [
            ('sessionid', sessionid),
            ('database', 'whitepages'),
            ('lastdatabase', 'whitepages'),
            ('city', ''),
            ('name', query_name),
            ('firstname', ''),
            ('btnunselect', 'Markierung+aufheben'), # search + unselect all
            # ('results_per_page', results_per_page),
        ]
        for i in range(results_per_page):
            params.append((f"rs{query_offset + i}", "0"))
        params.append(('startrecord', query_offset))
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}/telefonbuch.cgi?{query_string}"
        if debug: logger.debug(f"url: {url}")
        async with session.get(url) as response:
            assert response.status == 200, f"bad response.status {response.status}"
            # logger.debug("xml_bytes:"); logger.debug(await response.read())
            # xml_bytes = await response.read()
            # parse?

    def print_results(columns, sql_rows):
        # print results
        sql_columns = ["query_name", "query_offset"] + columns
        for row_idx, row in enumerate(sql_rows):
            row_dict = dict()
            for idx, val in enumerate(row):
                key = sql_columns[idx]
                row_dict[key] = val
            row = row_dict
            # logger.debug(row.keys()); break
            if 1:
                columns_to_keep = [
                    "name0",
                    "firstname0",
                    "street",
                    "housenumber",
                    "zipcode",
                    "city",
                    "areacode",
                    "phonenumber",
                    # "callrate",
                    "recordtype",
                ]
                sub_row = {
                    "query_name": query_name,
                    "query_offset": query_offset + row_idx,
                    **{k: row[k] for k in columns_to_keep if k in row}
                }
                if row_idx == 0:
                    logger.debug(list(sub_row.keys()))
                logger.debug(list(sub_row.values())); continue
                logger.debug(sub_row); continue
            elif 0:
                row = {
                    "query_name": query_name,
                    "query_offset": query_offset + row_idx,
                    **row
                }
                # print all columns
                if row_idx == 0:
                    logger.debug(list(row.keys()))
                logger.debug(list(row.values())); continue
                logger.debug(row); continue
            else:
                row = {
                    "query_name": query_name,
                    "query_offset": query_offset + row_idx,
                    **row
                }
                # print all columns
                logger.debug(row); continue
            # for key in row:
            #     logger.debug(f"{key} = {row[key]!r}")

    sessionid = await get_sessionid()

    # FIXME set_results_per_page has no effect
    await set_results_per_page()

    insert_columns = [
        "query_name",
        "query_offset",
        "query_child_num",
        "id",
        "parent_id",
        *telefonbuch_columns.keys()
    ]

    sql_insert_result = (
        "INSERT INTO telefonbuch_scrape (\n"
        +
        ",\n".join(insert_columns)
        +
        "\n) VALUES ("
        +
        ",".join("?" for _ in insert_columns)
        +
        ")"
    )
    if debug:
        logger.debug(f"sql_insert_result:\n{sql_insert_result}")

    query_name_list = []
    for query_name in itertools.product(query_name_alphabet, repeat=query_name_length):
        query_name = ''.join(query_name)
        if query_name[0] in string.digits:
            continue
        query_name_list.append(query_name)

    # query_name_list = ["aa"] # debug
    # query_name_list = ["ma"] # debug

    for query_idx, query_name in enumerate(query_name_list):

        # check if this query_name was already processed
        # assume atomic inserts per query_name (transaction start + transaction end)
        sql = "SELECT 1 FROM telefonbuch_scrape WHERE query_name = ?"
        args = (query_name,)
        res = db_cur.execute(sql, args).fetchone()
        if not res is None:
            # exists in database
            if debug: logger.debug(f"query_name={query_name!r}: exists in database")
            continue

        # restart_server(); sessionid = await get_sessionid()

        query_offset = 0
        last_xml_bytes = None
        num_results = -1

        if debug:
            # query_offset = 11444 - 10 # 11444 is num_results for query_name="aa" # 11434
            pass

        num_results, xml_bytes = await get_search_results()

        if num_results == 0:
            if debug: logger.debug(f"query_name={query_name!r}: no results")
            continue

        query_progress = ((query_idx + 1) / len(query_name_list))
        logger.info(f"query_name={query_name!r}: query {query_idx + 1} of {len(query_name_list)} = {query_progress * 100:.2f}%")

        if debug:
            logger.debug(f"query_name={query_name!r}: num_results={num_results}")

        with tqdm(total=num_results, desc=query_name, unit="rows", ncols=80) as progressbar:

            # transaction start
            # ensure atomic inserts per query_name
            # db_con.execute("BEGIN")

            loop_idx = -1

            for query_offset in range(0, (num_results + 1), results_per_page):

                loop_idx += 1

                # logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}")

                if loop_idx > 0:
                    num_results, xml_bytes = await get_search_results()

                # # TODO? parse num_results
                # # <p id="results">11444 Treffer gefunden.
                # #       <input type="hidden" name="startrecord" value="11443">
                # # </p>
                # if csv_str.startswith(csv_no_results_header):
                #     # not reached?
                #     # see query_offset_is_out_of_range
                #     logger.info(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: result is empty -> done query_name {query_name!r}")
                #     # stop looping query_offset, go to next query_name
                #     last_csv_str = ""
                #     await unselect_results()
                #     break

                # # logger.debug("csv_str:")
                # # logger.debug(csv_str)

                # # time.sleep(1)
                # # sys.exit()

                # csv_buffer = io.StringIO(csv_str)
                # csv_reader = csv.DictReader(csv_buffer)
                # assert csv_reader.fieldnames == list(telefonbuch_columns.keys()), \
                #     f"query_name={query_name!r} query_offset={query_offset}: bad csv_reader.fieldnames: {csv_reader.fieldnames}"
                # columns = csv_reader.fieldnames

                # # logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: num rows: {len(csv_reader)}")
                # # logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: len(csv_str)={len(csv_str)}")
                # # logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: csv_str={repr(csv_str)[:10000]}")

                # # NOTE we can use csv_reader only once
                # sql_rows = []
                # for row_idx, row in enumerate(csv_reader):
                #     sql_rows.append((
                #         query_name,
                #         query_offset + row_idx,
                #         *tuple(row[col] for col in columns)
                #     ))

                # if debug:
                #     print_results(columns, sql_rows)

                # if len(sql_rows) > results_per_page:
                #     raise ValueError(f"bad number of sql_rows: {len(sql_rows)} > {results_per_page}")

                # if debug: logger.debug(f"query_name={query_name!r} query_offset={query_offset}/{num_results}: inserting {len(sql_rows)} rows")
                # db_cur.executemany(sql_insert_result, sql_rows)
                # # db_con.commit()

                # time.sleep(1)
                # sys.exit()

                # await unselect_results()

                # predict next inserted rowid
                # NOTE this is why: db_con.execute("BEGIN EXCLUSIVE")
                next_id = db_cur.execute("SELECT COALESCE(MAX(id), 0) FROM telefonbuch_scrape").fetchone()[0] + 1
                next_id_counter = NextIdCounter(next_id)

                # print(xml_bytes); sys.exit()

                if match := re.search(results_per_page_regex, xml_bytes):
                    actual_results_per_page = int(match.group(1))
                    assert actual_results_per_page == results_per_page, \
                        f"results_per_page: actual={actual_results_per_page} != expected={results_per_page}"

                # parse xml
                xml_root = lxml.etree.fromstring(xml_bytes)

                # Find all top-level addresses
                addresses = xml_root.findall(".//entries/address")

                all_rows = []
                for addr in addresses:
                    # # Only process top-level (non-child) addresses
                    # # i.e., skip <address> elements that have an ancestor <address>
                    # recordtype_elem = addr.find("recordtype")
                    # if recordtype_elem is not None and recordtype_elem.text == "child":
                    #     continue
                    rows = parse_address(addr, next_id_counter)
                    all_rows.extend(rows)

                # # Prepare executemany data
                columns = ["id", "parent_id"] + list(telefonbuch_columns.keys())
                # placeholders = ", ".join(["?" for _ in columns])
                # insert_sql = f"INSERT INTO addresses ({', '.join(columns)}) VALUES ({placeholders})"

                # sql_rows = [
                #     tuple(row.get(col) for col in columns)
                #     for row in all_rows
                # ]

                sql_rows = []
                row_query_offset = query_offset
                query_child_num = 0
                for row_idx, row in enumerate(all_rows):
                    if row["parent_id"] is None:
                        # recordtype="single" or recordtype="parent"
                        query_child_num = 0
                    else:
                        # recordtype="child"
                        query_child_num += 1
                    sql_rows.append((
                        query_name,
                        row_query_offset,
                        query_child_num,
                        *tuple(row[col] for col in columns)
                    ))
                    if row["parent_id"] is None:
                        # recordtype="single" or recordtype="parent"
                        row_query_offset += 1

                db_cur.executemany(sql_insert_result, sql_rows)

                progressbar.update(len(sql_rows))

                last_xml_bytes = xml_bytes

            # done query_name
            # transaction end
            # atomic inserts per query_name
            db_con.commit()

            # transaction start
            # get exclusive write access
            db_con.execute("BEGIN EXCLUSIVE")

        # done progressbar

    # done main


asyncio.run(main())
