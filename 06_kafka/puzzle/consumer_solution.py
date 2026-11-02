#!/usr/bin/env python3

"""
Reads puzzle messages from the "puzzle" topic and reassembles the quote three
ways for comparison: (1) slotted Piece objects sorted by "order_no",
(2) a plain order_no -> letter dict sorted by key, and (3) a DuckDB table
(saved to puzzle.duckdb in this directory) queried with SQL. All three should print the same.
"""

import os
import json
import duckdb
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "puzzle"
MAX_EMPTY_POLLS = 5
DUCKDB_FILE = "puzzle.duckdb"

# Kafka app; start from the oldest message. Uncomment extra_config to force IPv4 if using "localhost"
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="puzzle-consumer",
    auto_offset_reset="earliest",
    # consumer_extra_config={"broker.address.family": "v4"},
)

# One letter of the quote and its position.
# __slots__ declares the only attributes a Piece can have. Python then stores
# them in fixed slots instead of a per-object __dict__, which uses less memory
# per object (useful when a stream delivers many small records) and raises an
# AttributeError if you mistype an attribute name (e.g. p.ordr_no = 3).
class Piece:
    __slots__ = ("order_no", "letter")

    def __init__(self, order_no, letter):
        self.order_no = order_no
        self.letter = letter

# Collect every message until the topic goes quiet, then sort and reassemble
def consume_messages():
    # Method 1 (slots): order_no -> Piece object.
    # Keyed by order_no so a duplicate message overwrites rather than repeats a letter
    pieces = {}
    # Method 2 (dict+sort): plain order_no -> letter mapping, no class needed
    letters = {}
    # Method 3 (DuckDB): raw (order_no, letter) rows, duplicates included; SQL handles them
    rows = []
    try:
        # No offset commits, so every run re-reads the topic from the start
        with app.get_consumer(auto_commit_enable=False) as consumer:
            consumer.subscribe([TOPIC])
            print(f"Consuming from topic '{TOPIC}' on {KAFKA_BROKER}")
            empty_polls = 0
            while empty_polls < MAX_EMPTY_POLLS:
                msg = consumer.poll(1)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                empty_polls = 0

                # value is the JSON payload; wrap the two fields we need in a Piece
                value = json.loads(msg.value())
                piece = Piece(int(value["order_no"]), value["letter"])
                pieces[piece.order_no] = piece

                # Same data stored the simple way: the dict key is the position
                letters[int(value["order_no"])] = value["letter"]

                # Same data again as a row tuple for loading into DuckDB
                rows.append((int(value["order_no"]), value["letter"]))
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

    # Method 1 (slots): messages arrive out of order; sort the Pieces by their order_no slot
    ordered = sorted(pieces.values(), key=lambda p: p.order_no)
    quote = "".join(p.letter for p in ordered)
    print(f"Received {len(ordered)} pieces")
    print(f"Quote (slots):     {quote}")

    # Method 2 (dict+sort): sorted() on a dict returns its keys in order,
    # so look up each letter by its sorted order_no and join them
    quote2 = "".join(letters[n] for n in sorted(letters))
    print(f"Quote (dict+sort): {quote2}")

    # Method 3 (DuckDB): load rows into a table in a local DuckDB file and let SQL do the work.
    # DISTINCT drops duplicate messages; string_agg(... ORDER BY order_no)
    # concatenates the letters in position order into a single string.
    try:
        con = duckdb.connect(DUCKDB_FILE)  # creates the file on first run
        # OR REPLACE so each run starts from a fresh table instead of failing or appending
        con.execute("CREATE OR REPLACE TABLE pieces (order_no INTEGER, letter VARCHAR)")
        con.executemany("INSERT INTO pieces VALUES (?, ?)", rows)
        quote3 = con.execute("""
            SELECT string_agg(letter, '' ORDER BY order_no)
            FROM (SELECT DISTINCT order_no, letter FROM pieces)
        """).fetchone()[0]
        print(f"Quote (DuckDB):    {quote3}")
        con.close()
    except Exception as e:
        print(f"DuckDB error: {e}")

if __name__ == "__main__":
    consume_messages()
