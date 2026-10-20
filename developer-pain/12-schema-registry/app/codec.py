"""Avro encoding with the schema id carried in an Apache Iggy message header."""
import io
import json
from pathlib import Path

from apache_iggy import SendMessage
from fastavro import parse_schema, schemaless_reader, schemaless_writer

SCHEMA_ID_HEADER = "schema-id"
CONTENT_TYPE_HEADER = "content-type"
SCHEMAS = Path(__file__).resolve().parent.parent / "schemas"


def load_schema(name: str) -> str:
    return (SCHEMAS / name).read_text()


def encode(record: dict, schema: dict, schema_id: int) -> SendMessage:
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(schema), record)
    # The payload stays pure Avro. The header tells any reader how to decode it.
    return SendMessage(
        buf.getvalue(),
        user_headers={SCHEMA_ID_HEADER: schema_id, CONTENT_TYPE_HEADER: "avro/binary"},
    )


def decode(payload: bytes, writer_schema: dict, reader_schema: dict) -> dict:
    # Avro resolves the two schemas: fields the writer didn't have get the reader's defaults.
    return schemaless_reader(io.BytesIO(payload), parse_schema(writer_schema), parse_schema(reader_schema))


def as_dict(schema_text: str) -> dict:
    return json.loads(schema_text)
