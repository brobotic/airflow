import csv
import logging
import sys


def normalize_imdb_null(value):
    return None if value == r"\N" else value


def to_int_or_none(value):
    value = normalize_imdb_null(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    return int(value) if value.isdigit() else None


def to_float_or_none(value):
    value = normalize_imdb_null(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_bool_or_none(value):
    value = normalize_imdb_null(value)
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    if value in {"0", "1"}:
        return bool(int(value))
    return None


def configure_csv_field_limit():
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            logging.info("Configured CSV field size limit to %d", limit)
            return
        except OverflowError:
            limit = int(limit / 10)
