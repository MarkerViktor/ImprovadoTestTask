from typing import IO, Iterable, Any
from itertools import chain
import csv

from .utils import determine_types


def parse(file_obj: IO) -> tuple[dict[str, type], Iterable[Iterable[Any]]]:
    rows = csv.reader(file_obj)
    # Get headers from first row and types according to the next
    headers = next(rows)
    first_row = next(rows)
    rows = chain([first_row], rows)  # Return first_row to the first position
    types = determine_types(first_row)
    # Make schema dict
    schema = dict(zip(headers, types))
    return schema, rows
