import json
from collections import Iterable
from typing import Any, IO

from .utils import ParseError


def parse(file_obj: IO) -> tuple[dict[str, type], Iterable[Iterable[Any]]]:
    try:
        row_dicts = json.load(file_obj)['fields']
    except (json.JSONDecodeError, KeyError) as e:
        raise ParseError(f"Can't parse file «{file_obj.name}». Check JSON format!") from e
    # Get schema from first element
    schema = {field_name: type(value) for field_name, value in row_dicts[0].items()}
    # Gets data from each row_dict in schema order
    rows_iterator = ((row_dict.get(field_name) for field_name in schema.keys()) for row_dict in row_dicts)
    return schema, rows_iterator
