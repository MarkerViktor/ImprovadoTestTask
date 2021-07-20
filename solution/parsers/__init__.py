from typing import Callable, IO, Iterable, Any

from . import csv_parser, json_parser, xml_parser
from .utils import ParseError

# Parser function must have the following signature:
#       parse(file_like_object: IO) -> (schema: dict[str, type]), rows_iterator: Iterable[tuple[Any]])
# and raise ParseError exception in case of parsing troubles.
ParserFunc = Callable[[IO], tuple[dict[str, type], Iterable[tuple[Any]]]]

parsers = {
    'csv': csv_parser.parse,
    'json': json_parser.parse,
    'xml': xml_parser.parse,
}


def get_parser(file_type: str) -> ParserFunc:
    """Return parser function for given file_type"""
    if parse_func := parsers.get(file_type.removeprefix('.')):
        return parse_func
    else:
        raise ModuleNotFoundError(f"There is no parser functions for {file_type} file.")
