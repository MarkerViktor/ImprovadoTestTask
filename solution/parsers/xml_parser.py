import xml.etree.cElementTree as ET
from typing import IO, Iterable, Any

from .utils import determine_types, ParseError


def parse(file_obj: IO) -> tuple[dict[str, type], Iterable[Iterable[Any]]]:
    try:
        tree = ET.ElementTree(file=file_obj)
        headers = [obj.get('name') for obj in tree.find('objects').iterfind('object')]
        first_row = (obj.find('value').text for obj in tree.find('objects').iterfind('object'))
    except (ET.ParseError, AttributeError) as e:
        raise ParseError(f"Can't parse file «{file_obj.name}». Check XML format!") from e
    types = determine_types(first_row)
    schema = dict(zip(headers, types))
    return schema, rows_iter(tree, headers)


def rows_iter(tree, headers):
    for row_tree in tree.iterfind('objects'):
        row_dict = {}
        for element_tree in row_tree.iterfind('object'):
            header = element_tree.get('name')
            value = element_tree.find('value').text
            row_dict[header] = value
        yield tuple(row_dict.get(h) for h in headers)
