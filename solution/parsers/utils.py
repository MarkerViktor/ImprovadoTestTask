from typing import Iterable


def determine_types(row: Iterable[str]) -> list[type]:
    """
    Try to parse string as integer, float or bool values. In other cases str type will be yielded.
    Integer guess checks firstly because string with integer value can be parsed by int() and float(),
    unlike string with float can be taken only by float().
    """
    types = []
    for element in row:
        try:
            int(element)
            types.append(int)  # executed if element is string with integer
            continue
        except ValueError: pass
        try:
            float(element)
            types.append(float)  # executed if element is not string with integer but number
            continue
        except ValueError: pass
        if element in ("True", "False"):
            types.append(bool)
        else:
            types.append(str)
    return types


class ParseError(Exception):
    pass
