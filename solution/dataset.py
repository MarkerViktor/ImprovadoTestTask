from itertools import chain
from io import StringIO
from collections import Collection
from typing import IO, Union, Iterator, Callable, Iterable

Schema = dict[str, type]
IndicesDict = dict[str, int]
RowValue = Union[str, int, float, bool]
Row = tuple[RowValue, ...]
RowDict = dict[str, RowValue]
GrouperOperator = Callable[[RowValue, RowValue], RowValue]


class Dataset(Collection):
    def __init__(self, name: str, schema: Schema, use_defaults_for_kwargs: bool = False):
        self._schema: Schema = schema
        self._indices: IndicesDict = {header: index for index, header in enumerate(self.schema.keys())}
        self._name: str = name
        self._data: list[Row] = []
        self._use_defaults_for_kwargs = use_defaults_for_kwargs

    def add_row(self, *args: RowValue, **kwargs: RowValue):
        """
        Add new row to the dataset converting values to suitable schema's types if possible.
        If any kwargs provided args will be ignored.
        Kwargs keys which aren't in the schema will be ignored.
        """
        values: list[RowValue] = []
        if kwargs:
            # Check kwargs completeness and add default values, if some missing and self._use_default_for_kwargs is set
            missing_keys = self.schema.keys() - kwargs.keys()
            if len(missing_keys) > 0:
                if self._use_defaults_for_kwargs:
                    for key in missing_keys:
                        kwargs[key] = self.schema[key]()  # Call type constructor without arguments
                else:
                    raise TypeError(f"Missing required keyword arguments: {missing_keys}", kwargs)
            # Try to fill values in schema order
            for header, type_ in self.schema.items():
                try:
                    values.append(type_(kwargs[header]))
                except ValueError:
                    raise ValueError(f"Can't convert keyword argument «{header}» to type {type_.__name__}.", kwargs)
                except TypeError:
                    raise TypeError(f"Missing required keyword argument «{header}».", kwargs)
        elif args:
            # Check given args completeness
            if len(args) < len(self.schema):
                raise TypeError(f"Missing required positional arguments: {list(self.schema.keys())[len(args) - 1:]}")
            # Try to fill values in provided order
            for index, type_ in enumerate(self.schema.values()):
                try:
                    values.append(type_(args[index]))
                except ValueError:
                    raise ValueError(f"Can't convert {index+1}th positional argument to type {type_.__name__}.", args)
                except TypeError:
                    raise TypeError(f"Missing required {index+1}th positional argument.", args)
        if not values:
            raise ValueError("Positional or keyword arguments must be provided.")
        self._data.append(tuple(values))

    def from_iterable(self, *row_iters: Iterable[Union[Row, RowDict]], dict_row: bool = False) -> list[Exception]:
        """
        Extend dataset by elements from given row iterator.
        Iterator must yield only tuples or only dicts depending on dict_row arg.
        Returns exceptions caught during adding.
        """
        errors = []
        for row in chain(*row_iters):
            try:
                if dict_row:
                    self.add_row(**row)
                else:
                    self.add_row(*row)
            except (TypeError, ValueError) as e:
                errors.append(e)
        return errors

    def sort(self, *headers: str) -> None:
        """Sort dataset rows by given headers. If no headers provided sorts by all headers."""
        if not headers:
            self._data.sort()
        else:
            self._data.sort(key=lambda row: tuple(self._by_headers(row, headers)))

    def group_by(self, *grouping_headers: str, operator: GrouperOperator) -> None:
        """
        Group dataset rows by given headers.
        Like SQL «GROUP BY ...» all group members will subject to operator call in order.
        """
        other_headers = sorted(self.headers - set(grouping_headers))  # Not grouping headers
        grouping_other_pairs: Iterator[tuple[tuple[RowValue, ...], list[RowValue]]] = \
            ((tuple(self._by_headers(r, grouping_headers)), list(self._by_headers(r, other_headers))) for r in self)

        groups_dict: dict[tuple[RowValue, ...], list[RowValue]] = {}
        for grouping_tuple, other_list in grouping_other_pairs:
            if values_list := groups_dict.get(grouping_tuple):
                for index, value in enumerate(other_list):
                    values_list[index] = operator(values_list[index], value)
            else:
                groups_dict[grouping_tuple] = other_list

        self._schema = {header: self.schema[header] for header in chain(grouping_headers, other_headers)}
        self._indices: IndicesDict = {header: index for index, header in enumerate(self.schema.keys())}
        self._data = [(*grouping, *other) for grouping, other in groups_dict.items()]

    def write_as_table(self, io_obj: IO, separator: str = '\t'):
        """Write rows to a file-like object. Row elements dividing by separator."""
        io_obj.write(separator.join(self.headers) + '\n')
        for row in self:
            io_obj.write(separator.join(str(element) for element in row))
            io_obj.write('\n')

    @property
    def headers(self):
        return self.schema.keys()

    @headers.setter
    def headers(self, headers: tuple[str]):
        if len(headers) == len(self.headers):
            self._schema = dict(zip(headers, self.schema.values()))
        else:
            raise IndexError(f"Incorrect number of headers is given (must be {len(self.headers)}).")

    @property
    def schema(self) -> Schema:
        return self._schema

    def dict_iter(self) -> Iterator[RowDict]:
        headers = self.headers
        for row in self:
            yield dict(zip(headers, row))

    def __repr__(self):
        buffer = StringIO()
        buffer.write(f"Dataset «{self._name}»:\n")
        self.write_as_table(buffer)
        return buffer.getvalue()

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, item):
        return item in self._data

    def __len__(self):
        return len(self._data)

    @staticmethod
    def merge_schemas(*datasets: 'Dataset', intersection: bool = True):
        """Merge given schema dicts to new one with checking types."""
        schemas = [ds.schema for ds in datasets]
        new_schema = {}
        if intersection:
            new_schema = schemas[0]
            for schema in schemas:
                if intersection:
                    # Get keys which are in new_schema and adding_schema
                    common_headers = schema.keys() & new_schema.keys()
                    # Check type difference for a single header
                    for header in common_headers:
                        if new_schema[header] != schema[header]:
                            raise TypeError(f"There are dataset row schemas with different types "
                                            f"«{new_schema[header].__name__}» and «{schema[header].__name__}» for "
                                            f"single header «{header}».")
                    new_schema = dict(schema.items() & new_schema.items())
        else:
            for adding_schema in schemas:
                # Get header->type pairs which not in new_schema
                diff_pairs = adding_schema.items() - new_schema.items()
                # Check type difference for a single header
                for header, type_ in diff_pairs:
                    if exist_type := new_schema.get(header):
                        if type_ != exist_type:
                            raise TypeError(f"There are dataset row schemas with different types "
                                            f"«{exist_type.__name__}» and «{type_.__name__}» for single header «{header}».")
                new_schema |= diff_pairs

        # Sort schema by headers
        new_schema = dict(sorted(new_schema.items()))
        return new_schema

    def _by_headers(self, row: Row, headers: Iterable[str]) -> Iterator[RowValue]:
        """
        Make iterator over row according to provided headers.
        If the dataset hasn't required header None will be yielded.
        """
        for header in headers:
            index = self._indices.get(header)
            if index is not None:
                yield row[index]
            else:
                yield None
