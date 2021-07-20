import argparse
from pathlib import Path
from typing import Iterable

from solution import Dataset, get_parser, ParseError


def main(data_folder_path: Path, result_file_name: str):
    print(f'Perform advanced task on files from «{data_folder_path}» directory:')
    for data_file in data_folder_path.iterdir():
        print(f' — {data_file.name}')

    errors = []
    datasets = []

    # Got datasets from each file of data_folder_path
    for data_file in data_folder_path.iterdir():
        file_extension = data_file.suffix
        file_name = data_file.stem
        with data_file.open() as file:
            try:
                # Get suitable parser and parse file to data
                parser = get_parser(file_extension)
                schema, data = parser(file)
            except (ParseError, ModuleNotFoundError) as e:
                errors.append(e)
                continue

            # Make Dataset instance and fill it by data
            dataset = Dataset(name=file_name, schema=schema)
            errors_ = dataset.from_iterable(data)
            datasets.append(dataset)
            errors.extend(errors_)

    dataset = make_merged_grouped_dataset(datasets)
    dataset.headers = tuple(h.replace("M", "MS") for h in dataset.headers)

    with open(result_file_name, mode='w') as file:
        dataset.write_as_table(file, '\t')

    if len(errors) > 0:
        print("Some errors were got during performing:")
        for error in errors:
            print(' — ', error)
    else:
        print("Done without errors.")


def make_merged_grouped_dataset(datasets: Iterable[Dataset], *grouping_headers: str) -> Dataset:
    """
    Concatenate data from given datasets to new one
    and group it's rows by given headers.
    """
    # Make merged dataset
    merged_schema = Dataset.merge_schemas(*datasets, intersection=True)
    merged_dataset = Dataset(name=f"merged", schema=merged_schema, use_defaults_for_kwargs=True)
    dataset_dict_iters = (ds.dict_iter() for ds in datasets)
    merged_dataset.from_iterable(*dataset_dict_iters, dict_row=True)
    # Group dataset by grouping_headers or headers starts from «D»
    if not grouping_headers:
        grouping_headers = filter(lambda h: h.startswith('D'), merged_dataset.headers)
    merged_dataset.group_by(*grouping_headers, operator=lambda a, b: a + b)
    return merged_dataset


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('result_file_name', type=str)
    parser.add_argument('data_folder_path', type=Path)
    args = parser.parse_args()
    main(**args.__dict__)
