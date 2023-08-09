import pandas as pd
from fastparquet import ParquetFile
from concurrent.futures import ThreadPoolExecutor


def read_partitioned(file_desc, columns, opener):
    dfs = []
    input_file = f"{file_desc['bucket']}/{file_desc['key']}"
    partitions = file_desc["partitions"]
    pf = ParquetFile(input_file, open_with=opener)
    for i, rg in enumerate(pf):  # read only said partitions:
        if partitions == [] or i in partitions:
            dfs.append(rg.to_pandas(columns=columns))
    return pd.concat(dfs)


def wrapper_read_partitioned(columns, opener):
    def read_partitioned_wrapped(x):
        return read_partitioned(x, columns, opener)

    return read_partitioned_wrapped


def read_partitioned_parallel(file_descriptions, columns, opener):
    with ThreadPoolExecutor(max_workers=min(16, len(file_descriptions))) as executor:
        result_dfs = list(
            executor.map(
                wrapper_read_partitioned(columns, opener), file_descriptions
            )
        )
    return pd.concat(result_dfs)
