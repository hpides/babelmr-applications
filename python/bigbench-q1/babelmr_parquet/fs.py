from fastparquet import ParquetFile, write


def local_read(file="/tmp/in.parquet", filters=[]):
    pf = ParquetFile(file, open_with=open)
    df = pf.to_pandas(filters=filters)
    return df

def local_write(data, row_group_offsets=None):
    data.reset_index(drop=True, inplace=True)
    write(
        "/tmp/out.parquet",
        data,
        row_group_offsets=row_group_offsets,
        compression="zstd"
    )