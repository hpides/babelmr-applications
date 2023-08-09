import pandas as pd

def local_read(filters=[]):
    df = pd.read_csv("/tmp/in.csv")
    return df

def local_write(data, row_group_offsets=None):
    data.reset_index(drop=True, inplace=True)
    data.to_csv("/tmp/out.csv", index=False)