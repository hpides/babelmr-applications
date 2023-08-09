from fs import local_write, local_read
import time

def reduce():
    TIMESTAMP = time.time()
    pairs = local_read()
    IMPORT_TIME = time.time()
    pairs = pairs.groupby(["item1", "item2"])
    reduced = (
        pairs["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
        .iloc[:100]
    )
    reduced = reduced[reduced["count"] > 50]
    CALC_TIME = time.time()

    local_write(
        reduced,
    )

    EXPORT_TIME = time.time()
    return {
        "timestamp": TIMESTAMP * 1000,
        "Import": IMPORT_TIME * 1000,
        "Calculation": CALC_TIME * 1000,
        "Export": EXPORT_TIME * 1000,
    }
