from fs import local_read, local_write
import time


def map1():
    TIMESTAMP = time.time()
    store_sales = local_read("/tmp/in.parquet")
    items = local_read("/tmp/in2.parquet")

    IMPORT_TIME = time.time()

    filtered_items = items[items["i_category_id"].isin([1, 2, 3])].filter(["i_item_sk"])

    filtered_sales = store_sales[
        store_sales["ss_store_sk"].isin([10, 20, 33, 40, 50])
    ].filter(["ss_item_sk", "ss_ticket_number"])

    filtered_sales["item_id"] = filtered_sales["ss_item_sk"]
    joined = filtered_sales.set_index("ss_item_sk").join(
        filtered_items.set_index("i_item_sk"), how="inner"
    )

    CALC_TIME = time.time()
    local_write(
        joined
    )

    EXPORT_TIME = time.time()
    return {
        "timestamp": TIMESTAMP * 1000,
        "Import": IMPORT_TIME * 1000,
        "Calculation": CALC_TIME * 1000,
        "Export": EXPORT_TIME * 1000,
    }
