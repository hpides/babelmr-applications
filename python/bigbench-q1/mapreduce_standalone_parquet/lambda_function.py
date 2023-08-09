from map1 import map1
from map2 import map2
from reduce import reduce
import s3fs


def lambda_handler(event, context):
    s3 = s3fs.S3FileSystem()
    myopen = s3.open
    if event["mode"] == "map1":
        return map1(event, myopen)
    if event["mode"] == "map2":
        return map2(event, myopen)
    elif event["mode"] == "reduce":
        return reduce(event, myopen)


if __name__ == "__main__":
    lambda_handler(
        {
            "mode": "map1",
            "Stores": [10, 20, 33, 40, 50],
            "Categories": [1, 2, 3],
            "Items": [
                {
                    "bucket": "benchmark-data-sets",
                    "key": "tpcx-bb/standard/parquet/sf1/item/item_0.parquet",
                    "partitions": [],
                }
            ],
            "Sales": [
                {
                    "bucket": "benchmark-data-sets",
                    "key": "tpcx-bb/standard/parquet/sf1/store_sales/0.parquet",
                    "partitions": [],
                }
            ],
            "Export": {
                "bucket": "rsws2022",
                "key": "intermediates/abcdefg/map1_0.parquet",
                "number_partition" : 10
            },
        },
        None,
    )
