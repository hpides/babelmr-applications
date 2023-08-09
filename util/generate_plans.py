import json
import random
import string
from pathlib import Path
import os
import math
from math import ceil
SCALEFACTORS = [1, 10 , 100, 1000]
CSV_SCALEFACTORS = [1, 10 , 100]

COLUMNS = [
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_shipdate",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
]

true = True
false = False


def generate_tpch_standalone_plan(sf: int, input_bucket: string = "benchmark-data-sets",
                                  input_key_prefix: string = "tpc-h/standard/parquet",
                                  input_file_type: string = "parquet",
                                  output_file_type: string = "parquet",
                                  output_bucket: string = "rsws2022",
                                  files_per_worker: int = 1):
    map_outputs = []
    dir_name = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
    )

    jobs = []
    number_input_files = sf
    if sf == 1000 and input_file_type == "parquet":
        number_input_files = 996
    if input_file_type == "csv":
      number_input_files *= 5
    for i in range(ceil(float(number_input_files) / files_per_worker)):
            imports = []
            for j in range(files_per_worker):
              number_of_import_file = (i * files_per_worker) + j
              partitions = [0,1,2] if sf == 100 and number_of_import_file == 99 else [0,1,2,3,4,5]
              import_ = {
                  "bucket": input_bucket,
                  "key":  f"{input_key_prefix}/sf{sf}/lineitem/{number_of_import_file}.{input_file_type}",
                  "partitions": partitions,
                  "columns": COLUMNS,
              }
              if number_of_import_file < number_input_files:
                imports.append(import_)
            output_file = (
                f"intermediate_results/map_sf{sf}/{dir_name}/{i}_map.{output_file_type}"
            )
            export_ = {
                "bucket": output_bucket,
                "key": output_file,
                "number_partitions": 4,
                "partition_on": ["l_returnflag", "l_linestatus"],
            }
            job = {"mode": "map", "Import": imports, "Export": export_}
            map_outputs.append(output_file)
            jobs.append(job)

    Imports = []
    for output in map_outputs:
        import_ = {"bucket": output_bucket, "key": output, "partitions": []}
        Imports.append(import_)
    reduce_job = {
        "mode": "reduce",
        "Import": Imports,
        "Export": {
            "bucket": output_bucket,
            "key": f"final_result/sf{sf}/{dir_name}/0.{output_file_type}",
        },
    }

    return [{"map": jobs, "reduce": [reduce_job]}, dir_name]

def generate_bb_q_1_standalone_plan(sf: int):
    map_1_outputs = []
    map_2_outputs = []
    dir_name = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
    )

    map_1_jobs = []
    map_2_jobs = []
    number_parquet_files = math.ceil(sf/2)

    number_mapper_2 = 1
    if sf > 1:
      number_mapper_2 = 5
    number_partitions = number_mapper_2

    number_partitions = 10
    if sf == 1:
      number_partitions = 1
    elif sf == 10:
      number_partitions = 1
    elif sf == 100:
      number_partitions = 10
    elif sf == 1000:
      number_partitions = 100

    files_per_mapper = min(number_parquet_files, 1)
    number_mapper = math.ceil(number_parquet_files / files_per_mapper)
    # for i in range(number_mapper):
    #   # map 1
    #   import_bucket = "rsws2022"
    #   export_bucket = "rsws2022"

    #   store_sale_import_keys = []
    #   for j in range(files_per_mapper):
    #
    #     store_sale_import_keys.append(f"tpcx_bb/parquet/sf{sf}/store_sales/{idx}.parquet")

    for i in range(number_mapper):
            item_import_keys = f"tpcx_bb/parquet/sf{sf}/item/0.parquet"


            sales_imports = []
            for j in range (files_per_mapper):
              idx = i * files_per_mapper + j
              store_sale_import_keys = f"tpcx_bb/parquet/sf{sf}/store_sales/{idx}.parquet"

            # Map pipeline 1
              sales_import_ = {
                "bucket": "rsws2022",
                "key": store_sale_import_keys,
                "partitions": [],
                "columns": ["ss_ticket_number", "ss_store_sk", "ss_item_sk"],
            }
              sales_imports.append(sales_import_)

            items_import_ = {
              "bucket": "rsws2022",
                "key": item_import_keys,
                "partitions": [],
                "columns": [  "i_category_id", "i_item_sk" ],
            }
            map_1_output_file = (
                f"intermediate_results/tpcx_bb/standalone/map_sf{sf}/{dir_name}/map1_{i}.parquet"
            )
            map_1_export_ = {
                "bucket": "rsws2022",
                "key": map_1_output_file,
                "number_partitions": number_partitions,
                "partition_on": ["ss_ticket_number"],
            }
            map_1_job = {"mode": "map1", "Sales": sales_imports, "Items": [items_import_], "Export": map_1_export_}
            map_1_outputs.append(map_1_output_file)
            map_1_jobs.append(map_1_job)

    # Map pipeline 2

    for i in range(number_partitions):
      map_2_imports = []
      for key in map_1_outputs:
        import_partitions = []
        if sf == 1:
          for x in range (number_mapper_2):
            import_partitions.append(x)
        else:
          import_partitions = [i]
        map_2_import = {
          "bucket": "rsws2022",
          "key": key,
          "partitions": import_partitions,
          "columns" : ["ss_ticket_number", "item_id"]
        }
        map_2_imports.append(map_2_import)
      map_2_output_file = (
          f"intermediate_results/tpcx_bb/standalone/map_sf{sf}/{dir_name}/map2_{i}.parquet"
      )
      map_2_export = {
        "bucket": "rsws2022",
        "key": map_2_output_file,
        "number_partitions": number_partitions,
        "partition_on" : ["item2"]
      }

      map_2_job = {"mode": "map2", "Import": map_2_imports, "Export": map_2_export }
      map_2_outputs.append(map_2_output_file)
      map_2_jobs.append(map_2_job)


    num_reduce_jobs = number_partitions
    reduce_jobs = []
    reduce_output_files = []
    for i in range(num_reduce_jobs):
        Imports = []
        reduce_output = f"final_result/tpcx_bb/standalone/sf{sf}/{dir_name}/{i}.parquet"
        for output in map_2_outputs:
            counted_pairs_import = {"bucket": "rsws2022", "key": output, "partitions": [i]}
            Imports.append(counted_pairs_import)
        reduce_job = {
            "mode": "reduce",
            "Import": Imports,
            "Export": {
                "bucket": "rsws2022",
                "key": reduce_output,
            },
        }
        reduce_output_files.append(reduce_output)
        reduce_jobs.append(reduce_job)


    Imports = []
    for output in reduce_output_files:
        counted_pairs_import = {"bucket": "rsws2022", "key": output, "partitions": []}
        Imports.append(counted_pairs_import)
    final_reduce_job = {
        "mode": "reduce",
        "Import": Imports,
        "Export": {
            "bucket": "rsws2022",
            "key": f"final_result/final/tpcx_bb/standalone/sf{sf}/{dir_name}/0.parquet",
        },
    }

    return [{"map1": map_1_jobs, "map2": map_2_jobs, "reduce" : reduce_jobs, "reduce2": [final_reduce_job]}, dir_name]

def skyrise_image_parquet(import_bucket : string, import_keys, export_bucket: string,
                          export_key : string, column_ids, rowgroup_ids,
                          export_format: string = "kParquet") ->  string:
  import_references = []
  print(import_keys)
  for key in import_keys:
    ref = {
      "bucket": import_bucket,
      "key": key,
      "etag": ""
    }
    import_references.append(ref)


  pqp_pipeline_fragment = {
  "root_operator_identity": "Export0x55555a3e6d60",
  "operators": {
    "Export0x55555a3e6d60": {
      "operator_type": "kExport",
      "operator_identity": "Export0x55555a3e6d60",
      "left_input_operator_identity": "import_operator",
      "bucket_name": export_bucket,
      "target_object_key": export_key,
      "export_format": export_format
    },
    "import_operator": {
      "operator_type": "kImport",
      "operator_identity": "import_operator",
      "column_ids": column_ids,
      "import_references": import_references,
      "import_options": {
        "parquet_format_reader_options": {
          "parse_dates_as_string": false,
          "include_columns": column_ids,
          "row_groups" : rowgroup_ids,
          "row_group_ids" : rowgroup_ids
        }
      }
    }
  }
  }
  return json.dumps(pqp_pipeline_fragment)


def generate_tpch_q1_shuffle_plan(sf: int, file_type: string = "parquet", input_key_prefix: string = "tpc-h/standard/parquet",
                                  import_bucket: string = "benchmark-data-sets", export_bucket: string = "rsws2022",
                                  files_per_worker: int = 1, file_format: string = "kParquet"):
  map_outputs = []

  dir_name = "".join(
      random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
  )


  shuffle_output = f"tpc-h/shuffled/sf_{sf}/{dir_name}/0.{file_type}"

  jobs = []
  number_files = sf

  if sf == 1000 and file_type == "parquet":
      number_files = 996
  if file_type == "csv":
    number_files *= 5

  for i in range(ceil(float(number_files) / files_per_worker)):
      imports = []
      for j in range(files_per_worker):
          number_of_import_file = (i * files_per_worker) + j
          partitions = [0,1,2] if sf == 100 and number_of_import_file == 99 else [0,1,2,3,4,5]
          import_ = {
              "bucket": import_bucket,
               "key": f"{input_key_prefix}/sf{sf}/lineitem/{number_of_import_file}.{file_type}",
              "partitions": partitions,
              "columns": COLUMNS,
          }
          if number_of_import_file < number_files:
            imports.append(import_)
      output_file = (
          f"intermediate_results/map_sf{sf}/{dir_name}/{i}_map.{file_type}"
      )
      export_ = {
          "bucket": export_bucket,
          "key": output_file,
          "number_partitions": 1,
          "partition_on": ["l_returnflag", "l_linestatus"],
      }
      job = {"mode": "map", "Import": imports, "Export": export_}
      map_outputs.append(output_file)
      jobs.append(job)

  shuffle_job = {
    "mode" : "shuffle_tpc_h_q1",
    "parallelism" : 1,
    "pqp_pipeline_fragment" : skyrise_image_parquet_partitioning(export_bucket, map_outputs, export_bucket, shuffle_output , [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [0], [0,1], 1, file_format)
  }

  # Imports = []
  # for output in map_outputs:
  import_ = {"bucket": export_bucket, "key": shuffle_output, "partitions": []}
      # Imports.append(import_)
  reduce_job = {
      "mode": "reduce",
      "Import": [import_],
      "Export": {
          "bucket": export_bucket,
          "key": f"final_result/sf{sf}/{dir_name}/0.{file_type}",
      },
  }

  return [{"map": jobs, "reduce": [reduce_job], "shuffle" : [shuffle_job]}, dir_name]



def skyrise_reader_options(include_columns, export_type ,row_group_ids):
  if export_type == "kParquet":
    return {
      "parquet_format_reader_options": {
          "parse_dates_as_string": false,
          "include_columns": include_columns,
          "row_groups" : row_group_ids,
          "row_group_ids" : row_group_ids
        }
    }
  elif export_type == "kCsv":
    return {
      "csv_format_reader_options": {
          "read_buffer_size": 20971520,
          "delimiter": ",",
          "guess_delimiter": false,
          "guess_has_header": false,
          "guess_has_types": false,
          "has_header": true,
          "has_types": false,
          "include_columns" : include_columns,
          "expected_schema": [
                        {
                            "name": "l_returnflag",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_linestatus",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "sum_qty",
                            "data_type": "kLong",
                            "nullable": false
                        },
                        {
                            "name": "sum_base_price",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "sum_disc_price",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "sum_charge",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "avg_qty",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "avg_price",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "avg_disc",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "count_order",
                            "data_type": "kLong",
                            "nullable": false
                        }
                    ]
        }
    }

def skyrise_image_parquet_partitioning(import_bucket : string, import_keys, export_bucket: string,
                                       export_key : string, column_ids, rowgroup_ids, partition_column_ids,
                                       partition_count, export_format: string = "kParquet") ->  string:
  import_references = []
  for key in import_keys:
    ref = {
      "bucket": import_bucket,
      "key": key,
      "etag": ""
    }
    import_references.append(ref)


  pqp_pipeline_fragment = {
  "root_operator_identity": "Export0x55555a3e6d60",
  "operators": {
    "Export0x55555a3e6d60": {
      "operator_type": "kExport",
      "operator_identity": "Export0x55555a3e6d60",
      "left_input_operator_identity": "Partition0x557473b9dac0",
      "bucket_name": export_bucket,
      "target_object_key": export_key,
      "export_format": export_format
    },
    "Partition0x557473b9dac0": {
      "operator_type": "kPartition",
      "operator_identity": "Partition0x557473b9dac0",
      "left_input_operator_identity": "import_operator",
      "partitioning_function": {
        "type": "kHash",
        "partition_column_ids": partition_column_ids,
        "partition_count": partition_count
      }
    },

    "import_operator": {
      "operator_type": "kImport",
      "operator_identity": "import_operator",
      "column_ids": column_ids,
      "import_references": import_references,
      "import_options": skyrise_reader_options(column_ids, export_format, rowgroup_ids)
    }
  }
  }
  return json.dumps(pqp_pipeline_fragment)


def generate_tpcx_bb_image_plan(sf: int):
  map_1_outputs = []
  map_2_outputs = []
  dir_name = "".join(
      random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
  )

  map_1_jobs = []
  map_2_jobs = []
  number_parquet_files = math.ceil(sf / 2)

  # number_partitions = number_parquet_files
  # if sf == 1:
  #   number_partitions = 1
  # elif sf == 10:
  #   number_partitions = 5
  # elif sf == 100:
  #   number_partitions = 10

  number_partitions = 10
  if sf == 1:
    number_partitions = 1
  elif sf == 10:
    number_partitions = 1
  elif sf == 100:
    number_partitions = 10
  elif sf == 1000:
    number_partitions = 100

  files_per_mapper = min(number_parquet_files, 1)
  number_mapper = math.ceil(number_parquet_files / files_per_mapper)
  for i in range(number_mapper):
    # map 1
    import_bucket = "rsws2022"
    export_bucket = "rsws2022"

    store_sale_import_keys = []
    for j in range(files_per_mapper):
      idx = i * files_per_mapper + j
      store_sale_import_keys.append(f"tpcx_bb/parquet/sf{sf}/store_sales/{idx}.parquet")

    # store_sale_import_keys =[ f"tpcx_bb/parquet/sf{sf}/store_sales/{i}.parquet"  ]


    item_import_keys = [f"tpcx_bb/parquet/sf{sf}/item/0.parquet" ]

    map_1_output_file = f"intermediate_results/tpcx_bb/image/map_sf{sf}/{dir_name}/map1_{i}.parquet"

    map_1_job = {
      "import": {
        "parallelism": 1,
        "mode": "udf_import",
        "pqp_pipeline_fragment": skyrise_image_parquet(import_bucket, store_sale_import_keys, "/tmp/","in.parquet",[2,7,9], [0])
      },
      "import2": {
        "parallelism": 1,
        "mode": "udf_import",
        "pqp_pipeline_fragment": skyrise_image_parquet(import_bucket, item_import_keys, "/tmp/","in2.parquet",[0,11], [0])
      },
      "mode": "map1",
      "export": {
        "parallelism": 1,
        "mode": "udf_export",
        "pqp_pipeline_fragment": skyrise_image_parquet_partitioning("/tmp/", ["out.parquet"], export_bucket, map_1_output_file ,[0,1], [0], [0], number_partitions)
      }
    }
    map_1_outputs.append(map_1_output_file)
    map_1_jobs.append(map_1_job)

  for i in range(number_partitions):
    map_2_imports = []
    map_2_output_file = f"intermediate_results/tpcx_bb/image/map_sf{sf}/{dir_name}/map2_{i}.parquet"

    for key in map_1_outputs:
      map_2_imports.append(key)

    map_2_job = {
        "import": {
          "parallelism": 1,
          "mode": "udf_import",
          "pqp_pipeline_fragment": skyrise_image_parquet(export_bucket, map_1_outputs, "/tmp/","in.parquet",[0,1], [i])
        },
        "mode": "map2",
        "export": {
          "parallelism": 1,
          "mode": "udf_export",
          # "pqp_pipeline_fragment": skyrise_image_parquet_partitioning("/tmp/", ["out.parquet"], export_bucket, map_2_output_file ,[0,1,2], [0], 1, number_partitions)
          "pqp_pipeline_fragment": skyrise_image_parquet_partitioning("/tmp/", ["out.parquet"], export_bucket, map_2_output_file ,[0,1,2], [0], [1,2], number_partitions)

        }
    }
    map_2_outputs.append(map_2_output_file)
    map_2_jobs.append(map_2_job)


  num_reduce_jobs = number_partitions
  reduce_jobs = []
  reduce_output_files = []
  for i in range(num_reduce_jobs):
    reduce_output_file = f"final_result/tpcx_bb/image/sf{sf}/{dir_name}/{i}.parquet"
    reduce_output_files.append(reduce_output_file)
    reduce_job = {
        "import": {
          "parallelism": 1,
          "mode": "udf_import",
          "pqp_pipeline_fragment": skyrise_image_parquet(export_bucket, map_2_outputs, "/tmp/","in.parquet",[0,1,2],[i])
        },
        "mode": "reduce",
        "export": {
          "parallelism": 1,
          "mode": "udf_export",
          "pqp_pipeline_fragment": skyrise_image_parquet("/tmp/", ["out.parquet"], export_bucket, reduce_output_file ,[0,1,2], [0])
        }
    }
    reduce_jobs.append(reduce_job)

  reduce_output_file = f"final_result/final/tpcx_bb/image/sf{sf}/{dir_name}/{i}.parquet"

  reduce_job_2 = {
        "import": {
          "parallelism": 1,
          "mode": "udf_import",
          "pqp_pipeline_fragment": skyrise_image_parquet(export_bucket, reduce_output_files, "/tmp/","in.parquet",[0,1,2],[0])
        },
        "mode": "reduce",
        "export": {
          "parallelism": 1,
          "mode": "udf_export",
          "pqp_pipeline_fragment": skyrise_image_parquet("/tmp/", ["out.parquet"], export_bucket, reduce_output_file ,[0,1,2], [0])
        }
    }

  return [{"map1": map_1_jobs, "map2": map_2_jobs, "reduce": reduce_jobs, "reduce2": [reduce_job_2]}, dir_name]


def import_json_parquet_tpch_q1(idx : int, sf : int, import_references, local_export_filetype: string = "parquet",
                                local_export_format: string = "kParquet", import_bucket: string = "benchmark-data-sets",
                                import_key_prefix: string = "tpc-h/standard/parquet", import_file_type = "parquet"):
    columns= [4,5,6,7,8,9,10]
    pqp_pipeline_fragment = {
      "root_operator_identity": "Export0x55555a3e6d60",
      "operators": {
        "Export0x55555a3e6d60": {
          "operator_type": "kExport",
          "operator_identity": "Export0x55555a3e6d60",
          "left_input_operator_identity": "import_operator",
          "bucket_name": "/tmp/",
          "target_object_key": f"in.{local_export_filetype}",
          "export_format": f"{local_export_format}"
        },
        "import_operator": {
          "operator_type": "kImport",
          "operator_identity": "import_operator",
          "column_ids": columns,
          "import_references": import_references,
          "import_options": skyrise_tpch_import_options(import_file_type, "original", columns)
        }
      }
    }
    return pqp_pipeline_fragment


def skyrise_tpch_import_options(file_type, schema = "modified", include_columns=[0,1,2,3,4,5,6,7,8,9]):
  if file_type == "parquet":
    return {
        "parquet_format_reader_options": {
          "parse_dates_as_string": false,
          "include_columns": include_columns
        }
      }
  if file_type == "csv":
    if schema == "original":
      return {
        "csv_format_reader_options" : {
          "read_buffer_size": 20971520,
                    "delimiter": ",",
                    "guess_delimiter": false,
                    "guess_has_header": false,
                    "guess_has_types": false,
                    "has_header": true,
                    "has_types": false,
                    "include_columns": include_columns,
                    "expected_schema": [
                        {
                            "name": "l_orderkey",
                            "data_type": "kInt",
                            "nullable": false
                        },
                        {
                            "name": "l_partkey",
                            "data_type": "kInt",
                            "nullable": false
                        },
                        {
                            "name": "l_suppkey",
                            "data_type": "kInt",
                            "nullable": false
                        },
                        {
                            "name": "l_linenumber",
                            "data_type": "kInt",
                            "nullable": false
                        },
                        {
                            "name": "l_quantity",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "l_extendedprice",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "l_discount",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "l_tax",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "l_returnflag",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_linestatus",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_shipdate",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_commitdate",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_receiptdate",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_shipinstruct",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_shipmode",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_comment",
                            "data_type": "kString",
                            "nullable": false
                        }
                    ]
        }
      }
    else:
      return {
      "csv_format_reader_options": {
                    "read_buffer_size": 20971520,
                    "delimiter": ",",
                    "guess_delimiter": false,
                    "guess_has_header": false,
                    "guess_has_types": false,
                    "has_header": true,
                    "has_types": false,
                    "include_columns": include_columns,
                    "expected_schema": [
                        {
                            "name": "l_returnflag",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "l_linestatus",
                            "data_type": "kString",
                            "nullable": false
                        },
                        {
                            "name": "sum_qty",
                            "data_type": "kLong",
                            "nullable": false
                        },
                        {
                            "name": "sum_base_price",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "sum_disc_price",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "sum_charge",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "avg_qty",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "avg_price",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "avg_disc",
                            "data_type": "kDouble",
                            "nullable": false
                        },
                        {
                            "name": "count_order",
                            "data_type": "kLong",
                            "nullable": false
                        }
                    ]
                }
      }

def export_json_parquet_tpch_q1(dirname : str, idx : int, sf : int, export_bucket: str = "rsws2022",
                                local_import_file_type: string = "parquet", export_file_type : string = "parquet",
                                export_file_format: string = "kParquet"):
    export_dir = f"intermediates/{dirname}/{idx}.{export_file_type}"
    # print(export_dir)
    columns = [0,1,2,3,4,5,6,7,8,9]
    pqp_pipeline_fragment = {
  "root_operator_identity": "Export0x55555a3eb510",
  "operators": {
    "Export0x55555a3eb510": {
      "operator_type": "kExport",
      "operator_identity": "Export0x55555a3eb510",
      "left_input_operator_identity": "import_operator",
      "bucket_name": export_bucket,
      "target_object_key": export_dir,
      "export_format": export_file_format
    },
    "import_operator": {
      "operator_type": "kImport",
      "operator_identity": "import_operator",
      "column_ids": columns,
      "import_references": [
        {
          "bucket": "/tmp/",
          "key": f"out.{local_import_file_type}",
          "etag": ""
        }
      ],
      "import_options": skyrise_tpch_import_options(local_import_file_type, include_columns=columns)
    }
  }
}

    return [pqp_pipeline_fragment, export_dir]

def reduce_import_json_parquet_tpch_q1(key_names, local_file_type = "parquet", local_file_format = "kParquet",
                                       import_file_type = "parquet", import_bucket = "rsws2022"):
    import_references = []
    for name in key_names:
        ref = {
          "bucket": import_bucket,
          "key": name,
          "etag": ""
        }
        import_references.append(ref)

    columns= [0,1,2,3,4,5,6,7,8,9]
    # write one rowgroup for many imports:
    if len(import_references) < 50:
      pqp_pipeline_fragment = {
        "root_operator_identity": "Export0x55555a3e6d60",
        "operators": {
          "Export0x55555a3e6d60": {
            "operator_type": "kExport",
            "operator_identity": "Export0x55555a3e6d60",
            "left_input_operator_identity": "import_operator",
            "bucket_name": "/tmp/",
            "target_object_key": f"in.{local_file_type}",
            "export_format": local_file_format
          },
          "import_operator": {
            "operator_type": "kImport",
            "operator_identity": "import_operator",
            "column_ids": columns,
            "import_references": import_references,
            "import_options": skyrise_tpch_import_options(import_file_type, include_columns=columns)
          }
        }
      }
    else:
      pqp_pipeline_fragment = {
        "root_operator_identity": "Export0x55555a3e6d60",
        "operators": {
          "Export0x55555a3e6d60": {
            "operator_type": "kExport",
            "operator_identity": "Export0x55555a3e6d60",
            "left_input_operator_identity": "Partition0x557473b9dac0",
            "bucket_name": "/tmp/",
            "target_object_key": f"in.{local_file_type}",
            "export_format": local_file_format
          },
          "Partition0x557473b9dac0": {
            "operator_type": "kPartition",
            "operator_identity": "Partition0x557473b9dac0",
            "left_input_operator_identity": "import_operator",
            "partitioning_function": {
              "type": "kHash",
              "partition_column_ids": 0,
              "partition_count": 1
            }
          },
          "import_operator": {
            "operator_type": "kImport",
            "operator_identity": "import_operator",
            "column_ids": columns,
            "import_references": import_references,
            "import_options": skyrise_tpch_import_options(import_file_type, include_columns=columns)
          }
        }
      }
    return pqp_pipeline_fragment

def generate_tpc_h_image_plan(sf : int,  import_file_type="parquet", import_bucket: string = "benchmark-data-sets", import_key_prefix: string = "tpc-h/standard/parquet",
                              local_write_filetype: string = "parquet", local_read_format: string = "kParquet",
                              local_read_filetype: string = "parquet",
                              intermediate_bucket: string = "rsws2022", export_bucket: string = "rsws2022",
                              export_file_type="parquet", export_file_format="kParquet",files_per_worker: int = 1):
    map_outputs = []
    dir_name = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
    )
    jobs = []
    number_files = sf
    if sf == 1000 and import_file_type=="parquet":
        number_files = 996
    if import_file_type=="csv":
        number_files *= 5
    for i in range(ceil(float(number_files) / files_per_worker)):
        imports = []
        for j in range(files_per_worker):
          file_id = (i * files_per_worker) + j
          if file_id < number_files:
            imports.append({
              "bucket": import_bucket,
              "key": f"{import_key_prefix}/sf{sf}/lineitem/{file_id}.{import_file_type}",
              "etag": ""
            })
        pqp_pipeline_fragment_import = json.dumps(import_json_parquet_tpch_q1(i, sf, imports,
                                                                              local_export_filetype=local_write_filetype,
                                                                              local_export_format=local_read_format,
                                                                              import_bucket=import_bucket,
                                                                              import_key_prefix=import_key_prefix,
                                                                              import_file_type=import_file_type))
        [pqp_pipeline_fragment_export, output_file] = export_json_parquet_tpch_q1(dir_name, i, sf,
                                                                                  local_import_file_type=local_read_filetype,
                                                                                  export_bucket=intermediate_bucket,
                                                                                  export_file_format=export_file_format,
                                                                                  export_file_type=export_file_type)
        export_json =  json.dumps(pqp_pipeline_fragment_export)

        job = {
        "import": {
            "parallelism": 1,
            "mode": "udf_import",
            "pqp_pipeline_fragment": f"{pqp_pipeline_fragment_import}"
        },
        "mode" : "map",
        "export": {
            "parallelism": 1,
            "mode": "udf_export",
            "pqp_pipeline_fragment": f"{export_json}"
            }
        }
        map_outputs.append(output_file)
        jobs.append(job)

    import_fragment = json.dumps(reduce_import_json_parquet_tpch_q1(map_outputs,
                                                                    import_bucket=intermediate_bucket,
                                                                    import_file_type=export_file_type,
                                                                    local_file_type=local_write_filetype,
                                                                    local_file_format=local_read_format))
    export_fragment = json.dumps(export_json_parquet_tpch_q1(dir_name + "final", sf, 0,
                                                            export_bucket=export_bucket,
                                                            local_import_file_type=local_write_filetype,
                                                            export_file_type=export_file_type,
                                                            export_file_format=export_file_format)[0])

    reduce_job = {
        "import": {
            "parallelism": 1,
            "mode": "udf_import",
            "pqp_pipeline_fragment": f"{import_fragment}"
        },
        "mode" : "reduce",
        "export": {
            "parallelism": 1,
            "mode": "udf_export",
            "pqp_pipeline_fragment": f"{export_fragment}"
            }
        }

    return [{"map": jobs, "reduce": [reduce_job]}, dir_name]


def save(query, mode, step_name, plan, file_type="parquet"):
  folder = Path("plans") / query / mode / file_type
  os.makedirs(folder, exist_ok=True)
  with open(folder / step_name, "w") as fp:
    json.dump(plan, fp)


def generate_tpcx_bb_synthetic_shuffle(sf : int):
  dir_name = "".join(
    random.choice(string.ascii_uppercase + string.digits) for _ in range(5)
  )

  number_parquet_files = math.ceil(sf/2)
  export_bucket = "rsws2022"
  number_mapper_2 = 1
  if sf > 1:
    number_mapper_2 = 5
  number_partitions = number_mapper_2

  number_partitions = 10
  if sf == 1:
    number_partitions = 1
  elif sf == 10:
    number_partitions = 1
  elif sf == 100:
    number_partitions = 10
  elif sf == 1000:
    number_partitions = 100

  files_per_mapper = min(number_parquet_files, 1)
  number_mapper = math.ceil(number_parquet_files / files_per_mapper)

  shuffle_1_n = 1
  shuffle_2_n = 1

  if sf == 100:
    shuffle_1_n  = 2
    shuffle_2_n = 2

  if sf == 1000:
    shuffle_1_n  = 20
    shuffle_2_n = 20


  shuffle_1_output_files = [f"shuffled/{dir_name}/output_1_{i}.parquet" for i in range(shuffle_1_n)]
  shuffle_2_output_files = [f"shuffled/{dir_name}/output_2_{i}.parquet" for i in range(shuffle_2_n)]

  map_1_outputs = []
  map_2_outputs = []

  map_1_jobs = []
  map_2_jobs = []

  # Map 1
  for i in range(number_mapper):
          item_import_keys = f"tpcx_bb/parquet/sf{sf}/item/0.parquet"


          sales_imports = []
          for j in range (files_per_mapper):
            idx = i * files_per_mapper + j
            store_sale_import_keys = f"tpcx_bb/parquet/sf{sf}/store_sales/{idx}.parquet"

          # Map pipeline 1
            sales_import_ = {
              "bucket": "rsws2022",
              "key": store_sale_import_keys,
              "partitions": [],
              "columns": ["ss_ticket_number", "ss_store_sk", "ss_item_sk"],
          }
            sales_imports.append(sales_import_)

          items_import_ = {
            "bucket": "rsws2022",
              "key": item_import_keys,
              "partitions": [],
              "columns": [  "i_category_id", "i_item_sk" ],
          }
          map_1_output_file = (
              f"intermediate_results/tpcx_bb/standalone/map_sf{sf}/{dir_name}/map1_{i}.parquet"
          )
          map_1_export_ = {
              "bucket": "rsws2022",
              "key": map_1_output_file,
              "number_partitions": 0,
              "partition_on": [],
          }
          map_1_job = {"mode": "map1", "Sales": sales_imports, "Items": [items_import_], "Export": map_1_export_}
          map_1_outputs.append(map_1_output_file)
          map_1_jobs.append(map_1_job)

  chunk_size = ceil(len(map_1_outputs) / shuffle_1_n)
  chunked_map_1_outputs = [map_1_outputs[x:x+chunk_size] for x in range(0, len(map_1_outputs), chunk_size)]
  shuffle_jobs = [
    {
    "mode" : "shuffle1",
    "parallelism" : 1,
    "pqp_pipeline_fragment" : skyrise_image_parquet_partitioning(export_bucket, chunked_map_1_outputs[i], export_bucket, shuffle_1_output_files[i] ,[0,1], [0], [0], number_partitions)
    } for i in range(shuffle_1_n)
  ]

  # Map 2
  for i in range(number_partitions):
    map_2_imports = [
      {
        "bucket": "rsws2022",
        "key": shuffle_1_output_files[i],
        "partitions": [i],
        "columns" : ["ss_ticket_number", "item_id"]
      } for i in range(shuffle_1_n)
    ]
    map_2_output_file = (
        f"intermediate_results/tpcx_bb/standalone/map_sf{sf}/{dir_name}/map2_{i}.parquet"
    )
    map_2_export = {
      "bucket": "rsws2022",
      "key": map_2_output_file,
      "number_partitions": 0,
      "partition_on" : []
    }

    map_2_job = {"mode": "map2", "Import": map_2_imports, "Export": map_2_export }
    map_2_outputs.append(map_2_output_file)
    map_2_jobs.append(map_2_job)

  chunk_size_2 = ceil(len(map_2_outputs) / shuffle_2_n)
  chunked_map_2_outputs = [map_2_outputs[x:x+chunk_size_2] for x in range(0, len(map_2_outputs), chunk_size_2)]
  shuffle_jobs_2 = [
    {
    "mode" : "shuffle2",
    "parallelism" : 1,
    "pqp_pipeline_fragment" : skyrise_image_parquet_partitioning(export_bucket, chunked_map_2_outputs[i], export_bucket, shuffle_2_output_files[i] ,[0,1,2], [0], [1,2], number_partitions)
    } for i in range(shuffle_2_n)
  ]

  num_reduce_jobs = number_partitions
  reduce_jobs = []
  reduce_output_files = []
  for i in range(num_reduce_jobs):
      # Imports = []
      reduce_output = f"final_result/tpcx_bb/standalone/sf{sf}/{dir_name}/{i}.parquet"
      counted_pairs_imports = [
        {"bucket": "rsws2022", "key": shuffle_2_output_files[j], "partitions": [i]}
        for j in range(shuffle_2_n)
      ]
      reduce_job = {
          "mode": "reduce",
          "Import": counted_pairs_imports,
          "Export": {
              "bucket": "rsws2022",
              "key": reduce_output,
          },
      }
      reduce_output_files.append(reduce_output)
      reduce_jobs.append(reduce_job)


  Imports = []
  for output in reduce_output_files:
      counted_pairs_import = {"bucket": "rsws2022", "key": output, "partitions": []}
      Imports.append(counted_pairs_import)
  final_reduce_job = {
      "mode": "reduce",
      "Import": Imports,
      "Export": {
          "bucket": "rsws2022",
          "key": f"final_result/final/tpcx_bb/standalone/sf{sf}/{dir_name}/0.parquet",
      },
  }

  return [{"map1": map_1_jobs, "map2": map_2_jobs, "reduce" : reduce_jobs, "reduce2": [final_reduce_job], "shuffle1": shuffle_jobs, "shuffle2": shuffle_jobs_2}, dir_name]


if __name__ == "__main__":
    for sf in SCALEFACTORS:

        # Generate plans for tpc-h-q1 standalones

        [plans, dir_name] = generate_tpch_standalone_plan(sf)

        map_invokes = {"jobs": plans["map"]}
        reduce_invokes = {"jobs": plans["reduce"]}

        save("tpc-h-q1", "standalone", f"maps_{sf}.json", map_invokes)
        save("tpc-h-q1", "standalone", f"reduces_{sf}.json", reduce_invokes)

        # Generate plans for tpc-h-q1 BabelMR

        [plans, dir_name] = generate_tpc_h_image_plan(sf,
                                                      import_file_type="parquet",
                                                      import_bucket="benchmark-data-sets",
                                                      import_key_prefix="tpc-h/standard/parquet",
                                                      local_write_filetype="parquet",
                                                      local_read_format="kParquet",
                                                      local_read_filetype="parquet",
                                                      intermediate_bucket="rsws2022",
                                                      export_bucket="rsws2022",
                                                      export_file_type="parquet",
                                                      export_file_format="kParquet")


        map_invokes = {"jobs": plans["map"]}
        reduce_invokes = {"jobs": plans["reduce"]}

        save("tpc-h-q1", "babelmr", f"maps_{sf}.json", map_invokes)
        save("tpc-h-q1", "babelmr", f"reduces_{sf}.json", reduce_invokes)

        # Generate plans for tpc-h-q1 with intermediate shuffle

        [plans, dir_name] = generate_tpch_q1_shuffle_plan(sf)
        map_invokes = {"jobs": plans["map"]}
        reduce_invokes = {"jobs": plans["reduce"]}
        shuffle_invokes = {"jobs": plans["shuffle"]}

        save("tpc-h-q1", "synthetic-shuffle", f"maps_{sf}.json", map_invokes)
        save("tpc-h-q1", "synthetic-shuffle", f"reduces_{sf}.json", reduce_invokes)
        save("tpc-h-q1", "synthetic-shuffle", f"shuffle_{sf}.json", shuffle_invokes)

        # Generate plans for bb-q1 standalones

        [plans, dir_name] = generate_bb_q_1_standalone_plan(sf)

        map_1_invokes = {"jobs": plans["map1"]}
        map_2_invokes = {"jobs": plans["map2"]}
        reduce_invokes = {"jobs": plans["reduce"]}
        reduce_2_invokes = {"jobs": plans["reduce2"]}

        save("bb-q1", "standalone", f"maps_1_{sf}.json", map_1_invokes)
        save("bb-q1", "standalone", f"maps_2_{sf}.json", map_2_invokes)
        save("bb-q1", "standalone", f"reduces_{sf}.json", reduce_invokes)
        save("bb-q1", "standalone", f"reduces_2_{sf}.json", reduce_2_invokes)

        [plans, dir_name] = generate_tpcx_bb_image_plan(sf)
        map_1_invokes = {"jobs": plans["map1"]}
        map_2_invokes = {"jobs": plans["map2"]}
        reduce_invokes = {"jobs": plans["reduce"]}
        reduce_2_invokes = {"jobs": plans["reduce2"]}


        save("bb-q1", "babelmr", f"maps_1_{sf}.json", map_1_invokes)
        save("bb-q1", "babelmr", f"maps_2_{sf}.json", map_2_invokes)
        save("bb-q1", "babelmr", f"reduces_{sf}.json", reduce_invokes)
        save("bb-q1", "babelmr", f"reduces_2_{sf}.json", reduce_2_invokes)

        [plans, dir_name] = generate_tpcx_bb_synthetic_shuffle(sf)
        map_1_invokes = {"jobs": plans["map1"]}
        synthetic_shuffle = {"jobs": plans["shuffle1"]}
        map_2_invokes = {"jobs": plans["map2"]}
        reduce_invokes = {"jobs": plans["reduce"]}

        synthetic_shuffle2 = {"jobs": plans["shuffle2"]}
        reduce_2_invokes = {"jobs": plans["reduce2"]}
        save("bb-q1", "synthetic-shuffle", f"maps_1_{sf}.json", map_1_invokes)
        save("bb-q1", "synthetic-shuffle", f"maps_2_{sf}.json", map_2_invokes)
        save("bb-q1", "synthetic-shuffle", f"reduces_{sf}.json", reduce_invokes)
        save("bb-q1", "synthetic-shuffle", f"reduces_2_{sf}.json", reduce_2_invokes)
        save("bb-q1", "synthetic-shuffle", f"shuffle_1_{sf}.json", synthetic_shuffle)
        save("bb-q1", "synthetic-shuffle", f"shuffle_2_{sf}.json", synthetic_shuffle2)

    # generate csv plans
    for sf in SCALEFACTORS:

        # Generate plans for tpc-h-q1 standalones

        [plans, dir_name] = generate_tpch_standalone_plan(sf,
                                                          input_bucket="rsws2022",
                                                          input_key_prefix="tpch-csv",
                                                          input_file_type="csv",
                                                          output_file_type="csv",
                                                          output_bucket="rsws2022",
                                                          files_per_worker=2)

        map_invokes = {"jobs": plans["map"]}
        reduce_invokes = {"jobs": plans["reduce"]}

        save("tpc-h-q1", "standalone", f"maps_{sf}.json", map_invokes, "csv")
        save("tpc-h-q1", "standalone", f"reduces_{sf}.json", reduce_invokes, "csv")

        # Generate plans for tpc-h-q1 BabelMR

        [plans, dir_name] = generate_tpc_h_image_plan(sf,
                                                      import_file_type="csv",
                                                      import_bucket="rsws2022",
                                                      import_key_prefix="tpch-csv",
                                                      local_write_filetype="csv",
                                                      local_read_format="kCsv",
                                                      local_read_filetype="csv",
                                                      intermediate_bucket="rsws2022",
                                                      export_bucket="rsws2022",
                                                      export_file_type="csv",
                                                      export_file_format="kCsv",
                                                      files_per_worker=2)


        map_invokes = {"jobs": plans["map"]}
        reduce_invokes = {"jobs": plans["reduce"]}

        save("tpc-h-q1", "babelmr", f"maps_{sf}.json", map_invokes, "csv")
        save("tpc-h-q1", "babelmr", f"reduces_{sf}.json", reduce_invokes, "csv")

        # Generate plans for tpc-h-q1 with intermediate shuffle

        [plans, dir_name] = generate_tpch_q1_shuffle_plan(sf,
                                                          file_type="csv",
                                                          input_key_prefix="tpch-csv",
                                                          import_bucket="rsws2022",
                                                          export_bucket="rsws2022",
                                                          files_per_worker=2,
                                                          file_format="kCsv")
        map_invokes = {"jobs": plans["map"]}
        reduce_invokes = {"jobs": plans["reduce"]}
        shuffle_invokes = {"jobs": plans["shuffle"]}

        save("tpc-h-q1", "synthetic-shuffle", f"maps_{sf}.json", map_invokes, "csv")
        save("tpc-h-q1", "synthetic-shuffle", f"reduces_{sf}.json", reduce_invokes, "csv")
        save("tpc-h-q1", "synthetic-shuffle", f"shuffle_{sf}.json", shuffle_invokes, "csv")
