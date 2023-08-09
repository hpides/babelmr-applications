import sys
import time
import json
from os import path, environ

from io import StringIO
from contextlib import redirect_stdout
import subprocess

pipeline_directory = path.join(
    path.dirname(path.dirname(path.abspath(__file__))), "testqueries"
)

# C:\Python310\python.exe       -> python
# /root/anaconda3/bin/python3   -> python3
# /root/anaconda3/bin/python    -> python
PYTHON = path.basename(sys.executable).split(".")[0]
REPETITIONS=11
SCALE_FACTORS=[1, 10, 100, 1000]

JOBS= [
    ##### TPC-H-Q1 #####

    # Measure TPC-H-Q1 Standalone parquet python
    {
        "language" : "python",
        "function" : "bench_tpchq1_standalone_parquet_python",
        "mode" : "standalone",
        "query": "tpc-h-q1",
        "format": "parquet",
        "save": "True",
        "plot": "False"
    },
    # Measure TPC-H-Q1 Standalone csv python
    {
        "language" : "python",
        "function" : "bench_tpchq1_standalone_csv_python",
        "mode" : "standalone",
        "query": "tpc-h-q1",
        "format": "csv",
        "save": "True",
        "plot": "False"
    },
    # Measure TPC-H-Q1 synthetic shuffle parquet python
    {
        "language" : "python",
        "function" : "bench_tpchq1_standalone_parquet_python",
        "mode" : "synthetic-shuffle",
        "query": "tpc-h-q1-synth",
        "format":"parquet",
        "save":"True",
        "plot":"False"
    },
    # Measure TPC-H-Q1 BabelMr parquet python
    {
        "language" : "python",
        "function" : "bench_tpchq1_babelmr_parquet_python",
        "mode" : "babelmr",
        "query": "tpc-h-q1",
        "format": "parquet",
        "save": "True",
        "plot": "False"
    },
    # Measure TPC-H-Q1 BabelMr csv python
    {
        "language" : "python",
        "function" : "bench_tpchq1_babelmr_csv_python",
        "mode" : "babelmr",
        "query": "tpc-h-q1",
        "format": "csv",
        "save": "True",
        "plot": "False"
    },
     # Measure TPC-H-Q1 BabelMr csv GO!!!
    {
        "language" : "go",
        "function" : "bench_babelmr_go_tpch",
        "mode" : "babelmr",
        "query": "tpc-h-q1",
        "format": "csv",
        "save": "True",
        "plot": "False"
    },
    # Measure TPC-H-Q1 Standalone parquet c#
    {
       "language" : "c#",
       "function" : "c_sharp_udf_tpch_q1_standalone",
       "mode" : "standalone",
       "query":"tpc-h-q1",
       "format":"parquet",
       "save":"True",
       "plot":"False"
    },
    # Measure TPC-H-Q1 synthetic shuffle parquet c#
    {
       "language" : "c#",
       "function" : "c_sharp_udf_tpch_q1_synthetic_shuffle",
       "mode" : "synthetic-shuffle",
       "query":"tpc-h-q1-synth",
       "format":"parquet",
       "save":"True",
       "plot":"False"
    },
    # Measure TPC-H-Q1 BabelMr parquet c#
    {
       "language" : "c#",
       "function" : "babel_mr_csharp_tpch_parquet",
       "mode" : "babelmr",
       "query":"tpc-h-q1",
       "format":"parquet",
       "save":"True",
       "plot":"False"
    },

    ##### BB-Q1 #####

    # Measure BB-Q1 Standalone parquet c#
    {
       "language" : "c#",
       "function" : "c_sharp_udf_bb_q1_standalone",
       "mode" : "standalone",
       "query":"bb-q1",
       "format":"parquet",
       "save":"True",
       "plot":"False"
    },
    # Measure BB-Q1 synthetic shuffle parquet c#
    {
        # we can use the standalone-function here because we can turn of partitioning and the reducer receives only 1 input file to read...
       "language" : "c#",
       "function" : "c_sharp_udf_bb_q1_synthetic_shuffle",
       "mode" : "synthetic-shuffle",
       "query":"bb-q1-synth",
       "format":"parquet",
       "save":"True",
       "plot":"False"
    },
    # Measure BB-Q1 BabelMr parquet c#
    {
       "language" : "c#",
       "function" : "csharp_babelmr_tpcx_bb_parquet",
       "mode" : "babelmr",
       "query":"bb-q1",
       "format":"parquet",
       "save":"True",
       "plot":"False"
    },
    # Measure BB-Q1 Standalone parquet python
    {
        "language" : "python",
        "function" : "bench_bbq1_standalone_parquet_python",
        "mode" : "standalone",
        "query" : "bb-q1",
        "format" : "parquet",
        "save" : "True",
        "plot" : "False"
    },
    # Measure BB-Q1 synthetic shuffle parquet python
    #{
    #    "language" : "c#",
    #    "function" : "bench_bbq1_standalone_parquet_python",
    #    "mode" : "synthetic-shuffle",
    #    "query": "bb-q1-synth",
    #    "format": "parquet",
    #    "save":True,
    #    "plot":False
    #},
    # Measure BB-Q1 BabelMr parquet python
    {
        "language" : "python",
        "function" : "bench_bbq1_babelmr_parquet_python",
        "mode" : "babelmr",
        "query": "bb-q1",
        "format": "parquet",
        "save": "True",
        "plot": "False"
    }
]

RELATED_JOBS = [
    {
        "identifier" : "PyWren/tpc-h-q1/csv",
        "command" : lambda _, sf: f"{PYTHON} PyWren/tpc-h-q1_csv.py {sf}"
    },
    {
        "identifier" : "PyWren/tpc-h-q1/parquet",
        "command" : lambda _, sf: f"{PYTHON} PyWren/tpc-h-q1_parquet.py {sf}"
    },
    {
        "identifier" : "EMR_Serverless/tpc-h-q1/csv/conservative",
        "command" : lambda _, sf: f"{PYTHON} pyspark/execute_pyspark.py --sf {sf} --execute benchmark --clustermode conservative",
        "setup_command" : lambda _, sf: f"{PYTHON} pyspark/execute_pyspark.py --sf {sf} --execute setup --clustermode conservative",
        "teardown_command" : lambda _, sf: f"{PYTHON} pyspark/execute_pyspark.py --sf {sf} --execute teardown --clustermode conservative"
    },
    {
        "identifier" : "EMR_Serverless/tpc-h-q1/csv/pre-initialized",
        "command" : lambda _, sf: f"{PYTHON} pyspark/execute_pyspark.py --sf {sf} --execute benchmark --clustermode pre-initialized",
        "setup_command" : lambda _, sf: f"{PYTHON} pyspark/execute_pyspark.py --sf {sf} --execute setup --clustermode pre-initialized",
        "teardown_command" : lambda _, sf: f"{PYTHON} pyspark/execute_pyspark.py --sf {sf} --execute teardown --clustermode pre-initialized"
    },
    {
        "identifier" : "Corral/tpc-h-q1/csv",
        "command" : lambda _, sf: f"./Corral/corral-q1 --lambda -v s3://rsws2022/tpch-csv/sf{sf}/lineitem/* -o s3://rsws2022/tpch-csv/results/",
        "env" : {
            "CORRAL_LAMBDAFUNCTIONNAME":"tpch_q1",
            "CORRAL_LAMBDAMANAGEROLE": False,
            "CORRAL_LAMBDAROLEARN" : f"arn:aws:iam::{environ['AWS_ACCOUNT_ID']}:role/AWSLambda",
            "CORRAL_LAMBDATIMEOUT" : 899,
            "CORRAL_LAMBDAMEMORY" : 5120,
            "CORRAL_MAPBINSIZE" : 356515840,
            "CORRAL_MAXCONCURRENCY" : 250
        }
    }
]

def identifier(job):
    if "identifier" in job:
        return job["identifier"]
    return f"{job['mode']}/{job['query']}/{job['language']}/{job['format']}"

def print_and_log(
    identifier: str,
    times: list,
    scale_factor: float,
    repetitions: int,
    log: dict,
):
    print(f"{identifier} @ {scale_factor} :  {times}")
    print(
        f"{identifier} @ {scale_factor} (avg) : {sum(times) / repetitions}"
    )
    print("")
    log["times"] = times
    log["avg"] = sum(times) / repetitions
    log["med"] = sorted(times)[repetitions // 2]

def time_job_execution(
    log: dict,
    job: dict,
    cmd_gen,
    cwd,
    repetitions=REPETITIONS,
    setup_cmd_gen=None,
    teardown_cmd_gen=None
):
    f = StringIO()
    id = identifier(job)
    print(f"Starting benchmark of {id}")
    for scale_factor in SCALE_FACTORS:
        log_sf = log.setdefault(id, {}).setdefault("runtimes", {}).setdefault(str(scale_factor), {})
        if log_sf:
            continue
        times = []
        for _ in range(repetitions):
            with redirect_stdout(f):
                if setup_cmd_gen is not None:
                    subprocess.run(
                        setup_cmd_gen(job, scale_factor),
                        shell=True,
                        cwd=cwd
                    )
                if "env" in job:
                    start = time.time()
                    subprocess.run(
                        cmd_gen(job, scale_factor),
                        shell=True,
                        cwd=cwd,
                        env=job["env"] if "env" in job else {}
                    )
                    end = time.time()
                else:
                    start = time.time()
                    subprocess.run(
                        cmd_gen(job, scale_factor),
                        shell=True,
                        cwd=cwd
                    )
                    end = time.time()
                if teardown_cmd_gen is not None:
                    subprocess.run(
                        teardown_cmd_gen(job, scale_factor),
                        shell=True,
                        cwd=cwd
                    )
            time_lapsed = end - start
            times.append(time_lapsed)
        print_and_log(id, times, scale_factor, repetitions, log_sf)
        new_log = json.dumps(log)
        with open("data/benchmark_log.json", "w") as log_file:
            log_file.write(new_log)


def main():
    # bench BabelMR jobs:
    try:
        with open("data/benchmark_log.json", "r") as log:
            benchmark_results = json.load(log)
    except:
        benchmark_results = {}

    for job in JOBS:
        time_job_execution(benchmark_results,
                           job,
                           lambda job, sf: f"{PYTHON} invoke.py --function {job['function']} --mode {job['mode']} --query {job['query']} --format {job['format']} --sf {sf} --save {job['save']} --plot {job['plot']}",
                           path.join(path.dirname(path.abspath(__file__)), "util"),
                           REPETITIONS)

    for job in RELATED_JOBS:
        setup_cmd = job["setup_command"] if "setup_command" in job else None
        teardown_cmd = job["teardown_command"] if "teardown_command" in job else None

        time_job_execution(benchmark_results,
                           job,
                           job["command"],
                           path.join(path.dirname(path.abspath(__file__)), "other_frameworks"),
                           REPETITIONS,
                           setup_cmd,
                           teardown_cmd)


if __name__ == "__main__":
    main()

