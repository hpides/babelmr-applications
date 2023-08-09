import json

def get_average_warm_runs(times):
    # -2.0 == failed run, -1.0 not benched
    if times == [-2.0] or times == [-1.0]:
        return -2.0
    assert(len(times) > 1)
    return sum(times[1:]) / (len(times)-1)

def get_benchmark_results():
    try:
        with open("../data/benchmark_log.json", "r") as log:
            benchmark_results = json.load(log)
        return benchmark_results
    except:
        print("No benchmark data found in data/benchmark_log")
        exit(1)

def get_initialization_benchmark_results():
    try:
        with open("../data/benchmark_startup_latency.json", "r") as log:
            benchmark_results = json.load(log)
        return benchmark_results
    except:
        print("No benchmark data found in data/benchmark_log")
        exit(1)

def get_metrics(function_package_name : str, log : dict):
    for run in log["runs"]:
        if run["parameters"]["function_package_name"] == function_package_name:
            return run["metrics"]