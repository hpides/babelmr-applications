import boto3
import botocore
import json
import argparse
import pickle
import os
import time

from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from plot_udf_invocations import plot
from evaluate_invoke_responses import LambdaInvocationResponse
from alive_progress import alive_bar


@dataclass
class Invoke:
    """Class to store invoke context and results"""

    function: str
    pipeline_payloads: list[list[str]]
    pipeline_modes: list[str]
    pipeline_results: list[list[LambdaInvocationResponse]]


config = botocore.config.Config(
    read_timeout=900, connect_timeout=900, retries={"max_attempts": 10}
)

LAMBDA_ = boto3.client("lambda", region_name="us-east-1", config=config)


def load_jobs(path):
    with open(path, "r") as f:
        job_invokes = json.load(f)["jobs"]
    return job_invokes


def execute_lambda_payloads(payloads, lambda_function, bar):
    with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
        result = list(executor.map(wrap_invoke_lambda(lambda_function, bar), payloads))
    return result


def wrap_invoke_lambda(lambda_function, bar):
    def wrapped_invoke_lambda(x):
        return invoke_lambda(x, lambda_function, bar)

    return wrapped_invoke_lambda


def invoke_lambda(payload, lambda_function, bar):
    response = LAMBDA_.invoke(
        FunctionName=lambda_function,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload),
    )
    bar()
    return response


def execute_pipeline_payloads(pipelines, pipeline_modes, function):
    pipeline_responses = []
    for mode, pipeline in zip(pipeline_modes, pipelines):
        with alive_bar(len(pipeline)) as bar:
            responses = None
            print(f"{mode} Phase")
            if mode.startswith("shuffle"):
                responses = execute_lambda_payloads(pipeline, "SystemSideShuffle", bar)
            else:
                responses = execute_lambda_payloads(pipeline, function, bar)
            pipeline_responses.append(
                sorted(
                    list(map(lambda x: LambdaInvocationResponse(x, mode), responses)),
                    key=lambda x: x.timestamp,
                )
            )
    return pipeline_responses


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Invoke Skyrise-UDFs")
    parser.add_argument(
        "--sf", type=int, help="sf to be used [1,10,100,1000]", default=1
    )
    parser.add_argument(
        "--function",
        type=str,
        help="Function name to be invoked in Lambda",
        default="MapReducer_UDF_python",
    )
    parser.add_argument(
        "--payload",
        type=str,
        help="If set, execute only this payload on given function",
        default=None,
    )
    parser.add_argument(
        "--mode",
        type=str,
        help="Determines wether we send payloads created for standalone version, or babelmr versions.",
        default="standalone",
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Choose which query to run",
        default="tpc-h-q1",
    )
    parser.add_argument(
        "--format",
        type=str,
        help="Choose which file format shall be used [csv, parquet]",
        default="parquet",
    )
    parser.add_argument("--save", type=str, default="True")
    parser.add_argument("--plot", type=str, default="True")

    args = parser.parse_args()

    if args.payload == None:
        if args.query == "tpc-h-q1":
            pipelines_payloads = [
                load_jobs(Path(f"plans/tpc-h-q1/{args.mode}/{args.format}/maps_{args.sf}.json")),
                load_jobs(Path(f"plans/tpc-h-q1/{args.mode}/{args.format}/reduces_{args.sf}.json")),
            ]
            pipeline_modes = ["map", "reduce"]
            #pipeline_modes = ["map"]
        elif args.query == "bb-q1":
            pipelines_payloads = [
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/maps_1_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/maps_2_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/reduces_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/reduces_2_{args.sf}.json")),
            ]
            pipeline_modes = ["map1", "map2", "reduce", "reduce2"]
        elif args.query == "bb-q1-synth":
            pipelines_payloads = [
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/maps_1_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/shuffle_1_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/maps_2_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/shuffle_2_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/reduces_{args.sf}.json")),
                load_jobs(Path(f"plans/bb-q1/{args.mode}/{args.format}/reduces_2_{args.sf}.json")),

            ]
            pipeline_modes = [ "map1", "shuffle1", "map2", "shuffle2", "reduce", "reduce2"]
        elif args.query == "tpc-h-q1-synth":
            pipelines_payloads = [
                load_jobs(Path(f"plans/tpc-h-q1/{args.mode}/{args.format}/maps_{args.sf}.json")),
                load_jobs(Path(f"plans/tpc-h-q1/{args.mode}/{args.format}/shuffle_{args.sf}.json")),
                load_jobs(Path(f"plans/tpc-h-q1/{args.mode}/{args.format}/reduces_{args.sf}.json")),
            ]
            #  pipeline_modes = ["map", "reduce"]
            pipeline_modes = ["map", "shuffle1", "reduce"]
        else:
            print("Unknown --query selected")
            exit(-1)
    else:
        pipelines_payloads = [[json.loads(args.payload)]]
        pipeline_modes = ["single"]

    pipelines = execute_pipeline_payloads(
        pipelines_payloads, pipeline_modes, args.function
    )

    price = 0
    end = 0
    for pipeline in pipelines:
        for invoke in pipeline:
            price += invoke.price()
            end = max(end, invoke.invocation_end)
    print()
    print("(Estimated) Price ($): ", price)
    print(
        "Duration: ",
        end - pipelines[0][0].timestamp,
    )

    if args.save == "True":
        folder = Path(f"cached_invokes/{time.strftime('%Y-%m-%d-%H-%M-%S')}")
        os.makedirs(folder, exist_ok=True)
        with open(folder / "invoke.pickle", "wb") as f:
            pickle.dump(
                Invoke(args.function, pipelines_payloads, pipeline_modes, pipelines), f
            )
        if args.plot == "True":
            plot(args.function, args.sf, pipelines, folder / "invocations.pdf")
    else:
        if args.plot == "True":
            plot(args.function, args.sf, pipelines)
