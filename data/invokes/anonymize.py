import pickle
from pathlib import Path
import sys
import os

# get relative imports from util
sys.path.append(os.path.join(sys.path[0], "..", "..", "util"))
from evaluate_invoke_responses import LambdaInvocationResponse
from invoke import Invoke

def anonymize(lambda_invocation_response : LambdaInvocationResponse):
    print(lambda_invocation_response.invocation_response)
    lambda_invocation_response.invocation_response = None
    print(lambda_invocation_response.invocation_response)


if __name__ == "__main__":
    for folder in ["babelmr", "standalone", "synth"]:
        with open(Path(folder) / "invoke.pickle", "rb") as file:
            invoke = pickle.load(file)
        for pipeline in invoke.pipeline_results:
            for lambda_invocation_response in pipeline:
                anonymize(lambda_invocation_response)
        with open(Path(folder) / "invoke.pickle", "wb") as file:
            pickle.dump(invoke, file)