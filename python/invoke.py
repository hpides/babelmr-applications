import boto3
import json
from concurrent.futures import ThreadPoolExecutor

lambda_ = boto3.client('lambda', region_name='us-east-1')

with open("tpc-h-q1/mapreduce_standalone/events/pipeline/map_jobs.json", "r") as f:
    map_invokes = json.load(f)["jobs"]
with open("tpc-h-q1/mapreduce_standalone/events/pipeline/reduce_jobs.json") as f:
    reduce_invokes = json.load(f)["jobs"]

def invoke_lambda(payload):
    response = lambda_.invoke(
        FunctionName='MapReducer_UDF_python',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps(payload)
    )

    res_payload = response.get('Payload').read()
    body = json.loads(res_payload).get('body')

    return body


MAX_WORKERS = 100  # how many lambdas you want to spin up concurrently

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    result = list(executor.map(invoke_lambda, map_invokes))
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    result = list(executor.map(invoke_lambda, reduce_invokes))