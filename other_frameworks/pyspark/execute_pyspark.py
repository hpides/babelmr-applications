import boto3
import argparse
emr_client = boto3.client('emr-serverless')
import time
from json import dumps
from os import environ

EXECUTION_ROLE = f"arn:aws:iam::{environ['AWS_ACCOUNT_ID']}:role/emr-serverless-job-role"
vCPUHourCost = 0.052624
memoryGBHourCost = 0.0057785
CONFIG_APPLICATION_ID_MAPPING = {
  "pre-initialized" : {
    1 : "00fb9cvoide4i909",
    10 : "00fb9d0q5dgam209",
    100 : "00fb9c6a31631909",
    1000 : "00fb9d1mfc45fa09"
  },
  "conservative" : {
    1 : "00fb8kendf07s909",
    10 : "00fb8kobvlafro09",
    100 : "00fb8ktcubdumk09",
    1000 : "00fb8l2jssfc0v09"
  }
}

def get_application_id(clustermode : str, sf : int):
  if clustermode not in CONFIG_APPLICATION_ID_MAPPING.keys() or sf not in [1, 10, 100, 1000]:
    raise NotImplementedError("Not supported")
  return CONFIG_APPLICATION_ID_MAPPING[clustermode][sf]

def get_application_status(application_id : str):
  return emr_client.get_application(applicationId=application_id)["application"]["state"]

def start_application(clustermode : str, sf : int):
  app_id = get_application_id(clustermode, sf)
  emr_client.start_application(applicationId=app_id)
  await_status(app_id, "STARTED")


def await_status(application_id : str, awaited_status : str):
  status = get_application_status(application_id)
  print(f"awaits status {awaited_status}, currently {status}")
  while status != awaited_status:
    time.sleep(5.0)
    print(f"still awaiting status {awaited_status}")
    status = get_application_status(application_id)

def stop_application(clustermode : str, sf :int):
  application_id = get_application_id(clustermode, sf)
  emr_client.stop_application(applicationId=application_id)
  await_status(application_id, "STOPPED")


def job_driver_config(sf:int):
  return {
    "sparkSubmit": {
        "entryPoint": "s3://rsws2022/code/pyspark/tpchq1.py",
        "entryPointArguments": [
                str(sf),
            ],
    }
  }


def get_job_status(application_id : str, job_id : str):
  return emr_client.get_job_run(applicationId=application_id, jobRunId=job_id)["jobRun"]["state"]

def parse_job_results(application_id :str , job_id : str):
  job_result = emr_client.get_job_run(applicationId=application_id, jobRunId=job_id)["jobRun"]
  runtime = job_result["totalExecutionDurationSeconds"]
  cost = job_result["totalResourceUtilization"]['vCPUHour'] * vCPUHourCost + job_result["totalResourceUtilization"]['memoryGBHour'] * memoryGBHourCost
  return {"runtime" : runtime, "cost" : cost, "applicationId": application_id, "jobId" : job_id}

def write_to_file(clustermode : str, sf : int , benchmark_results : dict):
  with open(f"./{clustermode}_{sf}.json", "a") as log:
    log.write(dumps(benchmark_results))


def execute_benchmark(clustermode : str, sf : int):
  application_id = get_application_id(clustermode, sf)
  driver_config = job_driver_config(sf)
  response = emr_client.start_job_run(applicationId=application_id,executionRoleArn=EXECUTION_ROLE, jobDriver=driver_config)
  job_id = response["jobRunId"]

  job_status = get_job_status(application_id, job_id)

  while job_status != "SUCCESS":
    time.sleep(10)
    job_status = get_job_status(application_id, job_id)
    if job_status == "FAILED":
      raise NotImplementedError("Job Failed. Not good")
  # Costs are only available after the application stopped. We just save jobId and applicationId and query the costs later.
  benchmark_results = {"applicationId" : application_id, "jobId" : job_id}
  write_to_file(clustermode, sf, benchmark_results)

def setup(clustermode : str, sf : int):
  try:
    start_application(clustermode, sf)
  except:
    print("Something went wrong during setup.")
  finally:
    stop_application(clustermode, sf)

def execute(clustermode : str, sf : int):
  try:
    execute_benchmark(clustermode, sf)
  except:
    print("Something went wrong during benchmark execution.")
  finally:
    stop_application(clustermode, sf)

def teardown(clustermode : str, sf : int):
  try:
    stop_application(clustermode, sf)
  except:
    print("Something went wrong during teardown.")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(prog="Invoke PySpark Service on AWS Elastic MapReduce (EMR)")

  parser.add_argument(
      "--sf", type=int, help="sf to be used [1,10,100,1000]", default=1
  )
  parser.add_argument(
      "--clustermode",
      type=str,
      help="Either pre-initialized or conservative",
      default="pre-initialized",
  )

  parser.add_argument(
      "--execute",
      type=str,
      help="Either setup, benchmark, teardown or all",
      default="all",
  )

  args = parser.parse_args()

  clustermode = args.clustermode
  sf = args.sf
  execute_arg = args.execute

  if execute_arg == "setup":
    start_application(clustermode, sf)
  elif execute_arg == "benchmark":
    execute_benchmark(clustermode, sf)
  elif execute_arg == "teardown":
    stop_application(clustermode, sf)
  elif execute_arg == "all":
    try:
      # setup
      start_application(clustermode, sf)
      # execution
      execute_benchmark(clustermode, sf)
    except:
      print("Something went wrong.")
    finally:
      # teardown
      stop_application(clustermode, sf)
  else:
    raise NotImplementedError("Not Supported execution argument")