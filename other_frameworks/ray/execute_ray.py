import boto3
import time
import argparse
from json import dumps
# those jobs execute the same script (./tpcq1_ray) but are invoked with different scalefactor
# parameter and are configured with ressources according to the scalefactor 
SF_JOB_MAPPING = {
    "1": "ray_tpch_q1_sf1", 
    "10": "ray_tpch_q1_sf10",
    "100": "ray_tpch_q1_sf100",
    "1000": "ray_tpch_q1_sf1000"
}

glue = boto3.client(service_name='glue', region_name='us-east-1')

def write_to_file(sf : int , benchmark_results : dict):
    with open(f"./ray_{sf}.json", "a") as log:
        log.write(dumps(benchmark_results))

def execute_etl_job(sf : int):
    if sf not in SF_JOB_MAPPING: 
        raise NotImplementedError("NOT SUPPORTED scalefactor")
    
    job_name = SF_JOB_MAPPING[sf]
    myNewJobRun = glue.start_job_run(JobName=job_name, Arguments={'--sf': sf})

    status = glue.get_job_run(JobName=job_name, RunId=myNewJobRun['JobRunId'])

    while status["JobRun"]["JobRunState"] == "RUNNING" : 
        status = glue.get_job_run(JobName=job_name, RunId=myNewJobRun['JobRunId'])
        time.sleep(10.0)
        print(status["JobRun"]["JobRunState"])
    
    results = {"RunID" : myNewJobRun['JobRunId'], "job_name": job_name, "status": status["JobRun"]["JobRunState"], "exeuction_time" : status["JobRun"]["ExecutionTime"], "started_on" : str(status["JobRun"]["StartedOn"]), "completed_on" : str(status["JobRun"]["CompletedOn"])}
    write_to_file(sf, results)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Invoke PySpark Service on AWS Elastic MapReduce (EMR)")

    parser.add_argument(
        "--sf",  help="sf to be used [1,10,100,1000]", default=1
    )
    args = parser.parse_args()
    sf = args.sf

    for i in range(10):
        execute_etl_job(sf)
        print("WAITING for " + str(i))
        time.sleep(60.0)