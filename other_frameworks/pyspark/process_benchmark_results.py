from json import loads
from os.path import exists
from execute_pyspark import parse_job_results

for mode in ["pre-initialized", "conservative"]: 
  for sf in [1,10,100,1000]:
    sf_str = str(sf).ljust(4)
    filename = f"./{mode}_{sf}.json"
    
    if not exists(filename): 
      continue
    
    with open(filename) as file:
      lines = [line.rstrip() for line in file]
      lines = lines[-11:]
      cost_agg = 0
      runtime_agg = 0
      for idx, line in enumerate(lines): 
        benchmark_result = loads(line)
        applicationId = benchmark_result["applicationId"]
        jobId = benchmark_result["jobId"]
        
        results = parse_job_results(applicationId, jobId)
        
        r = results["runtime"]
        
        mode_str = "cold" if idx == 0 else "warm"
        print(f"sf: {sf_str} \t runtime: {r} \t ({mode_str})")
        
        cost_agg += results["cost"]
        runtime_agg += results["runtime"]
      cost_agg /= len(lines)
      runtime_agg /= len(lines)
      cost_agg = round(cost_agg, 3)
      print(f"Mode: {mode} \t\t sf: {sf_str} \t\t cost_avg: {cost_agg} $ \t\t runtime_avg : {runtime_agg}")

