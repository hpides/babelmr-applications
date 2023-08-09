import sys
import json
from map1 import map1
from map2 import map2
from reduce import reduce


mode = sys.argv[1]
if mode == "map1":
    print(json.dumps(map1()))
elif mode == "map2":
    print(json.dumps(map2()))
elif mode == "reduce":
    print(json.dumps(reduce()))
else:
    print("given mode was not found")