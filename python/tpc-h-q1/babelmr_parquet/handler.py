import sys
import json
from map import map
from reduce import reduce

mode = sys.argv[1]
if mode == "map":
    print(json.dumps(map()))
elif mode == "reduce":
    print(json.dumps(reduce()))