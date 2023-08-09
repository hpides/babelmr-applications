import json
import base64
import re

LAMBDA_1MB_MS_DOLLAR = 0.0000000021 / 128

def extract_payload(invocation):
    return json.loads(invocation.get("Payload").read())


class LambdaInvocationResponse:

    def __init__(self, invocation_response, mode):

        self.invocation_response = invocation_response
        self.mode = mode

        self.payload = extract_payload(invocation_response)
        del self.invocation_response["Payload"]
        self.times = self.payload["times"]
        self.timestamp = self.times[0]["timestamp"]
        self._parse_log_result()
        self.invocation_end = self.timestamp + self.billed_duration_ms

    def _parse_log_result(self):
        log_string = base64.b64decode(self.invocation_response.get("LogResult")).decode("utf-8")
        self.duration_ms = float(re.findall(r"\tDuration: (\d*.\d*)", log_string)[0])
        self.billed_duration_ms = int(re.findall(r"Billed Duration: (\d*)", log_string)[0])
        self.memory_mb = int(re.findall(r"Memory Size: (\d*)", log_string)[0])
        self.max_memory_used_mb = int(re.findall(r"Max Memory Used: (\d*)", log_string)[0])

    def log(self):
        print(
            ", ".join(
                f"{x}: {y}"
                for x, y in vars(self).items()
                if x not in ["invocation_response", "payload"]
            )
        )

    def price(self):
        return self.billed_duration_ms * self.memory_mb * LAMBDA_1MB_MS_DOLLAR