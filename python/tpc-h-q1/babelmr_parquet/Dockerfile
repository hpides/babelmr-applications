FROM babelmr/al2:latest

RUN yum install python3 -y

COPY requirements.txt ./
RUN python3 -m pip install -r /var/task/requirements.txt

COPY fs.py ./
COPY handler.py ./
COPY map.py ./
COPY reduce.py ./


ENV MR_CMD="python3 /var/task/handler.py"