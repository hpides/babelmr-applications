FROM babelmr/al2:latest

RUN yum install python3 -y

COPY requirements.txt ./
RUN python3 -m pip install -r /var/task/requirements.txt

COPY app.py ./
COPY fs.py ./
COPY map1.py ./
COPY map2.py ./
COPY reduce.py ./


ENV MR_CMD="python3 /var/task/app.py"