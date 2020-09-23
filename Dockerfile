FROM python:3.8-slim
WORKDIR /root

COPY ["stockprophet", "/root/stockprophet"]
COPY ["requirements.txt", "/root/requirements.txt"]

RUN pip install -r requirements.txt --no-cache-dir
ENTRYPOINT ["python3", "stockprophet"]
