FROM python:3.7-alpine
ADD . /stockprophet
WORKDIR /stockprophet
RUN pip install -r requirements.txt
CMD ["python", "stockprophet"]
