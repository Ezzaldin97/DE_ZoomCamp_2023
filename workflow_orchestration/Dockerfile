FROM python:3.9

RUN apt-get install wget

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip -r requirements.txt
COPY ingest_data_flow.py ingest_data_flow.py

ENTRYPOINT ["python", "ingest_data_flow.py"]