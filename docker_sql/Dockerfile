FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY get_data_loop.sh get_data_loop.sh

ENTRYPOINT ["/bin/bash", "get_data_loop.sh"]