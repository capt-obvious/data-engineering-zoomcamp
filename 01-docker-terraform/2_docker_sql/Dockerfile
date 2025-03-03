FROM python:3.13.2

RUN apt-get install wget
RUN pip install duckdb

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]
