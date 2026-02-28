FROM python:3.12-slim

WORKDIR /home/flask-app

COPY payment/requirements.txt .
RUN pip install -r requirements.txt

COPY payment/ .
COPY dds_db ./dds_db

EXPOSE 5000