FROM python:3.12-slim

WORKDIR /home/flask-app

COPY order/requirements.txt .
RUN pip install -r requirements.txt

COPY order/ .
COPY dds_db ./dds_db

EXPOSE 5000