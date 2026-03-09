FROM python:3.12-slim

WORKDIR /app

COPY order/requirements.txt .
RUN pip install -r requirements.txt

COPY order/ .
COPY dds_db ./dds_db

EXPOSE 5000