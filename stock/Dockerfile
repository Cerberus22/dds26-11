FROM python:3.12-slim

WORKDIR /home/flask-app

COPY stock/requirements.txt .

RUN pip install -r requirements.txt

COPY stock/ .
COPY MqAbstraction.py .

EXPOSE 5000