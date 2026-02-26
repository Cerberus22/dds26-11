FROM python:3.12-slim

WORKDIR /home/flask-app

COPY payment/requirements.txt .

RUN pip install -r requirements.txt

COPY payment/ .
COPY MqAbstraction.py .

EXPOSE 5000