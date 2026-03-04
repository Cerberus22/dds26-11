FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./stock/requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY ./stock/ ./
COPY ./messages.py ./messages.py

EXPOSE 5000