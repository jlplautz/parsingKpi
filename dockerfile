FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN pip install paramiko psycopg2-binary pytz

CMD ["python", "kpiAirScale00.py"]