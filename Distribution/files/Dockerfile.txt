FROM python:3.8-slim

RUN apt-get update && apt-get install -y libpq-dev gcc
ENV TZ=Europe/Moscow
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["python", "app.py"]