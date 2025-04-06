FROM python:3.11.11-slim-buster

WORKDIR /app

COPY ./app /app

RUN pip install -r requirements.txt

CMD ["python3", "app.py"]