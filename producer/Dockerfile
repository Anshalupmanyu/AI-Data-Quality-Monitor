FROM python:3.11

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY fake_data_producer.py .

CMD ["python", "fake_data_producer.py"]