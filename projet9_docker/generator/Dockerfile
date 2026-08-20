FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ticket_generator.py .

ENV REDPANDA_BROKERS=redpanda:9092

ENTRYPOINT ["python", "ticket_generator.py"]
CMD ["--interval", "1"]
