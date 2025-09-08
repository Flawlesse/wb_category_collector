FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONOTWRITEBYTECODE=1

VOLUME ["/data", "/logs"]

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY user_agents.py .

CMD ["python", "main.py"]
