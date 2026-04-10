FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FLASK_ENV=production \
    DATABASE_URL=/runtime/aurum.db

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /runtime

EXPOSE 5000

CMD ["gunicorn", "--workers=2", "--threads=4", "--bind=0.0.0.0:5000", "--timeout=120", "app:app"]