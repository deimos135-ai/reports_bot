# -------------------- Base --------------------
FROM python:3.11-slim

# -------------------- System deps --------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# -------------------- App deps --------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -------------------- App code --------------------
COPY app_web/ app_web/
COPY shared/ shared/

# -------------------- Runtime --------------------
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "app_web.main:app", "--host", "0.0.0.0", "--port", "8080"]
