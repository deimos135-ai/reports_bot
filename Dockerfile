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
COPY . .

# -------------------- Runtime --------------------
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
