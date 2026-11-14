FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .

# Hosts such as Render give the port in $PORT (10000 by default there).
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-10000} --ws-ping-interval 20 --ws-ping-timeout 20"]
