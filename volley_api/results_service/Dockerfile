FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py .
CMD ["gunicorn", "-b", "0.0.0.0:8080", "service:app"]
