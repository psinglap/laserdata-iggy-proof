FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt iggy_lag_exporter.py ./
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 9108
ENTRYPOINT ["python", "iggy_lag_exporter.py"]
