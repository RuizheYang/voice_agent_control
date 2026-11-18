FROM python:3.12-slim

WORKDIR /app

ARG ENV=dev

COPY project/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && rm /app/requirements.txt

COPY project /app

ENV ENV=${ENV}
ENV PYTHONPATH=/app
ENV PORT=8001

EXPOSE 8001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--log-config", "log_conf.yaml", "--timeout-graceful-shutdown", "30"]
