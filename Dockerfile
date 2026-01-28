FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt* ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# Default to job mode; service overrides with WEB_MODE=1
ENV WEB_MODE=""
CMD ["/bin/sh", "-c", "if [ \"$WEB_MODE\" = \"1\" ]; then uvicorn app.web:app --host 0.0.0.0 --port 8080; else python -m app.main; fi"]
