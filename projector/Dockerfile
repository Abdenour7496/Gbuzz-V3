FROM python:3.15-slim

WORKDIR /app
COPY projector/requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY projector/main.py .
COPY projector/test_main.py .

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD python -c "import os,time; assert time.time()-os.stat('/tmp/projector-heartbeat').st_mtime<600"
CMD ["python", "main.py"]
